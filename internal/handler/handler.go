// Package handler implements the ingest business logic behind the HTTP
// servers: convert → validate → split → publish, with dis-compatible
// response semantics (200 only after JetStream ack).
package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/attest"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/fpvalidate"
	"github.com/DIMO-Network/din/internal/server"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
)

// Publisher is the durability point; implemented by stream.Publisher.
// PublishBatch pipelines a request's events (one async round-trip window for
// all of them) while preserving Publish's per-event error semantics (load
// review #2).
type Publisher interface {
	Publish(ctx context.Context, event *cloudevent.StoredEvent) error
	PublishBatch(ctx context.Context, events []*cloudevent.StoredEvent) error
}

// Converter turns raw connection payloads into validated events.
type Converter interface {
	Convert(ctx context.Context, sourceAddr string, body []byte) ([]cloudevent.RawEvent, error)
}

// AttestationParser verifies and parses attestation payloads.
type AttestationParser interface {
	Parse(ctx context.Context, jwtAddress common.Address, body []byte) (*attest.Attestation, error)
}

// Splitter externalizes oversized payloads.
type Splitter interface {
	MaybeSplit(ctx context.Context, event cloudevent.RawEvent) (cloudevent.StoredEvent, error)
}

// Handlers builds the connection and attestation http.Handlers.
type Handlers struct {
	Converter           Converter
	Attest              AttestationParser
	Splitter            Splitter
	Publisher           Publisher
	ValidateFingerprint bool
	Log                 zerolog.Logger
}

// Connection handles mTLS device/oracle ingest. Source identity comes from
// the client certificate CN injected by the server middleware.
func (h *Handlers) Connection() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		source, ok := server.SourceFromContext(r.Context())
		if !ok {
			http.Error(w, "missing client identity", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.writeError(w, err)
			return
		}

		events, err := h.Converter.Convert(r.Context(), source, body)
		if err != nil {
			h.writeError(w, err)
			return
		}

		// dis semantics: a fingerprint that fails validation is dropped (bad
		// VIN) or 400s the request (conversion failure), but valid sibling
		// events from the same payload are still persisted first. Split every
		// surviving event up front, then PublishBatch pipelines all their acks
		// in one round-trip window instead of blocking per event (load review #2).
		var validationErr error
		toPublish := make([]*cloudevent.StoredEvent, 0, len(events))
		for i := range events {
			if h.ValidateFingerprint {
				if err := fpvalidate.Validate(r.Context(), events[i]); err != nil {
					if errors.Is(err, fpvalidate.ErrInvalidVIN) {
						h.Log.Warn().Str("source", source).Str("id", events[i].ID).Msg("dropping fingerprint with invalid VIN")
					} else {
						validationErr = errors.Join(validationErr, fmt.Errorf("%w: validating fingerprint %s: %w", convert.ErrValidation, events[i].ID, err))
					}
					continue
				}
			}
			stored, err := h.split(r.Context(), events[i], "")
			if err != nil {
				h.writeError(w, err)
				return
			}
			toPublish = append(toPublish, &stored)
		}
		// Publish errors take precedence over validation faults, matching the
		// old serial loop (which returned a publish error before ever checking
		// validationErr).
		if err := h.Publisher.PublishBatch(r.Context(), toPublish); err != nil {
			h.writeError(w, err)
			return
		}
		if validationErr != nil {
			h.writeError(w, validationErr)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// Attestation handles JWT-authenticated attestation ingest. Source identity
// is the verified Ethereum address from the token.
func (h *Handlers) Attestation() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		source, ok := server.SourceFromContext(r.Context())
		if !ok {
			http.Error(w, "missing client identity", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.writeError(w, err)
			return
		}

		attestation, err := h.Attest.Parse(r.Context(), common.HexToAddress(source), body)
		if err != nil {
			h.writeError(w, err)
			return
		}

		if err := h.publishOne(r.Context(), attestation.RawEvent, attestation.VoidsID); err != nil {
			h.writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// split externalizes an oversized payload and stamps the VoidsID, producing the
// StoredEvent that will be published.
func (h *Handlers) split(ctx context.Context, event cloudevent.RawEvent, voidsID string) (cloudevent.StoredEvent, error) {
	stored, err := h.Splitter.MaybeSplit(ctx, event)
	if err != nil {
		return cloudevent.StoredEvent{}, fmt.Errorf("splitting event %s: %w", event.ID, err)
	}
	stored.VoidsID = voidsID
	return stored, nil
}

// publishOne splits and durably publishes a single event — the attestation path,
// which never fans out.
func (h *Handlers) publishOne(ctx context.Context, event cloudevent.RawEvent, voidsID string) error {
	stored, err := h.split(ctx, event, voidsID)
	if err != nil {
		return err
	}
	return h.Publisher.Publish(ctx, &stored)
}

// writeError maps error classes to HTTP statuses: validation faults are the
// caller's (400), an unacknowledged publish is retryable (503), everything
// else is ours (500).
func (h *Handlers) writeError(w http.ResponseWriter, err error) {
	var maxBytes *http.MaxBytesError
	switch {
	case errors.As(err, &maxBytes):
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
	case errors.Is(err, stream.ErrPayloadTooLarge):
		// Deterministic — the event exceeds NATS max_payload. Map to 413, not the
		// retryable 503, so the device doesn't re-send the identical oversized
		// payload forever.
		http.Error(w, "event exceeds maximum payload size", http.StatusRequestEntityTooLarge)
	case errors.Is(err, convert.ErrValidation):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, stream.ErrUnavailable):
		w.Header().Set("Retry-After", "5")
		http.Error(w, "ingest temporarily unavailable", http.StatusServiceUnavailable)
	default:
		h.Log.Error().Err(err).Msg("ingest request failed")
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
