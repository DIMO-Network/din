// Package admit takes CloudEvents from a source: a relay that signs in with a
// certificate naming its DID and sends events about vehicles it holds a write
// grant for (docs/2026-10-24-sources.md in did-directory).
//
// Unlike the connection-license path in package convert, nothing here rewrites
// the event. The source and, when it has a DID, the producer sign the header
// and the exact data, so din checks those signatures and the grant, then
// stores the event as sent. It adds two extensions of its own, which no
// signature covers: when the event arrived and which grant let it in.
package admit

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/did-directory/pkg/client"
	"github.com/DIMO-Network/did-directory/pkg/provenance"
	"github.com/DIMO-Network/din/internal/convert"
)

// Extensions din sets on every admitted event, replacing any the source sent.
const (
	// ArrivedAtAttr is when din received the event (RFC 3339, UTC). Writes
	// are checked at arrival, so this is the time the grant was live at; the
	// event's own time is the source's claim.
	ArrivedAtAttr = "arrivedat"
	// GrantAttr is the URI of the grant that let the source write, or empty
	// when the source is the vehicle's controller.
	GrantAttr = "grant"
)

var (
	// ErrForbidden is a source that may not write an event: a signature that
	// is missing or does not verify, or no grant covering the vehicle.
	ErrForbidden = errors.New("forbidden")
	// ErrUnavailable is a check din could not finish: the directory or the
	// org host did not answer. The source may retry.
	ErrUnavailable = errors.New("authorization unavailable")
)

// didSyntax is a DID with no path, query or fragment and no percent-encoding.
// The subject becomes part of blob keys (split.buildBlobKey), so it may not
// carry a "/".
var didSyntax = regexp.MustCompile(`^did:[a-z0-9]+:[A-Za-z0-9._:-]*[A-Za-z0-9._-]$`)

// abilities is what writing each event type takes. Every type din accepts
// from sources is telemetry for now; fingerprints and the rest may want
// their own *:write later.
var abilities = map[string]string{
	cloudevent.TypeStatus:      "telemetry:write",
	cloudevent.TypeSignals:     "telemetry:write",
	cloudevent.TypeEvents:      "telemetry:write",
	cloudevent.TypeFingerprint: "telemetry:write",
	cloudevent.TypeRawStatus:   "telemetry:write",
}

// Authorizer finds the grant that lets source exercise abilities on vehicle
// now. It returns a *Refusal when there is none.
type Authorizer interface {
	Authorize(ctx context.Context, source, vehicle string, abilities []string) (string, error)
}

// Admitter checks and stamps events from sources.
type Admitter struct {
	// Keys finds signing keys; provenance.DIDKeys over the directory.
	Keys provenance.Keys
	// Authorizer is the org host.
	Authorizer Authorizer
	// GrantTTL is how long an answer is reused for one (source, vehicle,
	// ability); RefusalTTL the same for a refusal. Defaults 1m and 10s.
	GrantTTL, RefusalTTL time.Duration
	// Now is the clock; time.Now when nil.
	Now func() time.Time

	mu    sync.Mutex
	cache map[cacheKey]cached
}

type cacheKey struct{ source, vehicle, ability string }

type cached struct {
	grant   string
	refusal error
	until   time.Time
}

// maxCached bounds the answer cache; past it the cache starts over.
const maxCached = 100_000

// Admit parses body, one CloudEvent or a JSON array of them, sent by source,
// and returns the events ready to store. It admits all of them or none.
// Errors wrap convert.ErrValidation for a malformed event, ErrForbidden or
// ErrUnavailable.
func (a *Admitter) Admit(ctx context.Context, source string, body []byte) ([]cloudevent.RawEvent, error) {
	events, err := parse(body)
	if err != nil {
		return nil, err
	}
	arrived := a.now().UTC()
	for i := range events {
		if err := a.admit(ctx, source, &events[i], arrived); err != nil {
			return nil, fmt.Errorf("event %d (%s): %w", i, events[i].ID, err)
		}
	}
	return events, nil
}

func parse(body []byte) ([]cloudevent.RawEvent, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("%w: empty body", convert.ErrValidation)
	}
	var events []cloudevent.RawEvent
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &events); err != nil {
			return nil, fmt.Errorf("%w: not a CloudEvent array: %w", convert.ErrValidation, err)
		}
	} else {
		var event cloudevent.RawEvent
		if err := json.Unmarshal(trimmed, &event); err != nil {
			return nil, fmt.Errorf("%w: not a CloudEvent: %w", convert.ErrValidation, err)
		}
		events = append(events, event)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("%w: no events", convert.ErrValidation)
	}
	return events, nil
}

func (a *Admitter) admit(ctx context.Context, source string, ev *cloudevent.RawEvent, arrived time.Time) error {
	if ev.Source != source {
		return fmt.Errorf("%w: source %q is not the certificate's %q", ErrForbidden, ev.Source, source)
	}
	if !didSyntax.MatchString(ev.Subject) {
		return fmt.Errorf("%w: subject must be the vehicle's DID", convert.ErrValidation)
	}
	ability, ok := abilities[ev.Type]
	if !ok {
		return fmt.Errorf("%w: unsupported type %q", convert.ErrValidation, ev.Type)
	}
	// The connection path's checks, minus its defaults: din may not fill in a
	// signed field, so a header the defaults would change is incomplete.
	isBase64 := ev.DataBase64 != ""
	check := ev.CloudEventHeader
	check.Extras = nil
	if err := convert.ValidateHeadersAndSetDefaults(&check, source, "", isBase64); err != nil {
		return err
	}
	sent := ev.CloudEventHeader
	sent.Extras = nil
	if !reflect.DeepEqual(check, sent) {
		return fmt.Errorf("%w: id, time, subject, specversion and datacontenttype must all be set", convert.ErrValidation)
	}

	sigs := provenance.Signatures{
		Source:   extraString(ev.Extras, provenance.SourceSignatureAttr),
		Producer: extraString(ev.Extras, provenance.ProducerSignatureAttr),
	}
	h := provenance.Header{
		ID: ev.ID, Source: ev.Source, Producer: ev.Producer, Subject: ev.Subject,
		Type: ev.Type, Time: ev.Time, DataVersion: ev.DataVersion, DataContentType: ev.DataContentType,
	}
	if _, err := provenance.Verify(ctx, a.Keys, h, ev.Data, sigs); err != nil {
		return signatureError(err)
	}

	grant, err := a.authorize(ctx, source, ev.Subject, ability)
	if err != nil {
		return err
	}
	if ev.Extras == nil {
		ev.Extras = map[string]any{}
	}
	ev.Extras[ArrivedAtAttr] = arrived.Format(time.RFC3339Nano)
	ev.Extras[GrantAttr] = grant
	return nil
}

// signatureError sorts a provenance failure: the source's fault, or a
// directory that did not answer.
func signatureError(err error) error {
	switch {
	case errors.Is(err, provenance.ErrInvalidHeader):
		return fmt.Errorf("%w: %w", convert.ErrValidation, err)
	case errors.Is(err, provenance.ErrMissingSignature), errors.Is(err, provenance.ErrWrongSigner),
		errors.Is(err, provenance.ErrBadSignature), errors.Is(err, provenance.ErrUnknownKey),
		errors.Is(err, client.ErrNotFound):
		return fmt.Errorf("%w: %w", ErrForbidden, err)
	default:
		return fmt.Errorf("%w: checking signatures: %w", ErrUnavailable, err)
	}
}

func (a *Admitter) authorize(ctx context.Context, source, vehicle, ability string) (string, error) {
	key := cacheKey{source, vehicle, ability}
	now := a.now()
	a.mu.Lock()
	c, ok := a.cache[key]
	a.mu.Unlock()
	if ok && now.Before(c.until) {
		return c.grant, c.refusal
	}

	grant, err := a.Authorizer.Authorize(ctx, source, vehicle, []string{ability})
	var refusal error
	switch {
	case err == nil:
	case errorsAsRefusal(err):
		refusal = fmt.Errorf("%w: %w", ErrForbidden, err)
	default:
		return "", err // an outage is not remembered
	}
	ttl := cmp.Or(a.GrantTTL, time.Minute)
	if refusal != nil {
		ttl = cmp.Or(a.RefusalTTL, 10*time.Second)
	}
	a.mu.Lock()
	if a.cache == nil || len(a.cache) >= maxCached {
		a.cache = map[cacheKey]cached{}
	}
	a.cache[key] = cached{grant: grant, refusal: refusal, until: now.Add(ttl)}
	a.mu.Unlock()
	return grant, refusal
}

func (a *Admitter) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func extraString(extras map[string]any, name string) string {
	s, _ := extras[name].(string)
	return s
}
