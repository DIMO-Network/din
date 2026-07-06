package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Publish outcome/latency metrics (H3): before these, a stalling or rejecting
// WAL was invisible until sink redeliveries fired minutes later. The duration
// histogram is the ingest-side ack-latency SLI; the outcome counter separates
// acks from transient failures (503 to devices) and payload rejections (413).
var (
	publishSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "din_publish_ack_seconds",
		Help:    "JetStream publish-to-ack latency.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	publishOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "din_publish_total",
		Help: "JetStream publishes by outcome: ok, unavailable (503), too_large (413).",
	}, []string{"outcome"})
)

func observePublish(start time.Time, err error) {
	publishSeconds.Observe(time.Since(start).Seconds())
	switch {
	case err == nil:
		publishOutcomes.WithLabelValues("ok").Inc()
	case errors.Is(err, ErrPayloadTooLarge):
		publishOutcomes.WithLabelValues("too_large").Inc()
	default:
		publishOutcomes.WithLabelValues("unavailable").Inc()
	}
}

// Header names carried on every published message. ce-* duplicate body
// fields so header-only consumers can route without unmarshaling; the
// din-* headers carry StoredEvent fields that the RawEvent body cannot
// (RawEvent's custom MarshalJSON knows nothing of storage metadata).
const (
	HeaderCEType    = "ce-type"
	HeaderCESubject = "ce-subject"
	HeaderCESource  = "ce-source"
	HeaderCEID      = "ce-id"

	HeaderVoidsID      = "din-voids-id"
	HeaderDataIndexKey = "din-data-index-key"
)

// ErrUnavailable reports that JetStream did not acknowledge the publish in
// time. Handlers map it to 503 so devices retry.
var ErrUnavailable = errors.New("jetstream publish not acknowledged")

// ErrPayloadTooLarge reports that the marshaled event exceeds NATS max_payload.
// Unlike ErrUnavailable this is deterministic — the identical event will always
// be rejected — so handlers map it to a non-retryable 413; mapping it to 503
// (the old behavior) made a device retry the same oversized payload forever.
var ErrPayloadTooLarge = errors.New("event exceeds max NATS payload")

// Publisher writes raw cloudevents to the INGEST_RAW stream and waits for
// the JetStream ack — the durability point of the ingest path.
type Publisher struct {
	js         jetstream.JetStream
	partitions int
}

// NewPublisher returns a Publisher routing to the given number of WAL
// partitions (1 = the single historical stream).
func NewPublisher(js jetstream.JetStream, partitions int) *Publisher {
	if partitions <= 0 {
		partitions = 1
	}
	return &Publisher{js: js, partitions: partitions}
}

// Publish sends one validated event and blocks until JetStream acks it or
// ctx expires. DataIndexKey and VoidsID travel as headers; the body is the
// RawEvent wire format.
func (p *Publisher) Publish(ctx context.Context, event *cloudevent.StoredEvent) (err error) {
	start := time.Now()
	defer func() { observePublish(start, err) }()
	body, err := event.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshaling event %s: %w", event.ID, err)
	}

	msg := &nats.Msg{
		Subject: Subject(&event.CloudEventHeader, p.partitions),
		Data:    body,
		Header: nats.Header{
			nats.MsgIdHdr:   []string{MsgID(&event.CloudEventHeader)},
			HeaderCEType:    []string{event.Type},
			HeaderCESubject: []string{event.Subject},
			HeaderCESource:  []string{event.Source},
			HeaderCEID:      []string{event.ID},
		},
	}
	if event.VoidsID != "" {
		msg.Header.Set(HeaderVoidsID, event.VoidsID)
	}
	if event.DataIndexKey != "" {
		msg.Header.Set(HeaderDataIndexKey, event.DataIndexKey)
	}

	future, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		return classifyPublishErr(err)
	}

	select {
	case <-future.Ok():
		return nil
	case err := <-future.Err():
		return classifyPublishErr(err)
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrUnavailable, ctx.Err())
	}
}

// classifyPublishErr maps a NATS publish error to retry semantics: a max-payload
// rejection is deterministic (the identical event always fails) → the
// non-retryable ErrPayloadTooLarge; anything else is a transient un-acked
// publish → ErrUnavailable (503, device retries).
func classifyPublishErr(err error) error {
	if errors.Is(err, nats.ErrMaxPayload) {
		return fmt.Errorf("%w: %w", ErrPayloadTooLarge, err)
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

// ParseMsg reconstructs a StoredEvent from a consumed JetStream message.
func ParseMsg(headers nats.Header, body []byte) (cloudevent.StoredEvent, error) {
	var raw cloudevent.RawEvent
	if err := raw.UnmarshalJSON(body); err != nil {
		return cloudevent.StoredEvent{}, fmt.Errorf("unmarshaling raw event: %w", err)
	}
	return cloudevent.StoredEvent{
		RawEvent:     raw,
		DataIndexKey: headers.Get(HeaderDataIndexKey),
		VoidsID:      headers.Get(HeaderVoidsID),
	}, nil
}
