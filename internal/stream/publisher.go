package stream

import (
	"context"
	"errors"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Header names carried on every published message so header-only consumers
// can route without unmarshaling the body.
const (
	HeaderCEType    = "ce-type"
	HeaderCESubject = "ce-subject"
	HeaderCESource  = "ce-source"
	HeaderCEID      = "ce-id"
	HeaderVoidsID   = "din-voids-id"
)

// ErrUnavailable reports that JetStream did not acknowledge the publish in
// time. Handlers map it to 503 so devices retry.
var ErrUnavailable = errors.New("jetstream publish not acknowledged")

// Publisher writes raw cloudevents to the INGEST_RAW stream and waits for
// the JetStream ack — the durability point of the ingest path.
type Publisher struct {
	js jetstream.JetStream
}

// NewPublisher returns a Publisher on the given JetStream context.
func NewPublisher(js jetstream.JetStream) *Publisher {
	return &Publisher{js: js}
}

// Publish sends one validated event and blocks until JetStream acks it or
// ctx expires. voidsID may be empty; when set it is carried as a header for
// tombstone-aware consumers.
func (p *Publisher) Publish(ctx context.Context, event *cloudevent.RawEvent, voidsID string) error {
	body, err := event.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshaling event %s: %w", event.ID, err)
	}

	msg := &nats.Msg{
		Subject: Subject(&event.CloudEventHeader),
		Data:    body,
		Header: nats.Header{
			nats.MsgIdHdr:   []string{MsgID(&event.CloudEventHeader)},
			HeaderCEType:    []string{event.Type},
			HeaderCESubject: []string{event.Subject},
			HeaderCESource:  []string{event.Source},
			HeaderCEID:      []string{event.ID},
		},
	}
	if voidsID != "" {
		msg.Header.Set(HeaderVoidsID, voidsID)
	}

	future, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	select {
	case <-future.Ok():
		return nil
	case err := <-future.Err():
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrUnavailable, ctx.Err())
	}
}
