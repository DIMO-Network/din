package stream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeFuture is a controllable jetstream.PubAckFuture: a test selects which arm of
// Publish's select fires by which channel it makes ready (or neither, to force ctx).
type fakeFuture struct {
	jetstream.PubAckFuture
	ok  chan *jetstream.PubAck
	err chan error
}

func (f *fakeFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *fakeFuture) Err() <-chan error            { return f.err }
func (f *fakeFuture) Msg() *nats.Msg               { return nil }

// fakeJS implements only PublishMsgAsync; any other JetStream call panics (Publish
// uses none of them), which keeps the fake minimal.
type fakeJS struct {
	jetstream.JetStream
	future   jetstream.PubAckFuture
	asyncErr error
}

func (f *fakeJS) PublishMsgAsync(*nats.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	return f.future, f.asyncErr
}

func testStoredEvent() *cloudevent.StoredEvent {
	return &cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				ID:      "id-1",
				Source:  "0xc0dec0dec0dec0dec0dec0dec0dec0dec0dec0de",
				Subject: "did:erc721:1:0xc0dec0dec0dec0dec0dec0dec0dec0dec0dec0de:1",
				Type:    "dimo.status",
				Time:    time.Unix(1700000000, 0).UTC(),
			},
			Data: json.RawMessage(`{"v":1}`),
		},
	}
}

func newFuture() *fakeFuture {
	return &fakeFuture{ok: make(chan *jetstream.PubAck, 1), err: make(chan error, 1)}
}

// A JetStream ack returns nil — the durable-publish success path.
func TestPublish_AckSucceeds(t *testing.T) {
	fut := newFuture()
	fut.ok <- &jetstream.PubAck{}
	p := stream.NewPublisher(&fakeJS{future: fut}, 1)
	if err := p.Publish(context.Background(), testStoredEvent()); err != nil {
		t.Fatalf("acked publish: want nil, got %v", err)
	}
}

// An async ack error must surface as ErrUnavailable so the handler returns 503 and the
// device retries — a regression to a bare error would 500 and silently drop the event.
func TestPublish_AckErrorIsUnavailable(t *testing.T) {
	fut := newFuture()
	fut.err <- errors.New("no responders available")
	p := stream.NewPublisher(&fakeJS{future: fut}, 1)
	err := p.Publish(context.Background(), testStoredEvent())
	if !errors.Is(err, stream.ErrUnavailable) {
		t.Fatalf("ack-error publish: want ErrUnavailable, got %v", err)
	}
}

// A lost ack (ctx deadline with no Ok/Err) must also map to ErrUnavailable.
func TestPublish_CtxTimeoutIsUnavailable(t *testing.T) {
	p := stream.NewPublisher(&fakeJS{future: newFuture()}, 1) // neither channel ever fires
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := p.Publish(ctx, testStoredEvent())
	if !errors.Is(err, stream.ErrUnavailable) {
		t.Fatalf("ctx-timeout publish: want ErrUnavailable, got %v", err)
	}
}

// PublishMsgAsync failing synchronously must also map to ErrUnavailable.
func TestPublish_AsyncErrorIsUnavailable(t *testing.T) {
	p := stream.NewPublisher(&fakeJS{asyncErr: errors.New("stream not found")}, 1)
	err := p.Publish(context.Background(), testStoredEvent())
	if !errors.Is(err, stream.ErrUnavailable) {
		t.Fatalf("async-error publish: want ErrUnavailable, got %v", err)
	}
}

// A max-payload rejection is deterministic — the identical event always fails —
// so it must map to ErrPayloadTooLarge (handler → non-retryable 413), NOT the
// retryable ErrUnavailable (which would make the device resend the same
// oversized payload forever). Both the synchronous reject and the async-future
// arm must classify it.
func TestPublish_MaxPayloadIsPayloadTooLarge(t *testing.T) {
	t.Run("sync reject", func(t *testing.T) {
		p := stream.NewPublisher(&fakeJS{asyncErr: fmt.Errorf("publish: %w", nats.ErrMaxPayload)}, 1)
		err := p.Publish(context.Background(), testStoredEvent())
		if !errors.Is(err, stream.ErrPayloadTooLarge) {
			t.Fatalf("sync max-payload: want ErrPayloadTooLarge, got %v", err)
		}
		if errors.Is(err, stream.ErrUnavailable) {
			t.Fatalf("max-payload must not be retryable ErrUnavailable: %v", err)
		}
	})
	t.Run("async future arm", func(t *testing.T) {
		fut := newFuture()
		fut.err <- fmt.Errorf("publish: %w", nats.ErrMaxPayload)
		p := stream.NewPublisher(&fakeJS{future: fut}, 1)
		err := p.Publish(context.Background(), testStoredEvent())
		if !errors.Is(err, stream.ErrPayloadTooLarge) {
			t.Fatalf("async max-payload: want ErrPayloadTooLarge, got %v", err)
		}
	})
}
