package stream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

// barrierJS releases every future's Ok() only once want async submits have
// landed. It proves PublishBatch pipelines: a serial submit→await→submit loop
// would block on the first await (its future never fires until all N are
// submitted, but only one has been) and deadlock; a pipelined batch submits all
// N, the barrier fires them, and every await resolves.
type barrierJS struct {
	jetstream.JetStream
	mu      sync.Mutex
	want    int
	futures []*fakeFuture
}

func (b *barrierJS) PublishMsgAsync(*nats.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := newFuture()
	b.futures = append(b.futures, f)
	if len(b.futures) == b.want {
		for _, ff := range b.futures {
			ff.ok <- &jetstream.PubAck{} // buffered: never blocks
		}
	}
	return f, nil
}

// seqJS hands out pre-seeded futures in call order, so a test can dictate each
// event's outcome.
type seqJS struct {
	jetstream.JetStream
	futures []jetstream.PubAckFuture
	i       int
}

func (s *seqJS) PublishMsgAsync(*nats.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	f := s.futures[s.i]
	s.i++
	return f, nil
}

func batchEvents(n int) []*cloudevent.StoredEvent {
	events := make([]*cloudevent.StoredEvent, n)
	for i := range events {
		e := testStoredEvent()
		e.ID = fmt.Sprintf("id-%d", i)
		events[i] = e
	}
	return events
}

// PublishBatch must issue every PublishMsgAsync BEFORE awaiting any ack — the
// whole point of load review #2. barrierJS only completes the futures once all
// N are in flight, so a still-serial implementation would deadlock here.
func TestPublishBatch_SubmitsAllBeforeAwaiting(t *testing.T) {
	const n = 4
	p := stream.NewPublisher(&barrierJS{want: n}, 1)
	done := make(chan error, 1)
	go func() { done <- p.PublishBatch(context.Background(), batchEvents(n)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pipelined batch: want nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PublishBatch deadlocked — it awaited an ack before submitting all events (serial, not pipelined)")
	}
}

// A failed ack anywhere in the batch surfaces as ErrUnavailable so the handler
// returns 503; a max-payload rejection surfaces as ErrPayloadTooLarge (413) and
// must NOT read as the retryable ErrUnavailable. writeError checks
// ErrPayloadTooLarge first, so a mixed batch maps to 413.
func TestPublishBatch_PreservesErrorSemantics(t *testing.T) {
	okFut := func() *fakeFuture { f := newFuture(); f.ok <- &jetstream.PubAck{}; return f }
	errFut := func(e error) *fakeFuture { f := newFuture(); f.err <- e; return f }

	t.Run("failed ack is unavailable", func(t *testing.T) {
		js := &seqJS{futures: []jetstream.PubAckFuture{okFut(), errFut(errors.New("no responders"))}}
		err := stream.NewPublisher(js, 1).PublishBatch(context.Background(), batchEvents(2))
		if !errors.Is(err, stream.ErrUnavailable) {
			t.Fatalf("want ErrUnavailable, got %v", err)
		}
	})

	t.Run("max-payload is too-large, not unavailable", func(t *testing.T) {
		js := &seqJS{futures: []jetstream.PubAckFuture{okFut(), errFut(fmt.Errorf("publish: %w", nats.ErrMaxPayload))}}
		err := stream.NewPublisher(js, 1).PublishBatch(context.Background(), batchEvents(2))
		if !errors.Is(err, stream.ErrPayloadTooLarge) {
			t.Fatalf("want ErrPayloadTooLarge, got %v", err)
		}
	})

	t.Run("all acked is nil", func(t *testing.T) {
		js := &seqJS{futures: []jetstream.PubAckFuture{okFut(), okFut(), okFut()}}
		if err := stream.NewPublisher(js, 1).PublishBatch(context.Background(), batchEvents(3)); err != nil {
			t.Fatalf("want nil, got %v", err)
		}
	})
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
