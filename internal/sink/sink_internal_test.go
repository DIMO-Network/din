package sink

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
)

// applyBackpressure must block the Run goroutine while the buffer is over the
// hard byte ceiling and no flush slot is free, so messages stop being pulled
// from JetStream and buffered memory stays bounded instead of growing until OOM
// (SR-8). It must preserve the buffer while blocked and return when ctx is done.
func TestApplyBackpressure_BlocksOverCeilingUntilCtxDone(t *testing.T) {
	s := &Sink{
		cfg:  Config{MaxBufferedBytes: 100},
		jobs: make(chan flushJob), // unbuffered, no worker draining → no free slot
	}
	s.buffer = &eventBuffer{events: []cloudevent.StoredEvent{{}}, bytes: 200} // over ceiling

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.applyBackpressure(ctx); close(done) }()

	select {
	case <-done:
		t.Fatal("returned without backpressuring while over ceiling and jobs full")
	case <-time.After(50 * time.Millisecond):
	}
	if s.buffer == nil {
		t.Fatal("buffer dropped while backpressured")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not return after ctx cancel")
	}
}

// Under the ceiling it is a no-op even with no free flush slot.
func TestApplyBackpressure_NoopUnderCeiling(t *testing.T) {
	s := &Sink{cfg: Config{MaxBufferedBytes: 1000}, jobs: make(chan flushJob)}
	s.buffer = &eventBuffer{bytes: 10}

	done := make(chan struct{})
	go func() { s.applyBackpressure(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked under the ceiling")
	}
}

// When a flush slot is free, it hands the over-ceiling buffer off and clears it.
func TestApplyBackpressure_HandsOffWhenSlotFree(t *testing.T) {
	s := &Sink{
		cfg:  Config{MaxBufferedBytes: 100},
		jobs: make(chan flushJob, 1), // one free slot
	}
	s.buffer = &eventBuffer{events: []cloudevent.StoredEvent{{}}, bytes: 200}

	s.applyBackpressure(context.Background())
	if s.buffer != nil {
		t.Fatal("buffer not handed off despite a free slot")
	}
	if len(s.jobs) != 1 {
		t.Fatalf("expected 1 queued job, got %d", len(s.jobs))
	}
}
