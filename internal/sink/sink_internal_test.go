package sink

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
)

// countingFailWriter fails every write and counts the attempts.
type countingFailWriter struct{ calls int }

func (w *countingFailWriter) WriteBundle(context.Context, []cloudevent.StoredEvent) error {
	w.calls++
	return errors.New("global fault")
}

// probeMsg is a minimal jetstream.Msg: only Metadata is exercised on the
// all-fail isolate path (under the poison threshold, so no Ack/Term).
type probeMsg struct {
	jetstream.Msg
	delivered uint64
}

func (m probeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.delivered}, nil
}

// flushReady's age trigger is adaptive: a buffer past the soft MaxAge flushes
// only once it holds MinFlushBytes (so low-traffic partitions don't emit tiny
// Parquet files), but the hard MaxAgeHard cap flushes regardless of size.
func TestFlushReady_AdaptiveAgeTrigger(t *testing.T) {
	mk := func() *Sink {
		return &Sink{
			cfg: Config{
				MaxRowsPerFlush: 100_000, MaxBytesPerFlush: 128 << 20,
				MinFlushBytes: 16 << 20, MaxAge: time.Minute, MaxAgeHard: 5 * time.Minute,
			},
			jobs: make(chan flushJob, 1),
		}
	}
	buf := func(bytes int, age time.Duration) *eventBuffer {
		return &eventBuffer{events: []cloudevent.StoredEvent{{}}, bytes: bytes, firstAt: time.Now().Add(-age)}
	}
	flushed := func(b *eventBuffer) bool {
		s := mk()
		s.buffer = b
		s.flushReady(false)
		return len(s.jobs) == 1
	}

	if flushed(buf(1<<20, 2*time.Minute)) {
		t.Fatal("a 1 MiB buffer past the soft age flushed before the hard cap (tiny file)")
	}
	if !flushed(buf(20<<20, 2*time.Minute)) {
		t.Fatal("a 20 MiB buffer past the soft age did not flush")
	}
	if !flushed(buf(1<<20, 6*time.Minute)) {
		t.Fatal("a buffer past the hard age cap did not flush regardless of size")
	}
}

// On a global fault (every write fails, nothing commits) isolate must stop after
// the probe instead of grinding the whole bundle into N single-row transactions
// — the bundle redelivers wholesale anyway (SR review — isolate-grind).
func TestIsolate_BailsOutOnGlobalFault(t *testing.T) {
	w := &countingFailWriter{}
	s := &Sink{writer: w, log: zerolog.Nop()}
	const n = 100
	job := flushJob{
		events: make([]cloudevent.StoredEvent, n),
		msgs:   make([]jetstream.Msg, n),
	}
	for i := range job.msgs {
		job.msgs[i] = probeMsg{delivered: 1} // under the poison threshold
	}
	s.isolate(context.Background(), job)
	if w.calls > isolateProbeLimit {
		t.Fatalf("isolate ground %d writes on a global fault; want <= %d (bail-out)", w.calls, isolateProbeLimit)
	}
}

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
