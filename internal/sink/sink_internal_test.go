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

// seqWriter commits the first bundle, then cancels its context and fails the rest —
// simulating a drain-timeout cancellation arriving mid-isolate after >=1 commit.
type seqWriter struct {
	calls  int
	cancel context.CancelFunc
}

func (w *seqWriter) WriteBundle(context.Context, []cloudevent.StoredEvent) error {
	w.calls++
	if w.calls == 1 {
		return nil
	}
	w.cancel()
	return context.Canceled
}

// trackMsg records Term so a test can assert a row was left for redelivery.
type trackMsg struct {
	jetstream.Msg
	delivered  uint64
	terminated bool
}

func (m *trackMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.delivered}, nil
}
func (m *trackMsg) Term() error { m.terminated = true; return nil }
func (m *trackMsg) Ack() error  { return nil }

// On a drain-timeout context cancellation, isolate's "committed > 0 ⇒ poison"
// heuristic is invalid: the remaining writes failed because ctx was canceled, not
// because the writer rejected them. Healthy never-persisted rows must be left
// un-acked for JetStream redelivery, NOT Term'd (silent data loss otherwise).
func TestIsolate_LeavesRowsForRedeliveryOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sink{writer: &seqWriter{cancel: cancel}, log: zerolog.Nop()}
	const n = 4
	msgs := make([]*trackMsg, n)
	job := flushJob{events: make([]cloudevent.StoredEvent, n), msgs: make([]jetstream.Msg, n)}
	for i := range msgs {
		msgs[i] = &trackMsg{delivered: 1} // under the poison threshold
		job.msgs[i] = msgs[i]
	}
	s.isolate(ctx, job)
	for i, m := range msgs {
		if m.terminated {
			t.Fatalf("msg %d Term'd under context cancellation; healthy rows must survive for redelivery", i)
		}
	}
}

// cancelAllWriter cancels its context on the first write and fails every write —
// simulating a drain-timeout cancellation that lands before any commit, driving
// isolate into the committed==0 global-fault bail-out.
type cancelAllWriter struct{ cancel context.CancelFunc }

func (w *cancelAllWriter) WriteBundle(context.Context, []cloudevent.StoredEvent) error {
	w.cancel()
	return context.Canceled
}

// The committed==0 global-fault bail-out must ALSO respect cancellation: rows past
// the redelivery threshold whose write failed only because ctx was canceled
// (drain/shutdown) must be left for redelivery, not Term'd as expired poison.
func TestIsolate_BailoutLeavesRowsForRedeliveryOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sink{writer: &cancelAllWriter{cancel: cancel}, log: zerolog.Nop()}
	n := isolateProbeLimit + 2 // enough failures to trip the bail-out
	msgs := make([]*trackMsg, n)
	job := flushJob{events: make([]cloudevent.StoredEvent, n), msgs: make([]jetstream.Msg, n)}
	for i := range msgs {
		msgs[i] = &trackMsg{delivered: poisonRedeliveryThreshold} // past the threshold
		job.msgs[i] = msgs[i]
	}
	s.isolate(ctx, job)
	for i, m := range msgs {
		if m.terminated {
			t.Fatalf("msg %d Term'd via the global-fault bail-out under cancellation; past-threshold rows must survive for redelivery when the failure is cancellation", i)
		}
	}
}

// applyBackpressure must block the Run goroutine while the buffer is over the
// hard byte ceiling and no flush slot is free, so messages stop being pulled
// from JetStream and buffered memory stays bounded instead of growing until OOM
// (SR-8). It must preserve the buffer while blocked and return when ctx is done.
func TestApplyBackpressure_BlocksOverCeilingUntilCtxDone(t *testing.T) {
	s := &Sink{
		cfg:     Config{MaxBufferedBytes: 100},
		jobs:    make(chan flushJob), // unbuffered, no worker draining → no free slot
		flushed: make(chan struct{}, 1),
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
	s := &Sink{cfg: Config{MaxBufferedBytes: 1000}, jobs: make(chan flushJob), flushed: make(chan struct{}, 1)}
	s.buffer = &eventBuffer{bytes: 10}

	done := make(chan struct{})
	go func() { s.applyBackpressure(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked under the ceiling")
	}
}

// When a flush slot is free it hands the over-ceiling buffer off, but it stays
// blocked until a worker actually drains the bundle — handing off only relocates
// the bytes into the queue, so total un-acked memory is still over the ceiling.
func TestApplyBackpressure_HandsOffThenReturnsOnDrain(t *testing.T) {
	s := &Sink{
		cfg:     Config{MaxBufferedBytes: 100},
		jobs:    make(chan flushJob, 1), // one free slot
		flushed: make(chan struct{}, 1),
	}
	s.buffer = &eventBuffer{events: []cloudevent.StoredEvent{{}}, bytes: 200}

	// A worker drains the handed-off bundle and reports completion (inflight drops
	// back under the ceiling), letting applyBackpressure return.
	go func() {
		job := <-s.jobs
		s.flushDone(job.bytes)
	}()

	s.applyBackpressure(context.Background()) // owns s.buffer; the worker only touches jobs/inflight/flushed

	if s.buffer != nil {
		t.Fatal("buffer not handed off despite a free slot")
	}
	if got := s.inflight.Load(); got != 0 {
		t.Fatalf("inflight not released after drain, got %d", got)
	}
}

// The ceiling must bound the whole flush queue, not just the current buffer: with
// the queue already over the ceiling (the OLD per-buffer check would have ignored
// it and let the Workers*8-deep channel fill to ~5x the ceiling), a small buffer
// must still block until the queue drains.
func TestApplyBackpressure_GatesOnQueueNotJustBuffer(t *testing.T) {
	s := &Sink{
		cfg:     Config{MaxBufferedBytes: 100},
		jobs:    make(chan flushJob, 8), // free slots — the old check would not have blocked
		flushed: make(chan struct{}, 1),
	}
	s.inflight.Store(150) // queue already over the ceiling, no current buffer

	done := make(chan struct{})
	go func() { s.applyBackpressure(context.Background()); close(done) }()
	select {
	case <-done:
		t.Fatal("returned while the queue is over the ceiling (per-buffer check, queue ignored)")
	case <-time.After(50 * time.Millisecond):
	}

	s.flushDone(150) // worker drains the queue under the ceiling
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not return after the queue drained under the ceiling")
	}
}
