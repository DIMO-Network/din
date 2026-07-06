package sink

import (
	"context"
	"errors"
	"testing"
	"time"

	"fmt"
	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/lake"
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
	s.buffer = &eventBuffer{events: []cloudevent.StoredEvent{{}}, footprint: 200} // over ceiling (ceiling gates on resident footprint, B4)

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
	s.buffer = &eventBuffer{footprint: 10}

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
	s.buffer = &eventBuffer{events: []cloudevent.StoredEvent{{}}, footprint: 200}

	// A worker drains the handed-off bundle and reports completion (inflight drops
	// back under the ceiling), letting applyBackpressure return.
	go func() {
		job := <-s.jobs
		s.flushDone(job.footprint)
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

// The MaxBufferedBytes ceiling gates on RESIDENT memory, not payload bytes: a
// buffered message pins both the raw jetstream.Msg and the parsed StoredEvent
// (a second copy of the payload), so charging only len(msg.Data()) let a
// writer stall hold ~2.5x the ceiling and OOMKill the pod instead of
// backpressuring (B4).
func TestBufferedFootprint_ChargesBothCopiesPlusOverhead(t *testing.T) {
	payload := make([]byte, 1000)
	ev := cloudevent.StoredEvent{}
	ev.Data = payload

	got := bufferedFootprint(&ev, 1200) // wire msg is payload + envelope
	want := 1200 + 1000 + bufferedMsgOverhead
	if got != want {
		t.Fatalf("bufferedFootprint = %d, want %d (msg copy + parsed Data copy + overhead)", got, want)
	}
	if got <= 1200 {
		t.Fatal("footprint must exceed payload bytes — the B4 undercount")
	}

	// Binary wire payloads parse into DataBase64 with Data empty — the second
	// copy must still be charged.
	bin := cloudevent.StoredEvent{}
	bin.DataBase64 = string(make([]byte, 1400))
	if got := bufferedFootprint(&bin, 1500); got != 1500+1400+bufferedMsgOverhead {
		t.Fatalf("bufferedFootprint(base64) = %d, must charge the DataBase64 copy", got)
	}
}

// enqueue charges the job's footprint to inflight and flushDone releases the
// same amount — asymmetric accounting here drifts the gauge and either wedges
// backpressure permanently or disables it.
func TestEnqueueFlushDone_FootprintSymmetric(t *testing.T) {
	s := &Sink{
		cfg:     Config{MaxBufferedBytes: 1 << 20},
		jobs:    make(chan flushJob, 1),
		flushed: make(chan struct{}, 1),
	}
	s.buffer = &eventBuffer{events: []cloudevent.StoredEvent{{}}, bytes: 100, footprint: 750}

	if !s.enqueue(s.buffer, false) {
		t.Fatal("enqueue failed with a free slot")
	}
	if got := s.inflight.Load(); got != 750 {
		t.Fatalf("inflight charged %d, want the job footprint 750", got)
	}
	job := <-s.jobs
	s.flushDone(job.footprint)
	if got := s.inflight.Load(); got != 0 {
		t.Fatalf("inflight = %d after release, want 0 (no drift)", got)
	}
}

// Flush-size triggers stay PAYLOAD-driven (they size the parquet output);
// only the backpressure ceiling uses resident footprint. A buffer whose
// footprint exceeds MaxBytesPerFlush but whose payload does not must keep
// accumulating.
func TestFlushReady_TriggersOnPayloadNotFootprint(t *testing.T) {
	s := &Sink{
		cfg:     Config{MaxRowsPerFlush: 1000, MaxBytesPerFlush: 1000, MaxAge: time.Hour, MaxAgeHard: time.Hour, MinFlushBytes: 1},
		jobs:    make(chan flushJob, 1),
		flushed: make(chan struct{}, 1),
	}
	s.buffer = &eventBuffer{
		events:    []cloudevent.StoredEvent{{}},
		bytes:     500,  // under the flush threshold
		footprint: 1500, // resident is over it — must NOT trigger a flush
		firstAt:   time.Now(),
	}
	s.flushReady(false)
	if s.buffer == nil {
		t.Fatal("flushed on footprint; flush sizing must follow payload bytes")
	}

	s.buffer.bytes = 1000 // payload reaches the threshold
	s.flushReady(false)
	if s.buffer != nil {
		t.Fatal("did not flush once payload hit MaxBytesPerFlush")
	}
}

// The default MaxBufferedBytes is a POD budget: a process running one sink per
// WAL partition divides it across PodSinks so raising the partition count
// cannot multiply pod memory (B4). An explicit ceiling is used as-is.
func TestApplyDefaults_DividesPodBudgetAcrossSinks(t *testing.T) {
	c := Config{PodSinks: 2}
	c.applyDefaults()
	// P=2 divides cleanly: 4x/2 = 2x... but the pipelining floor (3x) wins —
	// the footprint-denominated ceiling must fit one in-flight bundle (~2x
	// payload) plus an accumulating half, or partial buffers force-flush and
	// commits serialize behind a single bundle (adversarial review #1).
	if want := 3 * c.MaxBytesPerFlush; c.MaxBufferedBytes != want {
		t.Fatalf("MaxBufferedBytes = %d, want the 3x pipelining floor = %d", c.MaxBufferedBytes, want)
	}

	one := Config{PodSinks: 1}
	one.applyDefaults()
	if want := 4 * one.MaxBytesPerFlush; one.MaxBufferedBytes != want {
		t.Fatalf("single sink keeps the full pod budget, got %d want %d", one.MaxBufferedBytes, want)
	}

	many := Config{PodSinks: 8}
	many.applyDefaults()
	if want := 3 * many.MaxBytesPerFlush; many.MaxBufferedBytes != want {
		t.Fatalf("deep division must clamp at the pipelining floor, got %d want %d", many.MaxBufferedBytes, want)
	}

	explicit := Config{PodSinks: 4, MaxBufferedBytes: 999}
	explicit.applyDefaults()
	if explicit.MaxBufferedBytes != 999 {
		t.Fatalf("explicit MaxBufferedBytes overridden: %d", explicit.MaxBufferedBytes)
	}
}

// termMsg records Term/Ack so tests can assert the isolate leaf's decision.
type termMsg struct {
	jetstream.Msg
	delivered uint64
	termed    bool
	acked     bool
}

func (m *termMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.delivered}, nil
}
func (m *termMsg) Term() error { m.termed = true; return nil }
func (m *termMsg) Ack() error  { m.acked = true; return nil }

// TestIsolate_OutageNeverTermsHealthyRows pins the round-2 CRITICAL: during a
// global writer/catalog outage, redelivery count alone must NEVER Term
// (permanently destroy) a single-row leaf — NumDelivered keeps rising on
// healthy, already-HTTP-200-acknowledged events for the whole outage and
// survives pod restarts. Terming on count requires EVIDENCE the writer is
// alive: a sibling range committed in the same pass (st.committed > 0), or
// the writer deterministically tagged the row (ErrPoisonRow).
func TestIsolate_OutageNeverTermsHealthyRows(t *testing.T) {
	ctx := context.Background()

	newJob := func(delivered uint64) (flushJob, *termMsg) {
		msg := &termMsg{delivered: delivered}
		return flushJob{
			events: []cloudevent.StoredEvent{{}},
			msgs:   []jetstream.Msg{msg},
		}, msg
	}

	// Global outage: every write fails transiently, nothing has committed.
	// Even at 10x the poison threshold, the row must survive for redelivery.
	s := &Sink{cfg: Config{}, writer: &countingFailWriter{}, log: zerolog.Nop()}
	job, msg := newJob(poisonRedeliveryThreshold * 10)
	s.isolateRange(ctx, job, 0, 1, &isolateState{})
	if msg.termed {
		t.Fatal("outage + high redelivery count Term'd a healthy row — permanent data loss regression")
	}
	if msg.acked {
		t.Fatal("failed row must stay un-acked for redelivery")
	}

	// Writer provably alive (a sibling committed this pass): the same row
	// failing alone past the threshold IS evidence it can't be persisted.
	job, msg = newJob(poisonRedeliveryThreshold)
	s.isolateRange(ctx, job, 0, 1, &isolateState{committed: 1})
	if !msg.termed {
		t.Fatal("threshold-exceeded row with a live writer must be quarantined")
	}

	// Deterministic tag Terms regardless of outage state.
	job, msg = newJob(1)
	poison := &Sink{cfg: Config{}, writer: poisonWriter{}, log: zerolog.Nop()}
	poison.isolateRange(ctx, job, 0, 1, &isolateState{})
	if !msg.termed {
		t.Fatal("ErrPoisonRow-tagged row must be quarantined even with committed == 0")
	}
}

// poisonWriter always rejects with the deterministic poison tag.
type poisonWriter struct{}

func (poisonWriter) WriteBundle(context.Context, []cloudevent.StoredEvent) error {
	return fmt.Errorf("bad row: %w", lake.ErrPoisonRow)
}
