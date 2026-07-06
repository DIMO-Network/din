// Package sink consumes the INGEST_RAW stream and persists raw cloudevents
// into the DuckLake raw_events table. Messages are acknowledged only after
// the bundle containing them is durably committed, giving at-least-once
// delivery end to end; duplicate rows from redelivery die in readers'
// dedup (DuckLake compaction does not deduplicate).
package sink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/lake"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

// BundleWriter durably persists one batch of events; once it returns,
// acking the batch's messages is safe. Implemented by lake.Writer.
type BundleWriter interface {
	WriteBundle(ctx context.Context, events []cloudevent.StoredEvent) error
}

// redeliveries counts messages seen more than once. Redelivered rows can
// land in two committed bundles and persist as duplicates, so this is the
// observable bound on duplicate volume.
var redeliveries = promauto.NewCounter(prometheus.CounterOpts{
	Name: "din_sink_redeliveries_total",
	Help: "JetStream messages delivered to the sink more than once.",
})

// poisonRows counts messages terminated because the writer could not persist
// them (a row it deterministically rejects: bad UTF-8, type/precision, schema
// drift). Terminating them stops one bad row from wedging a whole WAL partition
// via infinite redelivery (SR review #1).
var poisonRows = promauto.NewCounter(prometheus.CounterOpts{
	Name: "din_sink_poison_rows_total",
	Help: "Raw events terminated because the writer could not persist them.",
})

// commitsTotal / commitFailuresTotal make the sink's write health directly
// observable (H15/H3): before them the only signal of a dead writer was
// redeliveries, ~an AckWait late and warning-severity.
var commitsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "din_sink_commits_total",
	Help: "Bundles committed to the lake.",
})

var commitFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "din_sink_commit_failures_total",
	Help: "Bundle commits that failed (before per-event isolation).",
})

// poisonRedeliveryThreshold bounds how many times a message that fails to
// commit even on its own is redelivered before it is terminated as poison.
// High enough that a transient writer/catalog outage — each redelivery waits
// one AckWait — resolves before a healthy message is ever dropped.
const poisonRedeliveryThreshold = 10

// isolateProbeLimit bounds the per-event isolation probe. If this many single-row
// writes fail with nothing committed, the fault is almost certainly global
// (catalog/S3 unreachable), not a poison row: grinding the rest into N single-row
// transactions only holds the writer lock and mints no progress, and the bundle
// redelivers wholesale anyway. A healthy writer commits something within the
// first few rows (poison is rare), so this only short-circuits a true outage.
const isolateProbeLimit = 8

// maxConsecutiveFetchErrs / fetchRetryBackoff bound how a JetStream Fetch error is
// handled: a transient NATS disconnect is retried with backoff (the connection
// reconnects forever) rather than crash-looping the pod, but a persistently failing
// fetch trips the backstop (~maxConsecutiveFetchErrs * fetchRetryBackoff) and exits so
// the pod restarts and re-asserts its consumer.
const (
	maxConsecutiveFetchErrs = 30
	fetchRetryBackoff       = 2 * time.Second
)

// DefaultMaxAgeHard is the default hard flush cap (Config.MaxAgeHard when unset).
// Exported so the consumer's AckWait can be sized above it: a buffer is acked only
// after it flushes, and a low-traffic buffer isn't flushed until MaxAgeHard, so
// AckWait <= MaxAgeHard would redeliver every quiet-partition message before its
// ack lands.
const DefaultMaxAgeHard = 5 * time.Minute

// Config tunes batching. Zero values take the defaults.
type Config struct {
	// MaxRowsPerFlush flushes the buffer at this row count.
	MaxRowsPerFlush int
	// MaxBytesPerFlush flushes the buffer at this accumulated payload
	// size. DuckLake splits each bundle across partitions itself, so a
	// single buffer needs no per-partition cap.
	MaxBytesPerFlush int
	// MaxBufferedBytes is the hard ceiling on RESIDENT buffered memory (not just
	// payload): a buffered message retains both the raw jetstream.Msg and the
	// parsed StoredEvent, so it is charged bufferedFootprint bytes, ~2x its
	// payload. When the writer stalls and flush slots fill, the Run loop blocks on
	// this ceiling so JetStream backpressure propagates instead of the buffer
	// growing until OOM (SR-8, B4). Per-SINK: when unset, the 4x MaxBytesPerFlush
	// default is treated as the POD budget and divided by PodSinks, so the pod
	// total stays bounded regardless of partition count. An explicit value is
	// used as-is per sink — size it yourself.
	MaxBufferedBytes int
	// PodSinks is how many sibling sinks this process runs (one per WAL
	// partition). Only used to divide the default MaxBufferedBytes pod budget;
	// zero means 1.
	PodSinks int
	// MaxAge is the soft age trigger: flush a buffer this old only once it has
	// accumulated at least MinFlushBytes, so a low-traffic partition doesn't emit
	// a tiny Parquet file every MaxAge. MaxAgeHard is the hard cap.
	MaxAge time.Duration
	// MinFlushBytes gates the MaxAge (soft) trigger — a buffer below this size
	// keeps accumulating past MaxAge (up to MaxAgeHard) rather than flushing tiny.
	MinFlushBytes int
	// MaxAgeHard force-flushes the buffer this long after its first event,
	// regardless of size — the latency bound for very-low-traffic partitions.
	MaxAgeHard time.Duration
	// Workers is the number of flush workers. Bundles serialize on the
	// writer's connection; extra workers only deepen the queue between
	// fetching and committing.
	Workers int
	// FetchBatch is the max messages per JetStream fetch.
	FetchBatch int
	// DrainTimeout bounds the shutdown flush. Past it, in-flight commits
	// are canceled and their messages redeliver (at-least-once holds).
	DrainTimeout time.Duration
	// FlushTimeout bounds one bundle's commit (WriteBundle + per-event
	// isolation). Without it, a wedged S3 multipart / catalog commit pins
	// that writer connection's mutex indefinitely and every bundle round-
	// robined onto it queues behind the wedge (H15). Keep it under the
	// consumer AckWait so a timed-out bundle redelivers cleanly.
	FlushTimeout time.Duration
	// CommitFailureWindow bounds how long EVERY commit may keep failing
	// before Run returns an error so the pod restarts and re-pins its
	// writer connections. A catalog failover can poison the pinned conns
	// permanently (SetConnMaxLifetime cannot recycle a checked-out conn) —
	// without this backstop the sink grinds redeliveries forever while the
	// pod sits Ready (H15). Partial isolate commits count as progress.
	CommitFailureWindow time.Duration
}

func (c *Config) applyDefaults() {
	if c.MaxRowsPerFlush == 0 {
		c.MaxRowsPerFlush = 100_000
	}
	if c.MaxBytesPerFlush == 0 {
		c.MaxBytesPerFlush = 128 << 20
	}
	if c.MaxBufferedBytes == 0 {
		// Headroom for normal bursts above the flush size, but a firm cap so
		// a stalled writer can't grow the buffer without bound. The default is
		// a POD budget: a process running one sink per WAL partition divides
		// it across PodSinks siblings so pod-total resident buffer memory
		// stays 4x MaxBytesPerFlush regardless of partition count (B4).
		c.MaxBufferedBytes = 4 * c.MaxBytesPerFlush
		if c.PodSinks > 1 {
			c.MaxBufferedBytes /= c.PodSinks
		}
	}
	if c.MaxAge == 0 {
		c.MaxAge = time.Minute
	}
	if c.MinFlushBytes == 0 {
		c.MinFlushBytes = 16 << 20 // 16 MiB — well above "tiny", well below the 128 MiB target
	}
	if c.FlushTimeout == 0 {
		c.FlushTimeout = 5 * time.Minute // < AckWait (MaxAgeHard+3m), so timeouts redeliver cleanly
	}
	if c.CommitFailureWindow == 0 {
		c.CommitFailureWindow = 10 * time.Minute
	}
	if c.MaxAgeHard == 0 {
		c.MaxAgeHard = DefaultMaxAgeHard
	}
	if c.Workers == 0 {
		c.Workers = 2
	}
	if c.FetchBatch == 0 {
		c.FetchBatch = 1000
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 30 * time.Second
	}
}

// Sink runs the fetch → batch → flush pipeline.
type Sink struct {
	cfg      Config
	consumer jetstream.Consumer
	writer   BundleWriter
	log      zerolog.Logger

	// buffer is owned by the Run goroutine; no locking needed.
	buffer *eventBuffer

	jobs chan flushJob
	wg   sync.WaitGroup

	// inflight is the byte size of every bundle handed to a worker but not yet
	// committed+acked (queued in jobs or in flight). Backpressure gates on
	// buffer + inflight so MaxBufferedBytes bounds total un-acked memory, not just
	// the current buffer — the jobs channel is Workers*8 deep and would otherwise
	// hold that many full bundles past the ceiling.
	inflight atomic.Int64
	// flushed wakes applyBackpressure when a worker finishes a bundle (inflight
	// dropped). Buffered to Workers so a worker never blocks signaling.
	flushed chan struct{}

	// commitFailFirst is the UnixNano of the first commit failure of the
	// current zero-progress streak (0 = healthy). When the streak spans
	// CommitFailureWindow, a worker posts to fatal and Run exits so the pod
	// restarts and re-pins its writer connections (H15).
	commitFailFirst atomic.Int64
	// fatal carries the writer-durably-broken error to Run. Buffered so a
	// worker never blocks reporting it.
	fatal chan error

	// drainDeadline bounds the whole shutdown flush; set once by Run.
	drainDeadline time.Time
}

type eventBuffer struct {
	events []cloudevent.StoredEvent
	msgs   []jetstream.Msg
	// bytes is payload (wire) bytes — it drives the flush-size triggers,
	// which target parquet output size. footprint is estimated RESIDENT
	// bytes (raw msg + parsed copy + overhead) — it drives the
	// MaxBufferedBytes backpressure ceiling, which bounds pod memory (B4).
	bytes     int
	footprint int
	firstAt   time.Time
}

type flushJob struct {
	events    []cloudevent.StoredEvent
	msgs      []jetstream.Msg
	bytes     int
	footprint int
}

// New constructs a Sink reading from consumer and committing via writer.
func New(cfg Config, consumer jetstream.Consumer, writer BundleWriter, log zerolog.Logger) *Sink {
	cfg.applyDefaults()
	return &Sink{
		cfg:      cfg,
		consumer: consumer,
		writer:   writer,
		log:      log.With().Str("component", "sink").Logger(),
		jobs:     make(chan flushJob, cfg.Workers*8),
		flushed:  make(chan struct{}, cfg.Workers),
		fatal:    make(chan error, 1),
	}
}

// Run processes messages until ctx is canceled, then flushes everything
// still buffered and drains the workers.
func (s *Sink) Run(ctx context.Context) error {
	// Workers outlive ctx so the shutdown flush below can still commit,
	// but cancelWorkers bounds them: a hung commit must not block exit
	// forever.
	workerCtx, cancelWorkers := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWorkers()
	for range s.cfg.Workers {
		s.wg.Add(1)
		go s.worker(workerCtx)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	msgs := make(chan jetstream.Msg, s.cfg.FetchBatch)
	fetchDone := make(chan error, 1)
	go s.fetchLoop(ctx, msgs, fetchDone)

loop:
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				break loop
			}
			s.add(msg)
			s.flushReady(false)
			s.applyBackpressure(ctx)
		case <-ticker.C:
			s.flushReady(false)
		case err := <-s.fatal:
			// The writer has committed NOTHING for CommitFailureWindow —
			// a failover-poisoned pinned connection never heals in-process
			// (H15). Stop the workers, return the error so the pod restarts
			// and re-pins; unacked messages redeliver (at-least-once holds).
			cancelWorkers()
			close(s.jobs)
			s.wg.Wait()
			return err
		}
	}
	fetchErr := <-fetchDone

	// Shutdown: flush the remaining buffer, then wait for workers so all
	// acks land before the process exits — but only up to DrainTimeout
	// TOTAL (one shared deadline), so a wedged commit can't hang
	// shutdown (unflushed messages just redeliver).
	s.drainDeadline = time.Now().Add(s.cfg.DrainTimeout)
	s.flushReady(true)
	close(s.jobs)
	workersDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(workersDone)
	}()
	// One shared budget for the whole drain: the blocking enqueue in
	// flushReady(true) already consumed part of drainDeadline, so wait only the
	// remainder here (not a fresh DrainTimeout) — otherwise the total drain can
	// reach 2× DrainTimeout and overrun the pod's termination grace period.
	select {
	case <-workersDone:
	case <-time.After(time.Until(s.drainDeadline)):
		s.log.Warn().Dur("timeout", s.cfg.DrainTimeout).Msg("drain timeout; canceling in-flight flushes")
		cancelWorkers()
		<-workersDone
	}

	if fetchErr != nil && !errors.Is(fetchErr, context.Canceled) {
		return fmt.Errorf("sink fetch loop: %w", fetchErr)
	}
	return nil
}

func (s *Sink) fetchLoop(ctx context.Context, out chan<- jetstream.Msg, done chan<- error) {
	defer close(out)
	consecutiveErrs := 0
	for {
		if ctx.Err() != nil {
			done <- ctx.Err()
			return
		}
		batch, err := s.consumer.Fetch(s.cfg.FetchBatch, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			// A transient NATS disconnect (the connection reconnects forever) surfaces
			// here, not as a context error. Crashing the pod on it would churn a
			// recoverable condition — log, back off, and retry. Only a persistently
			// failing fetch (e.g. the consumer was deleted) trips the backstop and
			// exits so the pod restarts and re-asserts the consumer.
			consecutiveErrs++
			if consecutiveErrs >= maxConsecutiveFetchErrs {
				done <- fmt.Errorf("sink fetch failed %dx consecutively: %w", consecutiveErrs, err)
				return
			}
			s.log.Warn().Err(err).Int("consecutive", consecutiveErrs).Msg("sink fetch failed; retrying")
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			case <-time.After(fetchRetryBackoff):
			}
			continue
		}
		consecutiveErrs = 0
		for msg := range batch.Messages() {
			select {
			case out <- msg:
			case <-ctx.Done():
				done <- ctx.Err()
				return
			}
		}
	}
}

// add decodes one message into the buffer. Undecodable messages are
// terminated: redelivery cannot fix them and they must not block acks.
func (s *Sink) add(msg jetstream.Msg) {
	event, err := stream.ParseMsg(msg.Headers(), msg.Data())
	if err != nil {
		s.log.Error().Err(err).Str("subject", msg.Subject()).Msg("terminating undecodable message")
		if termErr := msg.Term(); termErr != nil {
			s.log.Error().Err(termErr).Msg("terminating message failed")
		}
		return
	}
	if meta, err := msg.Metadata(); err != nil {
		// Don't let a metadata failure silently blind the duplicate-volume SLO —
		// metadata is least available exactly when the broker is stressed and
		// redelivery is most likely.
		s.log.Debug().Err(err).Msg("redelivery metadata unavailable; duplicate-volume metric may undercount")
	} else if meta.NumDelivered > 1 {
		redeliveries.Inc()
		// Debug, not a label: the DinSinkRedeliveriesHigh alert says redeliveries are
		// happening, but only this names which subject flaps. Debug keeps it off the hot
		// path by default (no storm) and out of metric cardinality, available on demand.
		s.log.Debug().Str("subject", msg.Subject()).Uint64("delivery", meta.NumDelivered).
			Msg("redelivered message")
	}

	if s.buffer == nil {
		s.buffer = &eventBuffer{firstAt: time.Now()}
	}
	s.buffer.events = append(s.buffer.events, event)
	s.buffer.msgs = append(s.buffer.msgs, msg)
	s.buffer.bytes += len(msg.Data())
	s.buffer.footprint += bufferedFootprint(&event, len(msg.Data()))
}

// bufferedMsgOverhead approximates the fixed per-message resident cost beyond
// the payload copies: jetstream.Msg internals (subject/reply strings, header
// map, metadata) plus the StoredEvent struct and its decoded header-string
// copies.
const bufferedMsgOverhead = 512

// bufferedFootprint estimates the resident memory one buffered message pins.
// The buffer retains BOTH the raw jetstream.Msg (its Data slice) AND the
// parsed StoredEvent, whose Data json.RawMessage is a second copy of the
// payload — so a buffered message costs ~2x its wire size, not 1x. The
// MaxBufferedBytes ceiling gates on this, not on payload bytes: counting only
// len(msg.Data()) let a writer stall hold ~2.5x the configured ceiling and
// OOMKill the pod in exactly the scenario backpressure exists for (B4).
func bufferedFootprint(ev *cloudevent.StoredEvent, msgLen int) int {
	return msgLen + len(ev.Data) + bufferedMsgOverhead
}

// flushReady enqueues the buffer once it hits a trigger (or always when
// force is set). Enqueues never block the Run goroutine: when the workers
// are saturated the buffer stays in place and retries next tick —
// blocking here would stall JetStream fetching behind a slow commit and
// let buffered bytes grow without bound.
func (s *Sink) flushReady(force bool) {
	buf := s.buffer
	if buf == nil || len(buf.events) == 0 {
		return
	}
	age := time.Since(buf.firstAt)
	if force ||
		len(buf.events) >= s.cfg.MaxRowsPerFlush ||
		buf.bytes >= s.cfg.MaxBytesPerFlush ||
		(age >= s.cfg.MaxAge && buf.bytes >= s.cfg.MinFlushBytes) ||
		age >= s.cfg.MaxAgeHard {
		s.enqueue(buf, force)
	}
}

// enqueue hands the buffer to the flush workers. Non-blocking by default;
// the shutdown drain (block=true) waits up to the drain deadline so a
// wedged writer can't hang exit — whatever doesn't flush redelivers.
func (s *Sink) enqueue(buf *eventBuffer, block bool) bool {
	job := flushJob{events: buf.events, msgs: buf.msgs, bytes: buf.bytes, footprint: buf.footprint}
	if block {
		wait := time.Until(s.drainDeadline)
		if wait <= 0 {
			return false
		}
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case s.jobs <- job:
		case <-t.C:
			return false
		}
	} else {
		select {
		case s.jobs <- job:
		default:
			return false
		}
	}
	s.inflight.Add(int64(job.footprint))
	s.buffer = nil
	return true
}

// applyBackpressure blocks the Run goroutine while the buffer sits above the
// hard byte ceiling and no flush slot is free. Blocking here stops draining the
// msgs channel, which fills and stalls the fetch loop, propagating backpressure
// to JetStream — bounding buffered memory instead of letting a stalled writer
// grow it without limit (SR-8). Returns immediately under the ceiling, on
// hand-off, or when ctx is done (the shutdown drain then flushes what remains).
func (s *Sink) applyBackpressure(ctx context.Context) {
	if s.cfg.MaxBufferedBytes <= 0 {
		return
	}
	for {
		bufBytes := 0
		if s.buffer != nil {
			bufBytes = s.buffer.footprint
		}
		if bufBytes+int(s.inflight.Load()) < s.cfg.MaxBufferedBytes {
			return
		}
		// Over the ceiling: hand the buffer off if a flush slot is free (frees the
		// Run goroutine's largest single chunk), then wait for a worker to finish a
		// bundle before accepting more. Blocking here stalls the msgs channel and
		// propagates backpressure to JetStream — bounding buffer+queue+in-flight at
		// MaxBufferedBytes, not just the current buffer (SR-8).
		if s.buffer != nil {
			s.enqueue(s.buffer, false)
		}
		select {
		case <-s.flushed:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Sink) worker(ctx context.Context) {
	defer s.wg.Done()
	for job := range s.jobs {
		s.flush(ctx, job)
	}
}

// flush commits one bundle, then acks its messages. On a bundle failure it
// isolates the cause per-event (the bundle's transaction rolled back atomically,
// so retries write fresh rows): see isolate. The old behavior — ack nothing,
// redeliver the whole bundle — wedged a partition forever when one row was
// unpersistable (no MaxDeliver, no Term on this path), since the bad row
// re-formed the same failing bundle on every redelivery (SR review #1).
func (s *Sink) flush(ctx context.Context, job flushJob) {
	defer s.flushDone(job.footprint)
	// Bound the whole commit + isolation: a wedged S3 multipart / catalog
	// commit otherwise pins this writer connection's mutex indefinitely and
	// every bundle round-robined onto it queues behind the wedge (H15). A
	// timed-out bundle's messages stay un-acked and redeliver after AckWait.
	fctx, cancel := context.WithTimeout(ctx, s.cfg.FlushTimeout)
	defer cancel()
	if err := s.writer.WriteBundle(fctx, job.events); err != nil {
		commitFailuresTotal.Inc()
		s.recordCommitFailure()
		s.log.Warn().Err(err).Int("events", len(job.events)).
			Msg("bundle commit failed; isolating per-event")
		s.isolate(fctx, job)
		return
	}
	commitsTotal.Inc()
	s.commitFailFirst.Store(0)
	s.ackAll(job.msgs)
	s.log.Info().Int("events", len(job.events)).Msg("bundle committed")
}

// recordCommitFailure tracks the zero-progress failure streak; when it spans
// CommitFailureWindow, the sink reports itself durably broken (see Run). Any
// commit — whole-bundle or an isolate range — clears the streak: a poison-
// heavy but healthy period must never trip a restart (H15).
func (s *Sink) recordCommitFailure() {
	now := time.Now().UnixNano()
	first := s.commitFailFirst.Load()
	if first == 0 {
		s.commitFailFirst.CompareAndSwap(0, now)
		return
	}
	if time.Duration(now-first) >= s.cfg.CommitFailureWindow {
		select {
		case s.fatal <- fmt.Errorf("sink committed nothing for %s: writer/catalog durably broken (restart re-pins connections)", s.cfg.CommitFailureWindow):
		default:
		}
	}
}

// flushDone releases a finished bundle's resident footprint from the inflight
// tally and wakes applyBackpressure (non-blocking — a missed wake is re-checked
// on the next completion, and there is always a pending completion while
// inflight is over the ceiling).
func (s *Sink) flushDone(footprint int) {
	s.inflight.Add(-int64(footprint))
	select {
	case s.flushed <- struct{}{}:
	default:
	}
}

func (s *Sink) ackAll(msgs []jetstream.Msg) {
	for _, msg := range msgs {
		if err := msg.Ack(); err != nil {
			s.log.Warn().Err(err).Msg("ack failed after successful commit; duplicate rows possible")
		}
	}
}

// isolate re-drives a bundle that failed to commit whole. It bisects: commit a
// range; on failure split and recurse; at a single row decide poison-vs-transient.
// This (a) preserves the bundle's healthy events — only a row that fails ALONE
// with a deterministic rejection is dropped — and (b) localizes poison in
// O(log n) commits instead of minting up to one DuckLake snapshot per row.
//
// The poison decision keys on lake.ErrPoisonRow, NOT on "did a sibling commit":
// a transient mid-bundle outage (an S3/catalog blip after some rows already
// committed) must never terminate healthy, never-persisted events. Only a
// deterministic per-row rejection — or a row that has redelivered past
// poisonRedeliveryThreshold — is Term'd.
func (s *Sink) isolate(ctx context.Context, job flushJob) {
	st := &isolateState{}
	s.isolateRange(ctx, job, 0, len(job.events), st)
	if st.committed == 0 && st.attempts > 0 && ctx.Err() == nil {
		s.log.Error().Int("attempts", st.attempts).
			Msg("isolate: nothing committed; treating as transient, leaving bundle for redelivery")
	}
}

// isolateState threads commit/attempt counts through the bisection so the
// global-fault short-circuit can stop grinding: a healthy writer commits SOME
// range within the first few attempts, so zero commits after isolateProbeLimit
// write attempts ⇒ the writer/catalog is down, not a poison row.
type isolateState struct {
	committed int // rows durably committed this isolate pass
	attempts  int // WriteBundle calls this pass (bounds the global-fault grind)
}

// isolateRange commits job.events[lo:hi], bisecting on failure. A whole-range
// commit acks every message in it. A size-1 failure is terminated only when it
// is deterministic poison (lake.ErrPoisonRow) or has already cycled past
// poisonRedeliveryThreshold; any other (transient) failure is left un-acked so
// JetStream redelivers it.
func (s *Sink) isolateRange(ctx context.Context, job flushJob, lo, hi int, st *isolateState) {
	if lo >= hi || ctx.Err() != nil {
		return
	}
	// Global-fault short-circuit: isolateProbeLimit write attempts with nothing
	// committed ⇒ the writer/catalog is down, not a poison row (a healthy writer
	// would have committed some range by now). Stop; untouched rows stay un-acked
	// and redeliver wholesale after AckWait. Once any range commits the bisection
	// runs to completion to localize a real poison row.
	if st.committed == 0 && st.attempts >= isolateProbeLimit {
		return
	}
	st.attempts++
	err := s.writer.WriteBundle(ctx, job.events[lo:hi])
	if err == nil {
		st.committed += hi - lo
		s.commitFailFirst.Store(0) // partial progress: the writer is alive (H15)
		for i := lo; i < hi; i++ {
			if ackErr := job.msgs[i].Ack(); ackErr != nil {
				s.log.Warn().Err(ackErr).Msg("ack failed after isolated commit; duplicate rows possible")
			}
		}
		return
	}
	if hi-lo == 1 {
		if errors.Is(err, lake.ErrPoisonRow) || redeliveryCount(job.msgs[lo]) >= poisonRedeliveryThreshold {
			s.terminatePoison(job.events[lo], job.msgs[lo])
		}
		// else: transient single-row failure → not acked, not terminated →
		// JetStream redelivers (the row is healthy; the writer was unavailable).
		return
	}
	mid := lo + (hi-lo)/2
	s.isolateRange(ctx, job, lo, mid, st)
	s.isolateRange(ctx, job, mid, hi, st)
}

func (s *Sink) terminatePoison(event cloudevent.StoredEvent, msg jetstream.Msg) {
	poisonRows.Inc()
	s.log.Error().Str("subject", event.Subject).Str("id", event.ID).Str("type", event.Type).
		Msg("terminating poison row: writer cannot persist it")
	if err := msg.Term(); err != nil {
		s.log.Error().Err(err).Msg("terminating poison message failed")
	}
}

// redeliveryCount returns how many times JetStream has delivered msg (1 on the
// first delivery), or 0 when metadata is unavailable.
func redeliveryCount(msg jetstream.Msg) uint64 {
	meta, err := msg.Metadata()
	if err != nil {
		return 0
	}
	return meta.NumDelivered
}
