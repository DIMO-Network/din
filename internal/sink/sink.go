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
	// MaxBufferedBytes is the hard ceiling on un-flushed buffered payload.
	// When the writer stalls and flush slots fill, the Run loop blocks on
	// this ceiling so JetStream backpressure propagates instead of the
	// buffer growing until OOM (SR-8). Defaults to 4x MaxBytesPerFlush.
	MaxBufferedBytes int
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
		// a stalled writer can't grow the buffer without bound.
		c.MaxBufferedBytes = 4 * c.MaxBytesPerFlush
	}
	if c.MaxAge == 0 {
		c.MaxAge = time.Minute
	}
	if c.MinFlushBytes == 0 {
		c.MinFlushBytes = 16 << 20 // 16 MiB — well above "tiny", well below the 128 MiB target
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

	// drainDeadline bounds the whole shutdown flush; set once by Run.
	drainDeadline time.Time
}

type eventBuffer struct {
	events  []cloudevent.StoredEvent
	msgs    []jetstream.Msg
	bytes   int
	firstAt time.Time
}

type flushJob struct {
	events []cloudevent.StoredEvent
	msgs   []jetstream.Msg
	bytes  int
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
			done <- err
			return
		}
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
	}

	if s.buffer == nil {
		s.buffer = &eventBuffer{firstAt: time.Now()}
	}
	s.buffer.events = append(s.buffer.events, event)
	s.buffer.msgs = append(s.buffer.msgs, msg)
	s.buffer.bytes += len(msg.Data())
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
	job := flushJob{events: buf.events, msgs: buf.msgs, bytes: buf.bytes}
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
	s.inflight.Add(int64(job.bytes))
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
			bufBytes = s.buffer.bytes
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
	defer s.flushDone(job.bytes)
	if err := s.writer.WriteBundle(ctx, job.events); err != nil {
		s.log.Warn().Err(err).Int("events", len(job.events)).
			Msg("bundle commit failed; isolating per-event")
		s.isolate(ctx, job)
		return
	}
	s.ackAll(job.msgs)
	s.log.Info().Int("events", len(job.events)).Msg("bundle committed")
}

// flushDone releases a finished bundle's bytes from the inflight tally and wakes
// applyBackpressure (non-blocking — a missed wake is re-checked on the next
// completion, and there is always a pending completion while inflight is over
// the ceiling).
func (s *Sink) flushDone(bytes int) {
	s.inflight.Add(-int64(bytes))
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

// isolate retries a failed bundle one event at a time so a single poison row
// can't take the bundle's good events or the partition's progress down with it.
// An event that commits alone is acked. An event that fails while another in the
// same bundle succeeds is poison (the writer is healthy, the row is bad) and is
// terminated. If NOTHING commits, the fault is treated as transient and the
// failures are left for redelivery — except messages that have already cycled
// poisonRedeliveryThreshold times, which are terminated to bound an all-poison
// or single-row-poison bundle that would otherwise redeliver forever.
func (s *Sink) isolate(ctx context.Context, job flushJob) {
	committed := 0
	var failed []int
	for i := range job.events {
		if err := s.writer.WriteBundle(ctx, job.events[i:i+1]); err != nil {
			failed = append(failed, i)
			// Global-fault bail-out: nothing has committed after probing the first
			// isolateProbeLimit rows → treat as a transient outage, not poison.
			// Leave the rest un-acked for redelivery instead of grinding the bundle.
			if committed == 0 && len(failed) >= isolateProbeLimit {
				s.log.Error().Int("probed", len(failed)).Int("remaining", len(job.events)-i-1).
					Msg("isolate: probe failed with zero commits; treating as global fault, leaving bundle for redelivery")
				s.termExpiredPoison(ctx, job, failed)
				return
			}
			continue
		}
		if err := job.msgs[i].Ack(); err != nil {
			s.log.Warn().Err(err).Msg("ack failed after isolated commit; duplicate rows possible")
		}
		committed++
	}
	// A canceled context (drain timeout / shutdown) means the remaining WriteBundle
	// failures are from cancellation, not the writer rejecting poison — so the
	// "committed > 0 ⇒ this row is poison" heuristic is invalid (the writer was never
	// really consulted). Leave every failed row un-acked for redelivery rather than
	// Term'ing healthy, never-persisted data.
	if ctx.Err() != nil {
		if len(failed) > 0 {
			s.log.Warn().Int("events", len(failed)).
				Msg("isolate: context canceled mid-bundle; leaving failed rows for redelivery")
		}
		return
	}
	for _, i := range failed {
		if committed > 0 || redeliveryCount(job.msgs[i]) >= poisonRedeliveryThreshold {
			s.terminatePoison(job.events[i], job.msgs[i])
		}
		// else: not acked, not terminated → JetStream redelivers (likely transient).
	}
	if committed == 0 && len(failed) > 0 {
		s.log.Error().Int("events", len(failed)).
			Msg("no event in bundle committed; treating as transient, leaving for redelivery")
	}
}

// termExpiredPoison terminates only the probed failures that have already cycled
// past the redelivery threshold, so a genuinely all-poison bundle still drains
// over successive redeliveries even when the global-fault bail-out skips full
// per-event isolation. Rows still under the threshold are left for redelivery.
// Under a canceled context (drain/shutdown) it Term's nothing: those write
// failures are cancellation, not the writer rejecting poison, so even a
// past-threshold row is left for redelivery — matching isolate's mid-bundle
// cancel guard (otherwise a pod drain that coincides with a redelivered row would
// silently drop healthy, never-persisted data).
func (s *Sink) termExpiredPoison(ctx context.Context, job flushJob, failed []int) {
	if ctx.Err() != nil {
		return
	}
	for _, i := range failed {
		if redeliveryCount(job.msgs[i]) >= poisonRedeliveryThreshold {
			s.terminatePoison(job.events[i], job.msgs[i])
		}
	}
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
