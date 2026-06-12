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

// Config tunes batching. Zero values take the defaults.
type Config struct {
	// MaxRowsPerFlush flushes the buffer at this row count.
	MaxRowsPerFlush int
	// MaxBytesPerFlush flushes the buffer at this accumulated payload
	// size. DuckLake splits each bundle across partitions itself, so a
	// single buffer needs no per-partition cap.
	MaxBytesPerFlush int
	// MaxAge flushes the buffer this long after its first event.
	MaxAge time.Duration
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
	if c.MaxAge == 0 {
		c.MaxAge = time.Minute
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

	var fetchErr error
loop:
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				break loop
			}
			s.add(msg)
			s.flushReady(false)
		case <-ticker.C:
			s.flushReady(false)
		}
	}
	fetchErr = <-fetchDone

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
	select {
	case <-workersDone:
	case <-time.After(s.cfg.DrainTimeout):
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
	if meta, err := msg.Metadata(); err == nil && meta.NumDelivered > 1 {
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
	if force ||
		len(buf.events) >= s.cfg.MaxRowsPerFlush ||
		buf.bytes >= s.cfg.MaxBytesPerFlush ||
		time.Since(buf.firstAt) >= s.cfg.MaxAge {
		s.enqueue(buf, force)
	}
}

// enqueue hands the buffer to the flush workers. Non-blocking by default;
// the shutdown drain (block=true) waits up to the drain deadline so a
// wedged writer can't hang exit — whatever doesn't flush redelivers.
func (s *Sink) enqueue(buf *eventBuffer, block bool) bool {
	job := flushJob{events: buf.events, msgs: buf.msgs}
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
	s.buffer = nil
	return true
}

func (s *Sink) worker(ctx context.Context) {
	defer s.wg.Done()
	for job := range s.jobs {
		s.flush(ctx, job)
	}
}

// flush commits one bundle, then acks its messages. On failure nothing is
// acked: JetStream redelivers and the rows land in a later bundle
// (at-least-once).
func (s *Sink) flush(ctx context.Context, job flushJob) {
	if err := s.writer.WriteBundle(ctx, job.events); err != nil {
		s.log.Error().Err(err).Int("events", len(job.events)).
			Msg("committing bundle failed; messages left for redelivery")
		return
	}

	for _, msg := range job.msgs {
		if err := msg.Ack(); err != nil {
			s.log.Warn().Err(err).Msg("ack failed after successful commit; duplicate rows possible")
		}
	}
	s.log.Info().Int("events", len(job.events)).Msg("bundle committed")
}
