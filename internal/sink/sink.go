// Package sink consumes the INGEST_RAW stream and persists raw cloudevents
// as hive-partitioned parquet bundles. Messages are acknowledged only after
// the bundle containing them is durably stored, giving at-least-once
// delivery end to end; duplicates die in compaction and in dq's dedup.
package sink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/pqwrite"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
)

// ObjectPutter stores one object. Implemented by s3client.
type ObjectPutter interface {
	PutObject(ctx context.Context, key string, body []byte) error
}

// Config tunes batching. Zero values take the plan defaults.
type Config struct {
	// RootPrefix is the partition root, normally pqwrite.RawPrefix.
	RootPrefix string
	// MaxRowsPerFlush flushes a partition buffer at this row count.
	MaxRowsPerFlush int
	// MaxBytesPerFlush flushes a partition buffer at this accumulated
	// payload size (pre-compression estimate).
	MaxBytesPerFlush int
	// MaxAge flushes a partition buffer this long after its first event.
	MaxAge time.Duration
	// GlobalMaxBytes caps memory across all partition buffers; exceeding it
	// flushes the largest buffer immediately.
	GlobalMaxBytes int
	// Workers is the number of concurrent flush workers.
	Workers int
	// FetchBatch is the max messages per JetStream fetch.
	FetchBatch int
}

func (c *Config) applyDefaults() {
	if c.RootPrefix == "" {
		c.RootPrefix = pqwrite.RawPrefix
	}
	if c.MaxRowsPerFlush == 0 {
		c.MaxRowsPerFlush = 100_000
	}
	if c.MaxBytesPerFlush == 0 {
		c.MaxBytesPerFlush = 128 << 20
	}
	if c.MaxAge == 0 {
		c.MaxAge = time.Minute
	}
	if c.GlobalMaxBytes == 0 {
		c.GlobalMaxBytes = 512 << 20
	}
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.FetchBatch == 0 {
		c.FetchBatch = 1000
	}
}

// Sink runs the fetch → batch → flush pipeline.
type Sink struct {
	cfg      Config
	consumer jetstream.Consumer
	store    ObjectPutter
	log      zerolog.Logger

	// buffers is owned by the Run goroutine; no locking needed.
	buffers map[pqwrite.PartitionKey]*partitionBuffer
	total   int

	jobs chan flushJob
	wg   sync.WaitGroup
}

type partitionBuffer struct {
	events  []cloudevent.StoredEvent
	msgs    []jetstream.Msg
	bytes   int
	firstAt time.Time
}

type flushJob struct {
	partition pqwrite.PartitionKey
	events    []cloudevent.StoredEvent
	msgs      []jetstream.Msg
}

// New constructs a Sink reading from consumer and writing via store.
func New(cfg Config, consumer jetstream.Consumer, store ObjectPutter, log zerolog.Logger) *Sink {
	cfg.applyDefaults()
	return &Sink{
		cfg:      cfg,
		consumer: consumer,
		store:    store,
		log:      log.With().Str("component", "sink").Logger(),
		buffers:  make(map[pqwrite.PartitionKey]*partitionBuffer),
		jobs:     make(chan flushJob, cfg.Workers*2),
	}
}

// Run processes messages until ctx is canceled, then flushes everything
// still buffered and drains the workers.
func (s *Sink) Run(ctx context.Context) error {
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
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

	// Shutdown: flush every remaining buffer, then wait for workers so all
	// acks land before the process exits.
	s.flushReady(true)
	close(s.jobs)
	s.wg.Wait()

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

// add decodes one message into its partition buffer. Undecodable messages
// are terminated: redelivery cannot fix them and they must not block acks.
func (s *Sink) add(msg jetstream.Msg) {
	event, err := stream.ParseMsg(msg.Headers(), msg.Data())
	if err != nil {
		s.log.Error().Err(err).Str("subject", msg.Subject()).Msg("terminating undecodable message")
		if termErr := msg.Term(); termErr != nil {
			s.log.Error().Err(termErr).Msg("terminating message failed")
		}
		return
	}

	key := pqwrite.PartitionFor(&event.CloudEventHeader)
	buf := s.buffers[key]
	if buf == nil {
		buf = &partitionBuffer{firstAt: time.Now()}
		s.buffers[key] = buf
	}
	buf.events = append(buf.events, event)
	buf.msgs = append(buf.msgs, msg)
	buf.bytes += len(msg.Data())
	s.total += len(msg.Data())
}

// flushReady enqueues every buffer that hit a trigger (or all of them when
// force is set). If the global cap is exceeded the largest buffer flushes.
func (s *Sink) flushReady(force bool) {
	var largest pqwrite.PartitionKey
	largestBytes := -1

	for key, buf := range s.buffers {
		if force ||
			len(buf.events) >= s.cfg.MaxRowsPerFlush ||
			buf.bytes >= s.cfg.MaxBytesPerFlush ||
			time.Since(buf.firstAt) >= s.cfg.MaxAge {
			s.enqueue(key, buf)
			continue
		}
		if buf.bytes > largestBytes {
			largest, largestBytes = key, buf.bytes
		}
	}

	if s.total > s.cfg.GlobalMaxBytes && largestBytes > 0 {
		if buf, ok := s.buffers[largest]; ok {
			s.enqueue(largest, buf)
		}
	}
}

func (s *Sink) enqueue(key pqwrite.PartitionKey, buf *partitionBuffer) {
	delete(s.buffers, key)
	s.total -= buf.bytes
	s.jobs <- flushJob{partition: key, events: buf.events, msgs: buf.msgs}
}

func (s *Sink) worker(ctx context.Context) {
	defer s.wg.Done()
	for job := range s.jobs {
		s.flush(ctx, job)
	}
}

// flush encodes and stores one bundle, then acks its messages. On failure
// nothing is acked: JetStream redelivers and the rows land in a later
// bundle (at-least-once).
func (s *Sink) flush(ctx context.Context, job flushJob) {
	objectKey := pqwrite.NewIngestObjectKey(s.cfg.RootPrefix, job.partition, time.Now())

	body, err := pqwrite.Encode(job.events, objectKey)
	if err != nil {
		s.log.Error().Err(err).Str("key", objectKey).Int("events", len(job.events)).
			Msg("encoding bundle failed; messages left for redelivery")
		return
	}
	if err := s.store.PutObject(ctx, objectKey, body); err != nil {
		s.log.Error().Err(err).Str("key", objectKey).
			Msg("storing bundle failed; messages left for redelivery")
		return
	}

	for _, msg := range job.msgs {
		if err := msg.Ack(); err != nil {
			s.log.Warn().Err(err).Msg("ack failed after successful store; duplicate rows possible")
		}
	}
	s.log.Info().Str("key", objectKey).Int("events", len(job.events)).Msg("bundle stored")
}
