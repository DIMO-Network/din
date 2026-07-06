package stream

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Config controls the INGEST_RAW stream provisioning.
type Config struct {
	// Replicas is the JetStream replication factor: 3 in a cluster, 1 for
	// embedded/single-node deployments.
	Replicas int
	// MaxAge bounds how long messages are retained. The parquet sink
	// normally acks within seconds; MaxAge is lag headroom, not archive.
	MaxAge time.Duration
	// MaxBytes is a hard backstop on stream size. Zero means unlimited.
	MaxBytes int64
	// DuplicateWindow is the Nats-Msg-Id dedup horizon.
	DuplicateWindow time.Duration
	// Partitions splits the WAL into N streams by subject hash; each
	// partition gets its own sink consumer, so flush throughput scales
	// with N while per-vehicle ordering is preserved. 1 = the historical
	// single INGEST_RAW stream.
	Partitions int
}

// DefaultConfig returns production defaults from the design plan.
func DefaultConfig() Config {
	return Config{
		Replicas:        1,
		MaxAge:          48 * time.Hour,
		DuplicateWindow: 2 * time.Minute,
		Partitions:      1,
	}
}

// EnsureStreams creates or updates the WAL stream for every partition. It
// first refuses to run against leftover streams from a DIFFERENT partition
// layout: a stale broad-subject INGEST_RAW overlaps every partitioned filter
// (CreateOrUpdateStream would fail forever = CrashLoop), and a stale
// INGEST_RAW_PNNN beyond the current count would silently strand its unacked
// backlog until MaxAge discards it (H12). Rescaling is a drain-and-delete
// operation — see docs/wal-partition-rescale.md.
func EnsureStreams(ctx context.Context, js jetstream.JetStream, cfg Config) ([]jetstream.Stream, error) {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if err := checkStaleStreams(ctx, js, cfg.Partitions); err != nil {
		return nil, err
	}
	streams := make([]jetstream.Stream, cfg.Partitions)
	for i := range cfg.Partitions {
		name := StreamNameFor(i, cfg.Partitions)
		streamCfg := jetstream.StreamConfig{
			Name:        name,
			Description: "Raw validated cloudevents awaiting persistence and fan-out",
			Subjects:    partitionSubjects(i, cfg.Partitions),
			Storage:     jetstream.FileStorage,
			Retention:   jetstream.LimitsPolicy,
			Replicas:    cfg.Replicas,
			MaxAge:      cfg.MaxAge,
			MaxBytes:    cfg.MaxBytes,
			Duplicates:  cfg.DuplicateWindow,
			// This stream is a durability WAL: when the MaxBytes backstop is
			// hit, REJECT new publishes (devices get 503 + Retry-After and
			// retry) instead of the JetStream default DiscardOld, which would
			// silently drop the OLDEST un-persisted events — exactly the data
			// the WAL exists to protect — while every publish kept succeeding
			// (H12).
			Discard: jetstream.DiscardNew,
		}
		s, err := js.CreateOrUpdateStream(ctx, streamCfg)
		if err != nil {
			return nil, fmt.Errorf("creating %s stream: %w", name, err)
		}
		streams[i] = s
	}
	return streams, nil
}

// checkStaleStreams fails with an actionable error when a WAL stream from a
// previous partition layout still exists (see EnsureStreams). Only
// INGEST_RAW-prefixed streams are considered; other streams (DIMO_SIGNALS,
// DIMO_EVENTS) are none of our business.
func checkStaleStreams(ctx context.Context, js jetstream.JetStream, partitions int) error {
	expected := make(map[string]struct{}, partitions)
	for i := range partitions {
		expected[StreamNameFor(i, partitions)] = struct{}{}
	}
	names := js.StreamNames(ctx)
	for name := range names.Name() {
		if !strings.HasPrefix(name, StreamName) {
			continue
		}
		if _, ok := expected[name]; !ok {
			return fmt.Errorf(
				"WAL stream %q is from a different partition layout than NATS_STREAM_PARTITIONS=%d: "+
					"its subjects overlap or its backlog would be stranded; drain and delete it first "+
					"(docs/wal-partition-rescale.md)", name, partitions)
		}
	}
	return names.Err()
}
