package stream

import (
	"context"
	"fmt"
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

// EnsureStreams creates or updates the WAL stream for every partition.
func EnsureStreams(ctx context.Context, js jetstream.JetStream, cfg Config) ([]jetstream.Stream, error) {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
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
		}
		s, err := js.CreateOrUpdateStream(ctx, streamCfg)
		if err != nil {
			return nil, fmt.Errorf("creating %s stream: %w", name, err)
		}
		streams[i] = s
	}
	return streams, nil
}
