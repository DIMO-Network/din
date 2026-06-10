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
}

// DefaultConfig returns production defaults from the design plan.
func DefaultConfig() Config {
	return Config{
		Replicas:        1,
		MaxAge:          48 * time.Hour,
		DuplicateWindow: 2 * time.Minute,
	}
}

// EnsureStream creates or updates the INGEST_RAW stream.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg Config) (jetstream.Stream, error) {
	streamCfg := jetstream.StreamConfig{
		Name:        StreamName,
		Description: "Raw validated cloudevents awaiting persistence and fan-out",
		Subjects:    []string{SubjectWildcard},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		Replicas:    cfg.Replicas,
		MaxAge:      cfg.MaxAge,
		MaxBytes:    cfg.MaxBytes,
		Duplicates:  cfg.DuplicateWindow,
	}

	stream, err := js.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return nil, fmt.Errorf("creating %s stream: %w", StreamName, err)
	}
	return stream, nil
}
