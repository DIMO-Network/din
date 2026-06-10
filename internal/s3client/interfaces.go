package s3client

import (
	"context"

	"github.com/DIMO-Network/din/internal/objstore"
	"github.com/DIMO-Network/din/internal/split"
)

// S3API defines the operations the ingest and compaction paths use.
// Interfaces are defined next to consumers; this mirrors the
// parquet-processor convention with a compile-time implementation check.
type S3API interface {
	PutObject(ctx context.Context, key string, body []byte) error
	GetObject(ctx context.Context, key string, maxSize int64) ([]byte, error)
	ListObjectsV2(ctx context.Context, prefix string) ([]ObjectInfo, error)
	DeleteObjects(ctx context.Context, keys []string) error
}

// Verify Client implements S3API, the shared store surface, and the
// splitter's blob store.
var (
	_ S3API             = (*Client)(nil)
	_ objstore.Store    = (*Client)(nil)
	_ split.ObjectStore = (*Client)(nil)
)
