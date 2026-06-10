// Package compact merges small parquet bundles within a hive partition into
// larger ones. There is no index database: readers list partitions directly,
// so compaction is write-new, grace-delete-old, with a crash manifest under
// raw/_compaction/ (outside the type=*/date=* read glob).
//
// Coordination contract with the dq materializer:
//   - only files at or below the materializer's published watermark for a
//     partition are compacted, so its StartAfter listing cursor never
//     re-observes a compacted region;
//   - compacted output names start with "c1-", which sorts before "ingest-",
//     keeping outputs below any watermark cursor.
package compact

import "context"

// ObjectInfo describes one stored object.
type ObjectInfo struct {
	Key  string
	Size int64
}

// ObjectStore is the storage surface the compactor needs. Implemented by
// s3client; tests use an in-memory fake.
type ObjectStore interface {
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	GetObject(ctx context.Context, key string) ([]byte, error)
	PutObject(ctx context.Context, key string, body []byte) error
	DeleteObject(ctx context.Context, key string) error
}
