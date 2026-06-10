// Package objstore defines the object-storage surface din's wiring selects
// an implementation for: S3 (s3client) for scaled deployments, local
// filesystem (fsstore) for single-node ones. Backend choice is inferred from
// the bucket setting: a path-like value ("/data/pipeline" or
// "file:///data/pipeline") means filesystem, anything else S3. The rule
// matches dq's isLocalBucket so one set of env values configures both
// services.
package objstore

import (
	"context"
	"strings"
)

// ObjectInfo describes one stored object.
type ObjectInfo struct {
	Key  string
	Size int64
}

// Store is the full client surface shared by s3client.Client and
// fsstore.Client. Consumers keep their own narrower interfaces (sink, split,
// compact); Store exists so main can hold either implementation.
type Store interface {
	PutObject(ctx context.Context, key string, body []byte) error
	GetObject(ctx context.Context, key string, maxSize int64) ([]byte, error)
	ListObjectsV2(ctx context.Context, prefix string) ([]ObjectInfo, error)
	DeleteObjects(ctx context.Context, keys []string) error
}

// IsLocalPath reports whether bucket names a local filesystem root rather
// than an S3 bucket. Byte-for-byte dq's isLocalBucket rule.
func IsLocalPath(bucket string) bool {
	return strings.HasPrefix(bucket, "file://") || strings.HasPrefix(bucket, "/")
}

// LocalRoot converts a local bucket value to a filesystem root path.
func LocalRoot(bucket string) string {
	return strings.TrimPrefix(bucket, "file://")
}
