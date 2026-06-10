// Package pqwrite builds hive-partitioned object keys and encodes raw
// cloudevents to sorted, compressed parquet for the ingest sink.
package pqwrite

import (
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/oklog/ulid/v2"
)

const (
	// RawPrefix roots all valid raw cloudevent bundles.
	RawPrefix = "raw"
	// PartialPrefix roots cloudevents that failed full validation but are
	// worth keeping for parse-on-read recovery.
	PartialPrefix = "raw_partial"

	// ingestFilePrefix marks files written by the sink; the compactor
	// writes c1- files. Both sort after any backfill/compacted names.
	ingestFilePrefix = "ingest"
)

// PartitionKey identifies one hive partition: type=<ceType>/date=<YYYY-MM-DD>.
type PartitionKey struct {
	Type string
	Date string // YYYY-MM-DD, from event.Time in UTC
}

// PartitionFor returns the partition for an event. The partition date comes
// from the event time (UTC), not ingest time, so late events land in their
// own day.
func PartitionFor(hdr *cloudevent.CloudEventHeader) PartitionKey {
	return PartitionKey{
		Type: sanitizePathValue(hdr.Type),
		Date: hdr.Time.UTC().Format("2006-01-02"),
	}
}

// Dir returns the S3 key prefix for this partition under root
// (RawPrefix or PartialPrefix): <root>/type=<type>/date=<date>/.
func (p PartitionKey) Dir(root string) string {
	return fmt.Sprintf("%s/type=%s/date=%s/", root, p.Type, p.Date)
}

// NewIngestObjectKey returns a unique, lexicographically time-ordered object
// key for a sink-written bundle: <dir>ingest-<unix_ms>-<ulid>.parquet.
// The materializer's listing cursor depends on this ordering.
func NewIngestObjectKey(root string, p PartitionKey, now time.Time) string {
	id := ulid.MustNew(ulid.Timestamp(now), rand.Reader)
	return fmt.Sprintf("%s%s-%013d-%s.parquet", p.Dir(root), ingestFilePrefix, now.UnixMilli(), id)
}

// sanitizePathValue keeps hive partition values S3- and glob-safe. Real CE
// types are dot-alphanumeric; anything else collapses to '-'.
func sanitizePathValue(s string) string {
	if s == "" {
		return "-"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '_', r == '-':
			return r
		default:
			return '-'
		}
	}, s)
}
