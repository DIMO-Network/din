package pqwrite

import (
	"bytes"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
	pq "github.com/DIMO-Network/cloudevent/parquet"
)

// Encode renders events as one parquet bundle: rows sorted by (subject,
// time), zstd-compressed, with a subject bloom filter — the layout the dq
// DuckDB readers prune against. StoredEvents preserve DataIndexKey so blob
// references survive into the bundle.
func Encode(events []cloudevent.StoredEvent, objectKey string) ([]byte, error) {
	var buf bytes.Buffer
	_, err := pq.Encode(&buf, events, objectKey,
		pq.WithSortedRows(),
		pq.WithZstdCompression(),
		pq.WithSubjectBloomFilter(),
	)
	if err != nil {
		return nil, fmt.Errorf("encoding parquet bundle %s: %w", objectKey, err)
	}
	return buf.Bytes(), nil
}
