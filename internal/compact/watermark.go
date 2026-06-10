package compact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// WatermarkKey is the dq materializer's published cursor: a JSON object
// mapping "type=T/date=D" to the last raw object key it has decoded.
const WatermarkKey = "decoded/v1/_state/watermark.json"

// ErrNoWatermark reports that the materializer has not published a cursor
// yet; the compactor must not touch anything in that case.
var ErrNoWatermark = errors.New("materializer watermark not found")

// Watermark answers "may this file be compacted?" per partition.
type Watermark map[string]string

// LoadWatermark fetches the materializer cursor. Missing object returns
// ErrNoWatermark wrapped so callers can skip the cycle cleanly.
func LoadWatermark(ctx context.Context, store ObjectStore) (Watermark, error) {
	body, err := store.GetObject(ctx, WatermarkKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoWatermark, err)
	}
	var w Watermark
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, fmt.Errorf("decoding watermark: %w", err)
	}
	return w, nil
}

// Covers reports whether key (a full object key) is at or below the cursor
// for its partition, i.e. already decoded by the materializer and therefore
// safe to compact away. partition is "type=T/date=D".
func (w Watermark) Covers(partition, key string) bool {
	cursor, ok := w[partition]
	if !ok {
		return false
	}
	return key <= cursor
}
