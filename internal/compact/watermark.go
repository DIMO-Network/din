package compact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// DefaultDecodedPrefix mirrors the dq materializer's default decoded layout
// root. Both services must agree on this prefix or the compactor reads a
// stale or missing cursor; override via Config.DecodedPrefix (DECODED_PREFIX)
// in lockstep with dq.
const DefaultDecodedPrefix = "decoded/v1/"

// WatermarkKey is the dq materializer's published cursor under the default
// prefix: a JSON object mapping "type=T/date=D" to the last raw object key
// it has decoded.
const WatermarkKey = DefaultDecodedPrefix + watermarkSuffix

const watermarkSuffix = "_state/watermark.json"

// ErrNoWatermark reports that the materializer has not published a cursor
// yet; the compactor must not touch anything in that case.
var ErrNoWatermark = errors.New("materializer watermark not found")

// Watermark answers "may this file be compacted?" per partition.
type Watermark map[string]string

// LoadWatermark merges every materializer cursor under
// <decodedPrefix>_state/ (watermark.json for a single replica,
// watermark-pNNNofMMM.json per shard when the materializer is sharded —
// shards own disjoint partitions, so the maps never conflict; if they ever
// do, the LOWER cursor wins, which only delays compaction). No cursor file
// at all returns ErrNoWatermark so callers skip the cycle cleanly.
func LoadWatermark(ctx context.Context, store ObjectStore, decodedPrefix string) (Watermark, error) {
	statePrefix := decodedPrefix + "_state/watermark"
	objects, err := store.List(ctx, statePrefix)
	if err != nil {
		return nil, fmt.Errorf("listing watermarks: %w", err)
	}
	merged := Watermark{}
	found := false
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		body, err := store.GetObject(ctx, obj.Key)
		if err != nil {
			return nil, fmt.Errorf("reading watermark %s: %w", obj.Key, err)
		}
		var w Watermark
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, fmt.Errorf("decoding watermark %s: %w", obj.Key, err)
		}
		found = true
		for partition, cursor := range w {
			if existing, ok := merged[partition]; !ok || cursor < existing {
				merged[partition] = cursor
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("%w: no cursor files under %s", ErrNoWatermark, statePrefix)
	}
	return merged, nil
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
