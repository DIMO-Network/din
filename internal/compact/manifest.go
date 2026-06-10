package compact

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"
)

// manifestPrefix lives outside the type=*/date=* glob so DuckDB readers and
// the materializer never see manifests as data.
const manifestPrefix = "_compaction/"

// Manifest records one compaction unit for crash recovery. Written after
// outputs are uploaded, deleted after sources are removed. Recovery rules:
// all outputs exist -> finish deleting sources + manifest; otherwise ->
// delete partial outputs + manifest, sources untouched.
type Manifest struct {
	Sources   []string  `json:"sources"`
	Outputs   []string  `json:"outputs"`
	CreatedAt time.Time `json:"created_at"`
}

// manifestKey builds the manifest object key under the partition root.
func manifestKey(root, id string) string {
	return path.Join(root, manifestPrefix, id+".json")
}

func writeManifest(ctx context.Context, store ObjectStore, root, id string, m Manifest) error {
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}
	if err := store.PutObject(ctx, manifestKey(root, id), body); err != nil {
		return fmt.Errorf("writing manifest %s: %w", id, err)
	}
	return nil
}

// listManifests returns manifest IDs currently present under root.
func listManifests(ctx context.Context, store ObjectStore, root string) (map[string]Manifest, error) {
	prefix := path.Join(root, manifestPrefix) + "/"
	objects, err := store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing manifests: %w", err)
	}

	manifests := make(map[string]Manifest, len(objects))
	for _, obj := range objects {
		body, err := store.GetObject(ctx, obj.Key)
		if err != nil {
			return nil, fmt.Errorf("reading manifest %s: %w", obj.Key, err)
		}
		var m Manifest
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, fmt.Errorf("decoding manifest %s: %w", obj.Key, err)
		}
		id := strings.TrimSuffix(path.Base(obj.Key), ".json")
		manifests[id] = m
	}
	return manifests, nil
}
