package compact_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/din/internal/compact"
	"github.com/DIMO-Network/din/internal/pqwrite"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) List(_ context.Context, prefix string) ([]compact.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []compact.ObjectInfo
	for k, v := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, compact.ObjectInfo{Key: k, Size: int64(len(v))})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *memStore) GetObject(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	body, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return body, nil
}

func (m *memStore) PutObject(_ context.Context, key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), body...)
	return nil
}

func (m *memStore) DeleteObject(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memStore) keys(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func storedEvent(id string, ts time.Time) cloudevent.StoredEvent {
	return cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        "dimo.status",
			Subject:     "did:erc721:137:0xA:1",
			Source:      "0xConn",
			ID:          id,
			Time:        ts,
		},
		Data: json.RawMessage(`{"v":1}`),
	}}
}

// writeBundle stores a small ingest bundle and returns its key.
func writeBundle(t *testing.T, store *memStore, partition string, seq int, events ...cloudevent.StoredEvent) string {
	t.Helper()
	key := fmt.Sprintf("raw/%s/ingest-%013d-TEST%04d.parquet", partition, 1749470000000+seq, seq)
	body, err := pqwrite.Encode(events, key)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(context.Background(), key, body))
	return key
}

func setWatermark(t *testing.T, store *memStore, cursor map[string]string) {
	t.Helper()
	body, err := json.Marshal(cursor)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(context.Background(), compact.WatermarkKey, body))
}

// newCompactor returns a compactor with instant grace for tests.
func newCompactor(store *memStore) *compact.Compactor {
	return compact.New(compact.Config{
		MinFiles:    3,
		DeleteGrace: time.Millisecond,
	}, store, zerolog.Nop())
}

func TestCycle_MergesCoveredFilesAndDedups(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	partition := "type=dimo.status/date=2026-06-09"
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)

	k1 := writeBundle(t, store, partition, 1, storedEvent("e1", ts))
	k2 := writeBundle(t, store, partition, 2, storedEvent("e2", ts.Add(time.Minute)))
	k3 := writeBundle(t, store, partition, 3, storedEvent("e2", ts.Add(time.Minute)), storedEvent("e3", ts.Add(2*time.Minute))) // e2 duplicated
	setWatermark(t, store, map[string]string{partition: k3})

	require.NoError(t, newCompactor(store).Cycle(context.Background()))

	keys := store.keys("raw/" + partition + "/")
	require.Len(t, keys, 1, "three sources merged to one output")
	assert.Contains(t, keys[0], "/c1-")
	assert.NotContains(t, store.keys("raw/"), k1)
	_ = k2

	body, err := store.GetObject(context.Background(), keys[0])
	require.NoError(t, err)
	events, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.Len(t, events, 3, "duplicate e2 removed")

	assert.Empty(t, store.keys("raw/_compaction/"), "manifest cleaned up")
}

func TestCycle_RespectsWatermark(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	partition := "type=dimo.status/date=2026-06-09"
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)

	k1 := writeBundle(t, store, partition, 1, storedEvent("e1", ts))
	writeBundle(t, store, partition, 2, storedEvent("e2", ts))
	writeBundle(t, store, partition, 3, storedEvent("e3", ts))
	// Watermark covers only the first file: nothing may compact (need >=2 covered).
	setWatermark(t, store, map[string]string{partition: k1})

	require.NoError(t, newCompactor(store).Cycle(context.Background()))

	keys := store.keys("raw/" + partition + "/")
	assert.Len(t, keys, 3, "files above watermark untouched")
	for _, k := range keys {
		assert.Contains(t, k, "/ingest-")
	}
}

func TestCycle_NoWatermarkSkips(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	partition := "type=dimo.status/date=2026-06-09"
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	for i := range 4 {
		writeBundle(t, store, partition, i, storedEvent(fmt.Sprintf("e%d", i), ts))
	}

	require.NoError(t, newCompactor(store).Cycle(context.Background()))
	assert.Len(t, store.keys("raw/"+partition+"/"), 4, "no watermark -> no compaction")
}

func TestRecover_FinishesCompleteUnit(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	ctx := context.Background()
	partition := "type=dimo.status/date=2026-06-09"
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)

	src := writeBundle(t, store, partition, 1, storedEvent("e1", ts))
	outKey := "raw/" + partition + "/c1-0000000000001-OUT.parquet"
	outBody, err := pqwrite.Encode([]cloudevent.StoredEvent{storedEvent("e1", ts)}, outKey)
	require.NoError(t, err)
	require.NoError(t, store.PutObject(ctx, outKey, outBody))

	manifest, err := json.Marshal(compact.Manifest{Sources: []string{src}, Outputs: []string{outKey}, CreatedAt: ts})
	require.NoError(t, err)
	require.NoError(t, store.PutObject(ctx, "raw/_compaction/CRASHED1.json", manifest))

	require.NoError(t, newCompactor(store).Recover(ctx))

	keys := store.keys("raw/" + partition + "/")
	assert.Equal(t, []string{outKey}, keys, "source deleted, output kept")
	assert.Empty(t, store.keys("raw/_compaction/"))
}

func TestRecover_RollsBackPartialUnit(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	ctx := context.Background()
	partition := "type=dimo.status/date=2026-06-09"
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)

	src := writeBundle(t, store, partition, 1, storedEvent("e1", ts))
	// Manifest references an output that was never uploaded plus one that was.
	partialOut := "raw/" + partition + "/c1-0000000000001-PART.parquet"
	require.NoError(t, store.PutObject(ctx, partialOut, []byte("partial")))
	missingOut := "raw/" + partition + "/c1-0000000000002-MISS.parquet"

	manifest, err := json.Marshal(compact.Manifest{Sources: []string{src}, Outputs: []string{partialOut, missingOut}, CreatedAt: ts})
	require.NoError(t, err)
	require.NoError(t, store.PutObject(ctx, "raw/_compaction/CRASHED2.json", manifest))

	require.NoError(t, newCompactor(store).Recover(ctx))

	keys := store.keys("raw/" + partition + "/")
	assert.Equal(t, []string{src}, keys, "partial output removed, source preserved")
	assert.Empty(t, store.keys("raw/_compaction/"))
}

func TestCompactedOutputsSortBelowIngestFiles(t *testing.T) {
	t.Parallel()
	// Contract: c1- names sort before ingest- names so the materializer's
	// lexicographic cursor never re-observes compacted output as new.
	assert.Less(t, "c1-0000000000001-X.parquet", "ingest-0000000000001-X.parquet")
}
