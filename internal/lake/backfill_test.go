package lake

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeLegacyBundle encodes events with the same cloudevent/parquet
// encoder DIS uses and writes the bundle under the DIS layout.
func writeLegacyBundle(t *testing.T, root, name string, events []cloudevent.StoredEvent) string {
	t.Helper()
	var buf bytes.Buffer
	_, err := ceparquet.Encode(&buf, events, name)
	require.NoError(t, err)
	path := filepath.Join(root, "cloudevent", "valid", "2026", "05", "01", name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o644))
	return path
}

func TestBackfill_RegistersLegacyDISBundles(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()
	legacyRoot := t.TempDir()

	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	inline := testEvent("legacy-1", "dimo.status", "did:erc721:137:0xA:1", ts)
	blob := testEvent("legacy-2", "dimo.attestation", "did:erc721:137:0xB:2", ts)
	blob.Data = nil
	blob.DataBase64 = "SGVsbG8="
	blob.DataIndexKey = "cloudevent/blobs/legacy"

	f1 := writeLegacyBundle(t, legacyRoot, "batch-aaaa.parquet",
		[]cloudevent.StoredEvent{inline})
	f2 := writeLegacyBundle(t, legacyRoot, "batch-bbbb.parquet",
		[]cloudevent.StoredEvent{blob})

	res, err := l.Backfill(ctx, []string{f1, f2, filepath.Join(legacyRoot, "not-parquet.json")}, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, BackfillResult{Registered: 2}, res)

	// Registered rows read with native semantics.
	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	require.Equal(t, 2, n)

	var gotData *string
	var gotBase64 []byte
	var gotIndexKey *string
	require.NoError(t, l.DB().QueryRowContext(ctx,
		`SELECT data, data_base64, data_index_key FROM lake.raw_events WHERE id = 'legacy-2'`).
		Scan(&gotData, &gotBase64, &gotIndexKey))
	assert.Nil(t, gotData)
	assert.Equal(t, []byte("SGVsbG8="), gotBase64)
	require.NotNil(t, gotIndexKey)
	assert.Equal(t, "cloudevent/blobs/legacy", *gotIndexKey)

	var gotSubject string
	require.NoError(t, l.DB().QueryRowContext(ctx,
		`SELECT subject FROM lake.raw_events WHERE type = 'dimo.status' AND "time"::DATE = DATE '2026-05-01'`).
		Scan(&gotSubject))
	assert.Equal(t, "did:erc721:137:0xA:1", gotSubject)

	// Rerun: idempotent, nothing double-registered.
	res, err = l.Backfill(ctx, []string{f1, f2}, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, BackfillResult{Skipped: 2}, res)
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, 2, n)
}

// Backfilled files and native writes coexist: maintenance merges across
// both without losing rows.
func TestBackfill_ThenMaintenance(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()
	legacyRoot := t.TempDir()

	ts := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	var legacy []cloudevent.StoredEvent
	for i := range 25 {
		legacy = append(legacy, testEvent(fmt.Sprintf("legacy-%d", i), "dimo.status", "did:1", ts))
	}
	f := writeLegacyBundle(t, legacyRoot, "batch-cccc.parquet", legacy)
	_, err := l.Backfill(ctx, []string{f}, zerolog.Nop())
	require.NoError(t, err)

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close()
	var native []cloudevent.StoredEvent
	for i := range 25 {
		native = append(native, testEvent(fmt.Sprintf("native-%d", i), "dimo.status", "did:1", ts))
	}
	require.NoError(t, w.WriteBundle(ctx, native))

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, 50, n)
}
