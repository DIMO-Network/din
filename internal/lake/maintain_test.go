package lake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countParquetFiles(t *testing.T, dataPath string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dataPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			n++
		}
		return nil
	})
	require.NoError(t, err)
	return n
}

// The load-bearing assertion for downstream consumers: merging files must
// not rewrite history. A reader paging the change feed by snapshot id
// sees every inserted row exactly as committed, before and after
// maintenance.
func TestMaintainer_MergePreservesChangeFeed(t *testing.T) {
	t.Parallel()
	l, dataPath := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	var startSnapshot int64
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT max(snapshot_id) FROM lake.snapshots()").Scan(&startSnapshot))

	// 5 file-backed bundles plus one inlined straggler, all in one
	// (type, day) partition so they are merge candidates.
	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	total := 0
	for b := range 5 {
		var bundle []cloudevent.StoredEvent
		for i := range 30 {
			bundle = append(bundle, testEvent(fmt.Sprintf("b%d-%d", b, i), "dimo.status", "did:1", ts))
			total++
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
	}
	require.NoError(t, w.WriteBundle(ctx, []cloudevent.StoredEvent{
		testEvent("inlined", "dimo.status", "did:1", ts)}))
	total++

	filesBefore := countParquetFiles(t, dataPath)
	require.GreaterOrEqual(t, filesBefore, 5)

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	// The current snapshot reads everything, exactly once.
	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, total, n)

	var endSnapshot int64
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT max(snapshot_id) FROM lake.snapshots()").Scan(&endSnapshot))

	// Change feed over the whole range still yields every insert.
	var inserted int
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(*) FROM lake.table_insertions('raw_events', %d, %d)",
		startSnapshot+1, endSnapshot)).Scan(&inserted))
	assert.Equal(t, total, inserted, "merge must not rewrite the change feed")
}

func TestMaintainer_RetentionReleasesFiles(t *testing.T) {
	t.Parallel()
	l, dataPath := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	total := 0
	for b := range 4 {
		var bundle []cloudevent.StoredEvent
		for i := range 30 {
			bundle = append(bundle, testEvent(fmt.Sprintf("b%d-%d", b, i), "dimo.status", "did:1", ts))
			total++
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
	}
	filesBefore := countParquetFiles(t, dataPath)
	require.GreaterOrEqual(t, filesBefore, 4)

	// Zero retention: everything but the current snapshot expires, and
	// the pre-merge files get deleted from disk.
	m := NewMaintainer(l, MaintConfig{SnapshotKeep: time.Nanosecond}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	filesAfter := countParquetFiles(t, dataPath)
	assert.Less(t, filesAfter, filesBefore, "expired snapshots must release merged-away files")

	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, total, n, "current snapshot survives expiry")
}

// TestMaintainer_OrphanSweepWindowAndDisable pins the B6 disaster-recovery
// guard: the orphan sweep must honor LAKE_ORPHAN_RETENTION — a young orphan
// (inside the window) survives, a negative window disables the sweep entirely
// (the post-restore freeze), and only an aged-out orphan is destroyed. After a
// catalog PITR restore, files written past the restore point are exactly such
// orphans — and with encryption on, deleting them is unrecoverable.
func TestMaintainer_OrphanSweepWindowAndDisable(t *testing.T) {
	t.Parallel()
	l, dataPath := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck
	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	require.NoError(t, w.WriteBundle(ctx, []cloudevent.StoredEvent{testEvent("o-1", "dimo.status", "did:1", ts)}))

	// An unreferenced file in the data path = an orphan, as a post-restore
	// world would have it.
	orphan := filepath.Join(dataPath, "orphan-restore-era.parquet")
	require.NoError(t, os.WriteFile(orphan, []byte("not-in-catalog"), 0o644))

	exists := func() bool {
		_, err := os.Stat(orphan)
		return err == nil
	}

	// Default-shaped window (7d): the young orphan is inside it and survives.
	m := NewMaintainer(l, MaintConfig{}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))
	assert.True(t, exists(), "orphan inside the retention window must survive the sweep")

	// Disabled sweep (post-restore freeze): survives even though it would age out.
	m = NewMaintainer(l, MaintConfig{OrphanRetention: -time.Second}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))
	assert.True(t, exists(), "negative window must skip the sweep entirely")

	// Aged out (tiny window): now — and only now — it is destroyed.
	m = NewMaintainer(l, MaintConfig{OrphanRetention: time.Nanosecond}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))
	assert.False(t, exists(), "orphan past the window is deleted")
}
