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
	dto "github.com/prometheus/client_model/go"
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

// The load review #10 fix: merge runs as bounded, independently-committed
// sub-calls, so partial progress survives a timeout instead of rolling back the
// whole compaction. This pins that each sub-call commits its own snapshot, the
// backlog shrinks, the change feed is preserved, and the loop converges.
func TestMaintainer_BoundedMergeCommitsPerSubCall(t *testing.T) {
	// Not Parallel: it overrides the package-level mergeMaxFilesPerCall.
	prev := mergeMaxFilesPerCall
	mergeMaxFilesPerCall = 2 // force several sub-calls over a handful of files
	defer func() { mergeMaxFilesPerCall = prev }()

	l, _ := openTestLake(t)
	ctx := context.Background()
	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	var startSnapshot int64
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT max(snapshot_id) FROM lake.snapshots()").Scan(&startSnapshot))

	// 6 (type, day) partitions × 4 file-backed bundles = many small merge groups
	// so a max_compacted_files of 2 cannot finish in a single CALL.
	total := 0
	for d := range 6 {
		ts := time.Date(2026, 6, 1+d, 10, 0, 0, 0, time.UTC)
		for b := range 4 {
			var bundle []cloudevent.StoredEvent
			for i := range 20 {
				bundle = append(bundle, testEvent(fmt.Sprintf("d%d-b%d-%d", d, b, i), "dimo.status", "did:1", ts))
				total++
			}
			require.NoError(t, w.WriteBundle(ctx, bundle))
		}
	}
	// The catalog file count (not the on-disk parquet count) is the backlog:
	// merge writes new files but leaves the superseded ones on disk until the
	// cleanup step, which runBoundedMerge does not run.
	catalogFiles := func() int64 {
		var n int64
		require.NoError(t, l.DB().QueryRowContext(ctx,
			"SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = 'raw_events'").Scan(&n))
		return n
	}
	filesBefore := catalogFiles()
	require.GreaterOrEqual(t, filesBefore, int64(24))

	snap := func() int64 {
		var n int64
		require.NoError(t, l.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.snapshots()").Scan(&n))
		return n
	}
	snapsBefore := snap()

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour}, zerolog.Nop())
	subCalls, err := m.runBoundedMerge(ctx)
	require.NoError(t, err)
	require.Greater(t, subCalls, 1, "a bounded merge over 24 files with a 2-file cap must take several sub-calls")

	// Each sub-call is its own committed transaction ⇒ one new snapshot per call.
	// This is what makes partial progress durable: a later timeout can't discard
	// the snapshots earlier sub-calls already committed.
	assert.Equal(t, int64(subCalls), snap()-snapsBefore,
		"each merge sub-call must commit exactly one snapshot (independent, durable progress)")

	// The catalog backlog actually shrank.
	assert.Less(t, catalogFiles(), filesBefore, "bounded merge must reduce the file backlog")

	// Merge must not rewrite history: every inserted row is still readable exactly
	// once, and the change feed over the range still yields every insert.
	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, total, n, "current snapshot reads every row exactly once after bounded merge")

	var endSnapshot int64
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT max(snapshot_id) FROM lake.snapshots()").Scan(&endSnapshot))
	var inserted int
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(*) FROM lake.table_insertions('raw_events', %d, %d)",
		startSnapshot+1, endSnapshot)).Scan(&inserted))
	assert.Equal(t, total, inserted, "bounded merge must not rewrite the change feed")

	// Converged: a second pass finds nothing left to merge.
	again, err := m.runBoundedMerge(ctx)
	require.NoError(t, err)
	assert.Zero(t, again, "bounded merge must converge — no work left on the second pass")
}

// A full cycle must publish the small-file-backlog gauge so a compaction stall
// is visible (load review #10). The gauge must match the catalog's own file
// count for raw_events.
func TestMaintainer_DataFileGaugeMatchesCatalog(t *testing.T) {
	// Not Parallel: reads a shared default-registry gauge.
	l, _ := openTestLake(t)
	ctx := context.Background()
	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	for b := range 3 {
		var bundle []cloudevent.StoredEvent
		for i := range 20 {
			bundle = append(bundle, testEvent(fmt.Sprintf("b%d-%d", b, i), "dimo.status", "did:1", ts))
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
	}

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	var catalogFiles int64
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = 'raw_events'").Scan(&catalogFiles))

	var mtr dto.Metric
	require.NoError(t, maintDataFiles.WithLabelValues(RawTable).Write(&mtr))
	got := mtr.GetGauge().GetValue()
	assert.Equal(t, float64(catalogFiles), got, "din_lake_data_files must mirror the catalog file count")
	assert.Positive(t, catalogFiles, "the test wrote file-backed bundles, so raw_events must have data files")
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
