package lake

import (
	"context"
	"errors"
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

// writeDeleteHeavyPartition writes 4 file-backed bundles into one (type, day)
// partition and deletes 60% of each bundle's rows, so every data file carries a
// delete file and sits past the default rewrite threshold (0.5). Returns the
// surviving row count and a closure counting current-snapshot data files that
// still carry a delete file.
func writeDeleteHeavyPartition(t *testing.T, l *Lake) (int, func() int) {
	t.Helper()
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	keep := 0
	for b := range 4 {
		var bundle []cloudevent.StoredEvent
		for i := range 30 {
			prefix := "del"
			if i >= 18 {
				prefix = "keep"
				keep++
			}
			bundle = append(bundle, testEvent(fmt.Sprintf("%s-%d-%d", prefix, b, i), "dimo.status", "did:1", ts))
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
	}

	res, err := l.DB().ExecContext(ctx, "DELETE FROM lake.raw_events WHERE id LIKE 'del-%'")
	require.NoError(t, err)
	deleted, err := res.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 4*18, deleted)

	deleteCarrying := func() int {
		var n int
		require.NoError(t, l.DB().QueryRowContext(ctx,
			"SELECT count(*) FROM ducklake_list_files('lake', 'raw_events') WHERE delete_file IS NOT NULL").Scan(&n))
		return n
	}
	require.Positive(t, deleteCarrying(), "deletes must land as merge-on-read delete files for this test to bite")
	return keep, deleteCarrying
}

// The 2026-07-17 read-mirror incident regression: merge_adjacent_files never
// compacts a file that carries a delete file, so DELETE+INSERT-churned tables
// (dq's signals_latest/events_latest rollups) fragment without bound unless
// maintenance first rewrites delete-heavy files live-rows-only. This pins that
// the rewrite step reclaims every past-threshold file and keeps surviving rows
// intact.
func TestMaintainer_RewriteReclaimsDeleteHeavyFiles(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()
	keep, deleteCarrying := writeDeleteHeavyPartition(t, l)

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	assert.Zero(t, deleteCarrying(), "every file was past the threshold; all must be rewritten live-rows-only")
	var rows int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&rows))
	assert.Equal(t, keep, rows)
}

// A negative threshold disables the rewrite step (mirrors the OrphanRetention
// disable contract), leaving delete-carrying files in place.
func TestMaintainer_RewriteDisabledByNegativeThreshold(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()
	_, deleteCarrying := writeDeleteHeavyPartition(t, l)

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour, RewriteDeleteThreshold: -1}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	assert.Positive(t, deleteCarrying(), "rewrite disabled: delete files must survive the cycle")
}

// The load review #10 fix: merge runs as bounded, independently-committed
// sub-calls, so partial progress survives a timeout instead of rolling back the
// whole compaction. This pins that each sub-call commits its own snapshot, the
// backlog shrinks, the change feed is preserved, and the loop converges.
func TestMaintainer_BoundedMergeCommitsPerSubCall(t *testing.T) {
	t.Parallel()
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

	// MergeMaxFilesPerCall pins the ceiling to 2, forcing several sub-calls over the
	// 24-file backlog (the ramp is clamped to this explicit cap, so it never grows).
	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour, MergeMaxFilesPerCall: 2}, zerolog.Nop())
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

// parseByteSize is the input to the derived merge ceiling: a wrong parse would size
// the whole memory guard wrong. Pin the suffix handling and the "give up → 0"
// contract that routes callers to the fallback ceiling (issue #11).
func TestParseByteSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"1073741824", 1 << 30},
		{"512MB", 512 << 20},
		{"512mb", 512 << 20},
		{" 1GB ", 1 << 30},
		{"2GiB", 2 << 30},
		{"4KB", 4 << 10},
		{"1TB", 1 << 40},
		{"1.5GB", 3 << 29}, // 1.5 * 2^30
		{"80%", 0},         // percentage form is unresolvable without total RAM
		{"garbage", 0},
		{"-5MB", 0},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, parseByteSize(c.in), "parseByteSize(%q)", c.in)
	}
}

// isOutOfMemory decides whether a merge error is a graceful DuckDB OOM (back off the
// ramp, don't fail the cycle) or a real failure (surface it). Pin both classes so a
// wording change on either side is caught (issue #11).
func TestIsOutOfMemory(t *testing.T) {
	t.Parallel()
	assert.False(t, isOutOfMemory(nil))
	assert.False(t, isOutOfMemory(errors.New("TransactionContext Error: conflict")))
	assert.False(t, isOutOfMemory(errors.New("connection refused")))
	assert.True(t, isOutOfMemory(errors.New("Out of Memory Error: could not allocate block of size 76.5 MiB (2.7 GiB/2.7 GiB used)")))
	assert.True(t, isOutOfMemory(errors.New("could not allocate 512MB")))
}

// The graceful-OOM contract: when a merge sub-call hits the DuckDB memory limit,
// the maintainer must NOT fail the cycle (which would march it toward the 4-strike
// restart backstop while it is in fact making progress). It halves the per-call cap,
// counts the backoff, and returns cleanly so the remaining files merge next cycle —
// and a real (non-OOM) error must still surface. Uses the fault seam because real
// memory pressure can't be reproduced deterministically here (issue #11).
func TestMaintainer_MergeOOMBacksOffWithoutFailingCycle(t *testing.T) {
	// Not Parallel: mutates the package-level fault hook and reads a shared counter.
	l, _ := openTestLake(t)
	ctx := context.Background()

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour}, zerolog.Nop())
	startRamp := m.mergeRamp
	require.Equal(t, mergeRampStartFiles, startRamp)

	backoffs := func() float64 {
		var mtr dto.Metric
		require.NoError(t, mergeOOMBackoffs.Write(&mtr))
		return mtr.GetCounter().GetValue()
	}
	before := backoffs()

	// First sub-call reports a graceful DuckDB OOM.
	mergeExecFaultHook = func(subCall int) (int, error) {
		return 0, errors.New("Out of Memory Error: could not allocate block of size 76.5 MiB (2.7 GiB/2.7 GiB used)")
	}
	defer func() { mergeExecFaultHook = nil }()

	subCalls, err := m.runBoundedMerge(ctx)
	require.NoError(t, err, "a graceful OOM must not fail the cycle")
	assert.Zero(t, subCalls, "the OOM sub-call committed nothing")
	assert.Equal(t, startRamp/2, m.mergeRamp, "the per-call cap must halve on OOM")
	assert.Equal(t, before+1, backoffs(), "the OOM backoff must be counted")

	// A non-OOM error, by contrast, must surface as a real failure and leave the cap alone.
	rampBefore := m.mergeRamp
	mergeExecFaultHook = func(subCall int) (int, error) {
		return 0, errors.New("connection refused")
	}
	_, err = m.runBoundedMerge(ctx)
	require.Error(t, err, "a non-OOM error must fail the cycle")
	assert.False(t, isOutOfMemory(err))
	assert.Equal(t, rampBefore, m.mergeRamp, "a non-OOM error must not move the ramp")
}

// The merge ceiling is what bounds a sub-call's working set to the memory budget.
// Pin the three regimes: explicit pin wins, a real budget derives a finite ceiling
// no larger than the hard cap, and a missing budget falls back — never unbounded.
func TestMaintainer_MergeCeiling(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()
	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck
	// Land some files so maxAvgFileBytes has a real average to divide into.
	ts := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC)
	for b := range 3 {
		var bundle []cloudevent.StoredEvent
		for i := range 20 {
			bundle = append(bundle, testEvent(fmt.Sprintf("c%d-%d", b, i), "dimo.status", "did:1", ts))
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
	}

	// An explicit pin wins outright and is clamped to the hard ceiling.
	pinned := NewMaintainer(l, MaintConfig{MergeMaxFilesPerCall: 7}, zerolog.Nop())
	assert.Equal(t, 7, pinned.mergeCeiling(ctx))
	pinnedHigh := NewMaintainer(l, MaintConfig{MergeMaxFilesPerCall: 999999}, zerolog.Nop())
	assert.Equal(t, mergeMaxFilesHardCeiling, pinnedHigh.mergeCeiling(ctx))

	// No budget string ⇒ fallback (never the old unbounded behavior).
	nobudget := NewMaintainer(l, MaintConfig{}, zerolog.Nop())
	assert.Equal(t, mergeMaxFilesFallback, nobudget.mergeCeiling(ctx))

	// A real budget derives a finite, in-range ceiling, and a larger budget never
	// yields a smaller ceiling (monotonic in memory).
	small := NewMaintainer(l, MaintConfig{MemoryLimit: "64MB"}, zerolog.Nop())
	big := NewMaintainer(l, MaintConfig{MemoryLimit: "4GB"}, zerolog.Nop())
	cSmall, cBig := small.mergeCeiling(ctx), big.mergeCeiling(ctx)
	assert.GreaterOrEqual(t, cSmall, mergeMinFilesPerCall)
	assert.LessOrEqual(t, cBig, mergeMaxFilesHardCeiling)
	assert.GreaterOrEqual(t, cBig, cSmall, "a larger memory budget must not derive a smaller merge ceiling")
}

// The cold-start guarantee: a maintainer with no explicit cap starts its ramp small
// (so a cold, uncompacted lake's first sub-call always fits) and ramps UP after a
// sub-call commits, converging toward the ceiling instead of attacking the whole
// backlog in call #1 (issue #11).
func TestMaintainer_MergeRampStartsSmallAndClimbs(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()
	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	// A default maintainer (no pin, no budget → fallback ceiling well above the start).
	m := NewMaintainer(l, MaintConfig{SnapshotKeep: 72 * time.Hour}, zerolog.Nop())
	assert.Equal(t, mergeRampStartFiles, m.mergeRamp, "the ramp must start small on boot")
	assert.Greater(t, m.mergeCeiling(ctx), mergeRampStartFiles,
		"ceiling must exceed the start so the ramp has room to climb")

	// Land enough small files that at least one sub-call merges real work.
	total := 0
	for d := range 4 {
		ts := time.Date(2026, 6, 1+d, 10, 0, 0, 0, time.UTC)
		for b := range 3 {
			var bundle []cloudevent.StoredEvent
			for i := range 20 {
				bundle = append(bundle, testEvent(fmt.Sprintf("r%d-%d-%d", d, b, i), "dimo.status", "did:1", ts))
				total++
			}
			require.NoError(t, w.WriteBundle(ctx, bundle))
		}
	}

	subCalls, err := m.runBoundedMerge(ctx)
	require.NoError(t, err)
	require.Positive(t, subCalls, "expected the merge to do real work")
	assert.Greater(t, m.mergeRamp, mergeRampStartFiles,
		"a committed sub-call must ramp the per-call cap UP toward the ceiling")
	assert.LessOrEqual(t, m.mergeRamp, m.mergeCeiling(ctx), "the ramp must never exceed the ceiling")
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
