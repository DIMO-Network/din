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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeAndCountRowGroups opens a lake at the given RowGroupSize, writes a
// single-day single-type bundle of n events (one (type, day) partition → one
// file), and returns the file's row group count and its largest row group.
func writeAndCountRowGroups(t *testing.T, rowGroupSize, n int) (groups, maxRows int64) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	l, err := Open(ctx, Config{
		CatalogDSN:   filepath.Join(dir, "meta.ducklake"),
		DataPath:     dataPath,
		RowGroupSize: rowGroupSize,
	})
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	base := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	var bundle []cloudevent.StoredEvent
	for i := range n {
		bundle = append(bundle, testEvent(
			fmt.Sprintf("evt-%05d", i),
			"dimo.status",
			fmt.Sprintf("did:erc721:137:0xVeh:%d", i%64),
			base.Add(time.Duration(i)*time.Second),
		))
	}
	require.NoError(t, w.WriteBundle(ctx, bundle))

	var file string
	require.NoError(t, filepath.WalkDir(dataPath, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			file = p
		}
		return nil
	}))
	require.NotEmpty(t, file, "expected a parquet file on disk")

	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT num_row_groups FROM parquet_file_metadata(%s)", sqlString(file))).Scan(&groups))
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT max(row_group_num_rows) FROM parquet_metadata(%s)", sqlString(file))).Scan(&maxRows))
	return groups, maxRows
}

// TestWriter_RowGroupSize proves parquet_row_group_size reaches DuckLake's
// writer. It is the only lever on point-read cost: parquet is read at row-group
// granularity, so at DuckDB's 122,880-row default a reader wanting 50 events
// pays for the whole `data` column chunk (measured: 15.68MB fetched over httpfs
// to return ~12KB of payload). Unset, a 20k-row write lands in one group;
// configured, it splits into bounded groups a reader can skip past.
func TestWriter_RowGroupSize(t *testing.T) {
	t.Parallel()
	const rows = 20000

	defGroups, defMax := writeAndCountRowGroups(t, 0, rows)
	assert.Equal(t, int64(1), defGroups, "unset must keep DuckDB's 122,880-row default: %d rows in one group", rows)
	assert.Equal(t, int64(rows), defMax)

	groups, maxRows := writeAndCountRowGroups(t, 8192, rows)
	assert.Equal(t, int64(8192), maxRows, "configured size must bound the row group exactly")
	assert.Greater(t, groups, defGroups, "a smaller row group size must split the file into more groups")
	t.Logf("row groups: default=%d (max %d rows), size=8192 → %d (max %d rows)",
		defGroups, defMax, groups, maxRows)
}

// TestRowGroupSize_MixedLayoutsCoexist pins the rollout invariant: row group
// size is a per-file Parquet property recorded in each file's own footer, not
// catalog-wide state, so turning the option on does not invalidate or require
// rewriting anything already written. Files at the old and new layouts sit in
// one table and one query reads across both — the same per-file-property
// reasoning that already lets backfilled DIS bundles keep their zstd codec in a
// snappy lake. Compaction then rewrites old files into the new layout on its
// own schedule, which is what makes this a gradual rollout rather than a
// migration.
func TestRowGroupSize_MixedLayoutsCoexist(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	catalog := filepath.Join(dir, "meta.ducklake")

	// day varies so each write lands in its own (type, day) partition → own file.
	write := func(rowGroupSize, day, n int, tag string) {
		l, err := Open(ctx, Config{CatalogDSN: catalog, DataPath: dataPath, RowGroupSize: rowGroupSize})
		require.NoError(t, err)
		defer l.Close() //nolint:errcheck
		w, err := l.NewWriter(ctx, RawTable)
		require.NoError(t, err)
		defer w.Close() //nolint:errcheck
		base := time.Date(2026, 6, day, 0, 0, 0, 0, time.UTC)
		var bundle []cloudevent.StoredEvent
		for i := range n {
			bundle = append(bundle, testEvent(fmt.Sprintf("%s-%05d", tag, i), "dimo.status",
				fmt.Sprintf("did:erc721:137:0xVeh:%d", i%64), base.Add(time.Duration(i)*time.Second)))
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
	}

	write(0, 8, 20000, "old")     // pre-change layout: DuckDB's default
	write(16384, 9, 20000, "new") // post-change layout

	l, err := Open(ctx, Config{CatalogDSN: catalog, DataPath: dataPath, RowGroupSize: 16384})
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	// The two files really do carry different layouts.
	var layouts []int64
	require.NoError(t, filepath.WalkDir(dataPath, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		var maxRows int64
		require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
			"SELECT max(row_group_num_rows) FROM parquet_metadata(%s)", sqlString(p))).Scan(&maxRows))
		layouts = append(layouts, maxRows)
		return nil
	}))
	assert.ElementsMatch(t, []int64{20000, 16384}, layouts,
		"expected one default-layout file and one 16384-row-group file")

	// And one query reads across both, including a point read hitting each.
	var total int64
	require.NoError(t, l.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_events").Scan(&total))
	assert.Equal(t, int64(40000), total, "mixed layouts must read as one table")

	var got int64
	require.NoError(t, l.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM lake.raw_events
		 WHERE id IN ('old-00007', 'new-00007', 'old-19999', 'new-19999')`).Scan(&got))
	assert.Equal(t, int64(4), got, "a point read must resolve rows in both layouts")
}

// TestRowGroupSize_CompactionAdoptsNewSize pins the other half of the rollout:
// files written before the option was set are not stuck at the old layout.
// ducklake_merge_adjacent_files writes its output under the currently-set
// option, so the maintenance loop converts old partitions to the new layout as
// it compacts them — no backfill step, and nothing to schedule by hand.
func TestRowGroupSize_CompactionAdoptsNewSize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	catalog := filepath.Join(dir, "meta.ducklake")

	// Several bundles into ONE (type, day) partition so they are adjacent and
	// mergeable, all written with the option unset.
	l0, err := Open(ctx, Config{CatalogDSN: catalog, DataPath: dataPath})
	require.NoError(t, err)
	base := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	for b := range 4 {
		w, err := l0.NewWriter(ctx, RawTable)
		require.NoError(t, err)
		var bundle []cloudevent.StoredEvent
		for i := range 9000 {
			bundle = append(bundle, testEvent(fmt.Sprintf("b%d-%05d", b, i), "dimo.status",
				fmt.Sprintf("did:erc721:137:0xVeh:%d", i%64),
				base.Add(time.Duration(b*9000+i)*time.Second)))
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
		require.NoError(t, w.Close())
	}
	require.NoError(t, l0.Close())

	// Reopen with the option set, then compact.
	l, err := Open(ctx, Config{CatalogDSN: catalog, DataPath: dataPath, RowGroupSize: 16384})
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck
	_, err = l.DB().ExecContext(ctx, "CALL ducklake_merge_adjacent_files('lake')")
	require.NoError(t, err)

	// The live file for the partition is the compacted one, at the new layout.
	var path string
	require.NoError(t, l.DB().QueryRowContext(ctx,
		`SELECT data_file FROM ducklake_list_files('lake', 'raw_events')`).Scan(&path))
	if !filepath.IsAbs(path) {
		path = filepath.Join(dataPath, path)
	}
	var groups, maxRows int64
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT num_row_groups FROM parquet_file_metadata(%s)", sqlString(path))).Scan(&groups))
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT max(row_group_num_rows) FROM parquet_metadata(%s)", sqlString(path))).Scan(&maxRows))
	assert.Equal(t, int64(16384), maxRows,
		"compaction must write its output under the currently-set row group size")
	assert.Greater(t, groups, int64(1))

	var total int64
	require.NoError(t, l.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_events").Scan(&total))
	assert.Equal(t, int64(36000), total, "compaction must not lose rows")
}

// TestWriter_RowGroupSize_VectorGranularity pins the rounding DuckDB applies:
// row groups flush at vector boundaries, so the option is rounded UP to a
// multiple of the 2048-row vector size and can never go below it. Asking for
// 512 or 2048 both yield 2048; 3000 yields 4096. This is why the configured
// default is 16384 — an exact multiple, so the value that ships is the value
// that lands, and shrinking further than 2048 is not available as a lever.
func TestWriter_RowGroupSize_VectorGranularity(t *testing.T) {
	t.Parallel()
	for size, want := range map[int]int64{
		512:   2048,  // below the vector size → floored to it
		2048:  2048,  // exact
		3000:  4096,  // rounded up to the next vector boundary
		16384: 16384, // the shipped default, an exact multiple
	} {
		_, maxRows := writeAndCountRowGroups(t, size, 20000)
		assert.Equal(t, want, maxRows, "RowGroupSize=%d", size)
	}
}
