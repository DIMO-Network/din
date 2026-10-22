package lake

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/require"
)

// TestWriter_FilesSortedBySubject pins the physical layout dq's per-vehicle reads
// depend on: every raw_events file is written in (subject, "time") order, so each
// row group covers a narrow subject range and a single-vehicle lookup can skip the
// rest by min/max stats. The bundle mirrors ingest: time-ordered with subjects
// interleaved, spanning several (type, day) partitions in one write.
func TestWriter_FilesSortedBySubject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	l, err := Open(ctx, Config{
		CatalogDSN:   filepath.Join(dir, "meta.ducklake"),
		DataPath:     filepath.Join(dir, "data"),
		RowGroupSize: 16384,
	})
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	base := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	const n = 80000
	bundle := make([]cloudevent.StoredEvent, 0, n)
	for i := range n {
		ceType := "dimo.status"
		if i%2 == 1 {
			ceType = "dimo.event"
		}
		bundle = append(bundle, testEvent(fmt.Sprintf("e%06d", i), ceType,
			fmt.Sprintf("did:erc721:137:0xVeh:%04d", (i*7919)%500),
			base.Add(time.Duration(i)*2*time.Second))) // ~1.9 days -> 2 day partitions per type
	}
	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	require.NoError(t, w.WriteBundle(ctx, bundle))
	require.NoError(t, w.Close())

	rows, err := l.DB().QueryContext(ctx, `SELECT data_file FROM ducklake_list_files('lake', 'raw_events')`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var files []string
	for rows.Next() {
		var f string
		require.NoError(t, rows.Scan(&f))
		if !filepath.IsAbs(f) {
			f = filepath.Join(dir, "data", f)
		}
		files = append(files, f)
	}
	require.NoError(t, rows.Err())
	require.GreaterOrEqual(t, len(files), 4, "the bundle spans 2 types x 2 days")

	for _, f := range files {
		var inversions int
		require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(`
			SELECT count(*) FROM (
				SELECT subject, "time",
					lag(subject) OVER (ORDER BY file_row_number) AS ps,
					lag("time") OVER (ORDER BY file_row_number) AS pt
				FROM read_parquet(%s, file_row_number = true))
			WHERE subject < ps OR (subject = ps AND "time" < pt)`, sqlString(f))).Scan(&inversions))
		require.Zerof(t, inversions, "%s is not in (subject, time) order", filepath.Base(f))
	}
}
