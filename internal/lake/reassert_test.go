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
	"github.com/stretchr/testify/require"
)

// TestEnsureSchema_ReassertsPartitioningOnReboot proves a reboot re-asserts the
// raw_events partition layout even when the table already exists. A backfill
// drops partitioning for its registration window and restores it in a defer; a
// SIGKILL skips the defer and leaves the table unpartitioned, and the existence
// check would otherwise make the table permanently unpartitioned (CHD-23).
func TestEnsureSchema_ReassertsPartitioningOnReboot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	cfg := Config{CatalogDSN: filepath.Join(dir, "meta.ducklake"), DataPath: dataPath}

	l, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	// Simulate a crashed backfill: partitioning was RESET and the restore defer
	// never ran.
	_, err = l.DB().ExecContext(ctx, "ALTER TABLE lake.raw_events RESET PARTITIONED BY")
	require.NoError(t, err)

	// A reboot re-runs ensureSchema; it must re-assert the layout, not
	// early-return because the table exists.
	require.NoError(t, l.ensureSchema(ctx, cfg))

	// Write data spanning three (type, day) partitions; a partitioned table
	// writes one file per partition.
	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck
	day1 := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	var bundle []cloudevent.StoredEvent
	for i := range 20 {
		bundle = append(bundle,
			testEvent(fmt.Sprintf("s1-%d", i), "dimo.status", "did:1", day1),
			testEvent(fmt.Sprintf("s2-%d", i), "dimo.status", "did:1", day2),
			testEvent(fmt.Sprintf("f-%d", i), "dimo.fingerprint", "did:2", day2),
		)
	}
	require.NoError(t, w.WriteBundle(ctx, bundle))

	var files []string
	require.NoError(t, filepath.WalkDir(dataPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			files = append(files, path)
		}
		return nil
	}))
	require.Len(t, files, 3, "re-asserted partitioning writes one file per (type,day): %v", files)
}
