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
	require.NoError(t, l.ensureSchema(ctx))

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

// TestReassertLayout_PausedByFreshBackfillHeartbeat proves the maintainer's
// per-cycle layout re-assert defers to a running backfill (fresh heartbeat) but
// resumes once the heartbeat is stale or cleared — so a live backfill's RESET
// window is not re-partitioned out from under it, while a crashed backfill is
// still recovered.
func TestReassertLayout_PausedByFreshBackfillHeartbeat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	cfg := Config{CatalogDSN: filepath.Join(dir, "meta.ducklake"), DataPath: filepath.Join(dir, "data")}
	l, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	// Simulate a backfill's registration window: partitioning RESET.
	_, err = l.DB().ExecContext(ctx, "ALTER TABLE lake.raw_events RESET PARTITIONED BY")
	require.NoError(t, err)

	// Fresh heartbeat → reassertLayout must NOT re-partition.
	require.NoError(t, l.heartbeatBackfillPause(ctx))
	active, err := l.backfillPauseActive(ctx)
	require.NoError(t, err)
	require.True(t, active, "a fresh heartbeat is active")
	require.NoError(t, l.reassertLayout(ctx))
	part, err := l.isPartitioned(ctx)
	require.NoError(t, err)
	require.False(t, part, "a fresh backfill heartbeat must keep reassertLayout from re-partitioning")

	// Stale heartbeat → reassertLayout resumes and recovers the layout.
	_, err = l.DB().ExecContext(ctx, "UPDATE meta.lake_backfill_pause SET updated_at = now() - INTERVAL '1 hour'")
	require.NoError(t, err)
	active, err = l.backfillPauseActive(ctx)
	require.NoError(t, err)
	require.False(t, active, "a stale heartbeat is not active")
	require.NoError(t, l.reassertLayout(ctx))
	part, err = l.isPartitioned(ctx)
	require.NoError(t, err)
	require.True(t, part, "a stale heartbeat must let reassertLayout recover the layout")

	// Cleared heartbeat is not active either.
	require.NoError(t, l.heartbeatBackfillPause(ctx))
	require.NoError(t, l.clearBackfillPause(ctx))
	active, err = l.backfillPauseActive(ctx)
	require.NoError(t, err)
	require.False(t, active, "a cleared heartbeat is not active")
}
