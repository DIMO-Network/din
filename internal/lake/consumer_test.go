package lake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSnapshots commits n file-backed bundles and returns the snapshot id
// after each, so a test can place a consumer cursor at a known point.
func writeSnapshots(t *testing.T, l *Lake, n int) []int64 {
	t.Helper()
	ctx := context.Background()
	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck
	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	ids := make([]int64, 0, n)
	for b := range n {
		var bundle []cloudevent.StoredEvent
		for i := range 30 { // clear the inlining threshold so each bundle is a file
			bundle = append(bundle, testEvent(fmt.Sprintf("b%d-%d", b, i), "dimo.status", "did:1", ts))
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
		var id int64
		require.NoError(t, l.DB().QueryRowContext(ctx,
			"SELECT max(snapshot_id) FROM lake.snapshots()").Scan(&id))
		ids = append(ids, id)
	}
	return ids
}

func snapshotCount(t *testing.T, l *Lake) int {
	t.Helper()
	var n int
	require.NoError(t, l.DB().QueryRowContext(context.Background(),
		"SELECT count(*) FROM lake.snapshots()").Scan(&n))
	return n
}

func TestConsumerFloor_RecordAndRead(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	_, ok, err := l.ConsumerFloor(ctx, time.Hour)
	require.NoError(t, err)
	assert.False(t, ok, "no consumers -> no floor")

	require.NoError(t, l.RecordConsumerProgress(ctx, "dq", 5))
	require.NoError(t, l.RecordConsumerProgress(ctx, "other", 9))
	floor, ok, err := l.ConsumerFloor(ctx, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(5), floor, "floor is the slowest consumer")

	// Upsert: dq advances; floor follows the new minimum.
	require.NoError(t, l.RecordConsumerProgress(ctx, "dq", 12))
	floor, _, err = l.ConsumerFloor(ctx, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(9), floor)
}

func TestConsumerFloor_StalenessExcludesDeadConsumers(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	require.NoError(t, l.RecordConsumerProgress(ctx, "dq", 3))
	// Backdate the row to simulate a consumer that stopped reporting.
	_, err := l.DB().ExecContext(ctx,
		"UPDATE meta.din_consumer_progress SET updated_at = now() - INTERVAL '2 hours' WHERE consumer = 'dq'")
	require.NoError(t, err)

	_, ok, err := l.ConsumerFloor(ctx, time.Hour)
	require.NoError(t, err)
	assert.False(t, ok, "a consumer stale beyond the window is presumed dead and dropped")

	_, ok, err = l.ConsumerFloor(ctx, 3*time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "within a wider window it still counts")
}

// The floor holds expiry back: a lagging live consumer's unconsumed
// snapshots survive a zero-retention maintenance cycle.
func TestMaintainer_FloorHoldsExpiry(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	ids := writeSnapshots(t, l, 6)
	before := snapshotCount(t, l)
	// Consumer has only processed up to the 2nd snapshot.
	require.NoError(t, l.RecordConsumerProgress(ctx, "dq", ids[1]))

	// Zero retention would expire everything but the current snapshot —
	// except the floor protects everything after the consumer's cursor.
	m := NewMaintainer(l, MaintConfig{SnapshotKeep: time.Nanosecond, ConsumerStaleness: time.Hour}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	after := snapshotCount(t, l)
	assert.Less(t, after, before, "snapshots at/below the cursor still expire")
	// Everything the consumer hasn't read must still be queryable via the feed.
	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(*) FROM lake.table_insertions('raw_events', %d, %d)", ids[1]+1, ids[len(ids)-1])).Scan(&n))
	assert.Positive(t, n, "unconsumed snapshots survive for the lagging consumer")

	// All rows remain (expiry drops history, not live data).
	var rows int
	require.NoError(t, l.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_events").Scan(&rows))
	assert.Equal(t, 6*30, rows)
}

// With the consumer caught up to head, the floor doesn't bind and
// retention expires history as usual.
func TestMaintainer_CaughtUpConsumerDoesNotBlock(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	ids := writeSnapshots(t, l, 5)
	require.NoError(t, l.RecordConsumerProgress(ctx, "dq", ids[len(ids)-1])) // at head
	before := snapshotCount(t, l)

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: time.Nanosecond, ConsumerStaleness: time.Hour}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	after := snapshotCount(t, l)
	assert.Less(t, after, before, "a caught-up consumer must not hold expiry back")
}

// A dead (stale) consumer does not wedge expiry: once it ages out of the
// window, retention proceeds even though its cursor is far behind.
func TestMaintainer_DeadConsumerDoesNotWedge(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	ids := writeSnapshots(t, l, 5)
	require.NoError(t, l.RecordConsumerProgress(ctx, "dq", ids[0]))
	_, err := l.DB().ExecContext(ctx,
		"UPDATE meta.din_consumer_progress SET updated_at = now() - INTERVAL '2 hours' WHERE consumer = 'dq'")
	require.NoError(t, err)
	before := snapshotCount(t, l)

	m := NewMaintainer(l, MaintConfig{SnapshotKeep: time.Nanosecond, ConsumerStaleness: time.Hour}, zerolog.Nop())
	require.NoError(t, m.Cycle(ctx))

	after := snapshotCount(t, l)
	assert.Less(t, after, before, "a consumer stale past the window must not block expiry")
}
