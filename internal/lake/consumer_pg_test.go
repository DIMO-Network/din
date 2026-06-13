package lake

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsumerProgress_Postgres exercises the prod transport that the
// file-catalog tests can't: the `meta` database attached directly to the
// catalog Postgres, with the progress table created and upserted through
// DuckDB's postgres extension. Skips unless LAKE_TEST_PG_DSN points at a
// throwaway Postgres (e.g. host=localhost port=15432 dbname=ducklake
// user=postgres password=pw).
func TestConsumerProgress_Postgres(t *testing.T) {
	dsn := os.Getenv("LAKE_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set LAKE_TEST_PG_DSN to a throwaway Postgres to run the catalog-transport test")
	}
	ctx := context.Background()
	l, err := Open(ctx, Config{CatalogDSN: dsn, DataPath: filepath.Join(t.TempDir(), "data")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	// Unique names so the test is isolated on a shared DB; clean up after.
	a, b := "pgtest-a-"+t.Name(), "pgtest-b-"+t.Name()
	t.Cleanup(func() {
		_, _ = l.DB().ExecContext(ctx,
			"DELETE FROM meta.din_consumer_progress WHERE consumer IN (?, ?)", a, b)
	})

	require.NoError(t, l.RecordConsumerProgress(ctx, a, 100))
	require.NoError(t, l.RecordConsumerProgress(ctx, b, 250))

	floor, ok, err := l.ConsumerFloor(ctx, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(100), floor, "floor is the slowest consumer, read back through Postgres")

	// Upsert advances the slow one; floor follows.
	require.NoError(t, l.RecordConsumerProgress(ctx, a, 300))
	floor, _, err = l.ConsumerFloor(ctx, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(250), floor)
}
