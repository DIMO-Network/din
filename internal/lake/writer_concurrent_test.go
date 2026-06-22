package lake

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/require"
)

// TestWriter_MultiConnConcurrentBundles proves a pooled writer commits bundles
// concurrently across its connections without DuckLake commit conflicts — raw
// writes are pure appends (no shared cursor row, no overlapping files), so they
// are commutative and don't contend at the catalog the way the materializer's
// cursor CAS does.
func TestWriter_MultiConnConcurrentBundles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	cfg := Config{
		CatalogDSN: filepath.Join(dir, "meta.ducklake"),
		DataPath:   filepath.Join(dir, "data"),
		MaxConns:   8,
	}
	l, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	w, err := l.NewWriterN(ctx, RawTable, 3)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck
	require.Len(t, w.conns, 3)

	const bundles = 9
	day := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make([]error, bundles)
	for i := range bundles {
		wg.Go(func() {
			ev := []cloudevent.StoredEvent{testEvent(fmt.Sprintf("b%d", i), "dimo.status", "did:1", day)}
			errs[i] = w.WriteBundle(ctx, ev)
		})
	}
	wg.Wait()
	for i, e := range errs {
		require.NoError(t, e, "bundle %d", i)
	}

	var count int
	require.NoError(t, l.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_events").Scan(&count))
	require.Equal(t, bundles, count, "every concurrent bundle persisted across the connection pool")
}
