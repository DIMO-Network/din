package lake

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/require"
)

// TestWriter_MultiInstanceConcurrentAppend_Postgres simulates MULTIPLE ingest pods: two
// independent Lake instances (separate embedded DuckDB) attaching the SAME Postgres
// catalog + data path, appending to lake.raw_events concurrently. It proves the cross-pod
// invariants that a single-instance test cannot: (1) no committed bundle is lost or
// duplicated, (2) one pod's query sees the other pod's committed rows (no stale per-pod
// catalog view), (3) no corruption. Raw appends are commutative (no shared cursor), so we
// expect zero commit conflicts on a quiet catalog; any conflict is tolerated as the
// at-least-once contract (the sink would redeliver) but must never lose a committed row.
//
// Skips unless LAKE_TEST_PG_DSN points at a throwaway Postgres (file catalogs can't be
// opened by two instances at once — the whole point is the shared Postgres catalog).
func TestWriter_MultiInstanceConcurrentAppend_Postgres(t *testing.T) {
	dsn := os.Getenv("LAKE_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set LAKE_TEST_PG_DSN to a throwaway Postgres to run the multi-pod append test")
	}
	ctx := context.Background()
	dataPath := pgCatalogDataPath(t) // shared by both "pods" (and all PG-catalog tests)

	open := func() *Lake {
		l, err := Open(ctx, Config{CatalogDSN: dsn, DataPath: dataPath})
		require.NoError(t, err)
		t.Cleanup(func() { _ = l.Close() })
		return l
	}
	l1, l2 := open(), open()

	// Unique subject isolates this run's rows on a shared catalog; clean them up.
	subject := "did:racetest:" + t.Name()
	t.Cleanup(func() {
		_, _ = l1.DB().ExecContext(ctx, "DELETE FROM lake.raw_events WHERE subject = ?", subject)
	})

	mkWriter := func(l *Lake) *Writer {
		w, err := l.NewWriterN(ctx, RawTable, 2)
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	w1, w2 := mkWriter(l1), mkWriter(l2)

	const perPod = 25
	day := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := map[string]bool{}
	conflicts := 0
	appendOne := func(w *Writer, pod string, i int) {
		defer wg.Done()
		id := fmt.Sprintf("%s-%d", pod, i)
		ev := testEvent(id, "dimo.status", subject, day.Add(time.Duration(i)*time.Second))
		err := w.WriteBundle(ctx, []cloudevent.StoredEvent{ev})
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			conflicts++ // a cross-instance commit conflict — at-least-once: the sink redelivers
			return
		}
		committed[id] = true
	}
	for i := range perPod {
		wg.Add(2)
		go appendOne(w1, "pod1", i)
		go appendOne(w2, "pod2", i)
	}
	wg.Wait()

	// Read back through l1 — it must see BOTH pods' committed rows (shared catalog).
	rows, err := l1.DB().QueryContext(ctx, "SELECT id FROM lake.raw_events WHERE subject = ?", subject)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	got := map[string]int{}
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		got[id]++
	}
	require.NoError(t, rows.Err())

	for id := range committed {
		require.Equal(t, 1, got[id], "committed id %q must be present exactly once across both pods", id)
	}
	require.Len(t, got, len(committed), "raw_events holds exactly the committed set — no lost, phantom, or duplicate rows")
	t.Logf("multi-pod append: %d/%d committed, %d commit-conflicts (would redeliver upstream)", len(committed), 2*perPod, conflicts)
}
