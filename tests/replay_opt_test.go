// replay_opt_test.go holds optimization experiments layered on the replay
// harness (parquet_replay_test.go): a compression sweep on the real-bytes lake
// write (the materialize lever) and a micro-benchmark of the per-event
// ValidIdentifier regex (a decode lever). Same -replay gate.
//
//	go test ./tests/ -run TestReplayLakeCompression -replay -v -timeout 20m
//	go test ./tests/ -run TestReplayLakeBundle      -replay -v -timeout 20m
//	go test ./tests/ -bench BenchmarkValidIdentifier -run x -replay
package tests

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/lake"
	"github.com/stretchr/testify/require"
)

// storedFromDump builds StoredEvents from real dump rows verbatim (real bytes,
// real subject/type/time) — the faithful materialize input.
func storedFromDump(events []loadedEvent) []cloudevent.StoredEvent {
	stored := make([]cloudevent.StoredEvent, len(events))
	for i, e := range events {
		stored[i] = cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: "1.0", ID: e.id, Source: replaySource,
				Subject: e.subject, Producer: e.producer, Time: e.t,
				Type: e.typ, DataContentType: e.dct, DataVersion: e.dver,
			},
			Data: e.data,
		}}
	}
	return stored
}

// dirBytes sums the size of every .parquet file under root — the on-disk cost
// that pairs with each codec's write speed.
func dirBytes(t *testing.T, root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if fi, e := d.Info(); e == nil {
				total += fi.Size()
			}
		}
		return nil
	})
	return total
}

func writeBundles(t *testing.T, w *lake.Writer, stored []cloudevent.StoredEvent, bundle, conns int) time.Duration {
	var bundles [][]cloudevent.StoredEvent
	for i := 0; i < len(stored); i += bundle {
		bundles = append(bundles, stored[i:min(i+bundle, len(stored))])
	}
	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, conns)
	errs := make(chan error, len(bundles))
	for _, b := range bundles {
		wg.Add(1)
		sem <- struct{}{}
		go func(b []cloudevent.StoredEvent) {
			defer wg.Done()
			defer func() { <-sem }()
			// require.* inside a goroutine would Goexit only this goroutine and hang
			// wg.Wait; collect and assert on the caller's goroutine instead.
			errs <- w.WriteBundle(context.Background(), b)
		}(b)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	return time.Since(start)
}

// TestReplayLakeCompression sweeps the parquet codec on the REAL dump bytes at a
// fixed writerConns/bundle, reporting both materialize throughput and the file
// size each codec produces — the speed/size tradeoff.
func TestReplayLakeCompression(t *testing.T) {
	if !*runReplay {
		t.Skip("pass -replay")
	}
	stored := storedFromDump(loadEvents(t, *replayN))
	const conns, bundle = 4, 100_000

	t.Logf("%-14s %14s %12s %10s", "compression", "events/s", "MiB on disk", "B/event")
	for _, codec := range []string{"zstd", "lz4", "snappy", "uncompressed"} {
		dir := t.TempDir()
		dataPath := filepath.Join(dir, "data")
		lk, err := lake.Open(context.Background(), lake.Config{
			CatalogDSN:  filepath.Join(dir, "meta.ducklake"),
			DataPath:    dataPath,
			MaxConns:    conns + 2,
			Compression: codec,
		})
		require.NoError(t, err)
		w, err := lk.NewWriterN(context.Background(), lake.RawTable, conns)
		require.NoError(t, err)
		dur := writeBundles(t, w, stored, bundle, conns)
		_ = w.Close()
		_ = lk.Close()
		bytes := dirBytes(t, dataPath)
		t.Logf("%-14s %14.0f %12.1f %10d", codec,
			float64(len(stored))/dur.Seconds(), float64(bytes)/(1<<20), bytes/int64(len(stored)))
	}
}

// TestReplayLakeBundle sweeps bundle size at fixed codec/conns: bigger bundles =
// fewer commits/files but more peak memory.
func TestReplayLakeBundle(t *testing.T) {
	if !*runReplay {
		t.Skip("pass -replay")
	}
	stored := storedFromDump(loadEvents(t, *replayN))
	const conns = 4
	t.Logf("%-12s %14s", "bundle", "events/s")
	for _, bundle := range []int{10_000, 25_000, 50_000, 100_000, 200_000} {
		dir := t.TempDir()
		lk, err := lake.Open(context.Background(), lake.Config{
			CatalogDSN: filepath.Join(dir, "meta.ducklake"),
			DataPath:   filepath.Join(dir, "data"),
			MaxConns:   conns + 2,
		})
		require.NoError(t, err)
		w, err := lk.NewWriterN(context.Background(), lake.RawTable, conns)
		require.NoError(t, err)
		dur := writeBundles(t, w, stored, bundle, conns)
		_ = w.Close()
		_ = lk.Close()
		t.Logf("%-12d %14.0f", bundle, float64(len(stored))/dur.Seconds())
	}
}

// TestReplayE2E gets a clean end-to-end HTTP→durable number at a concurrency low
// enough to avoid the embedded-NATS pull-consumer wedge that stalls
// TestReplayHTTP at conc>=256. Hang-proof: it polls rowCount to a deadline and
// reports, never blocking on sink shutdown.
func TestReplayE2E(t *testing.T) {
	if !*runReplay {
		t.Skip("pass -replay")
	}
	events := loadEvents(t, *replayN)
	payloads := make([][]byte, len(events))
	for i, e := range events {
		payloads[i] = statusPayload(e)
	}
	for _, conc := range []int{16, 32, 64} {
		p := newPipeline(t, 4, 2)
		start := time.Now()
		rps, failed := replayHTTP(t, p.url, payloads, conc)
		// Poll durable rows to a deadline; do not wait on sink shutdown.
		deadline := time.After(60 * time.Second)
		tick := time.NewTicker(200 * time.Millisecond)
		var rows int
		for rows < len(payloads) {
			rows = p.rowCount(t)
			select {
			case <-tick.C:
			case <-deadline:
				goto report
			}
		}
	report:
		tick.Stop()
		eps := float64(rows) / time.Since(start).Seconds()
		t.Logf("e2e conc=%-4d p=4: acked %.0f req/s, durable %d/%d rows, %.0f ev/s e2e, %d failed",
			conc, rps, rows, len(payloads), eps, failed)
		p.cancel()
		p.httpSrv.Close()
		_ = p.lk.Close()
		p.connCl()
		p.natsSrv()
	}
}

// --- decode micro-opt: the old regex baseline vs the production hand-rolled scan ---

// validCharsRE is the regexp ValidIdentifier used before the hand-rolled scan; kept
// here as the benchmark's "before" baseline (it no longer exists in convert). The
// "after" side benchmarks the real convert.ValidIdentifier directly — no test copy,
// so it can't silently drift from production.
var validCharsRE = regexp.MustCompile(`^[a-zA-Z0-9\-_/,. :]+$`)

func validIdentRE(s string) bool { return validCharsRE.MatchString(s) }

// identStrings returns the header fields ValidIdentifier actually checks (id,
// subject, producer, type, specversion, dataversion) from real rows.
func identStrings(tb testing.TB) []string {
	evs := loadEvents(tb, 20_000)
	out := make([]string, 0, len(evs)*4)
	for _, e := range evs {
		out = append(out, e.id, e.subject, e.producer, e.typ, "1.0", e.dver)
	}
	return out
}

func BenchmarkValidIdentifier(b *testing.B) {
	if !*runReplay {
		b.Skip("pass -replay")
	}
	strs := identStrings(b)
	b.Run("regexp", func(b *testing.B) {
		var ok bool
		for i := 0; i < b.N; i++ {
			ok = validIdentRE(strs[i%len(strs)])
		}
		_ = ok
	})
	b.Run("production", func(b *testing.B) {
		var ok bool
		for i := 0; i < b.N; i++ {
			ok = convert.ValidIdentifier(strs[i%len(strs)])
		}
		_ = ok
	})
}
