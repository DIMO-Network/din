// parquet_replay_test.go is an ingestion benchmark that replays the real
// production parquet dump (~/prod-dump-parquet) through din and measures
// throughput in events/sec, the gate being 100k/s. It has three benches,
// each gated by -replay so a normal `go test` skips them:
//
//	TestReplayHTTP    end-to-end HTTP → convert → JetStream-ack → sink → DuckLake,
//	                  swept over WAL partitions and poster concurrency. This is
//	                  "as http calls": the number a device fleet actually sees.
//	TestReplayLake    the pure DuckLake parquet-write ceiling on the REAL dump
//	                  bytes (bypasses HTTP+NATS) — the steady-state floor that
//	                  caps everything downstream of the WAL.
//	TestReplayStages  per-stage attribution: convert-only, per-event sync ack
//	                  (today's handler model), and batched async ack (the
//	                  pipelining headroom a batch endpoint would unlock).
//
// The stored dump is POST-conversion (data carries signals as an object map,
// which the source-specific Ruptela/AutoPi modules produced from raw wire
// formats we no longer have). Replaying it back through the *default* module
// would tag it dimo.unknown and 400. So the HTTP bench rebuilds each event as a
// default-module status payload sized to the real row's data bytes and reusing
// the real subject/producer/time — keeping payload size and subject cardinality
// (what actually bottlenecks TLS/convert/NATS/parquet) faithful. The lake bench
// uses the real bytes verbatim.
//
// Run:
//
//	go test ./tests/ -run TestReplayHTTP   -v -replay -timeout 30m
//	go test ./tests/ -run TestReplayLake   -v -replay -timeout 30m
//	go test ./tests/ -run TestReplayStages -v -replay -timeout 30m
package tests

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/fsstore"
	"github.com/DIMO-Network/din/internal/handler"
	"github.com/DIMO-Network/din/internal/lake"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/server"
	"github.com/DIMO-Network/din/internal/sink"
	"github.com/DIMO-Network/din/internal/split"
	"github.com/DIMO-Network/din/internal/stream"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var (
	runReplay  = flag.Bool("replay", false, "run the parquet→din ingestion benchmark")
	replayDump = flag.String("replay-dump", "", "parquet dump dir (default ~/prod-dump-parquet)")
	replayN    = flag.Int("replay-n", 50_000, "events to replay per run")
	replayConc = flag.Int("replay-conc", 256, "default concurrent HTTP posters")
)

const replaySource = "0xc0dec0dec0dec0dec0dec0dec0dec0dec0dec0de"

// loadedEvent is one real row from the dump, enough to rebuild a wire payload.
type loadedEvent struct {
	subject  string
	producer string
	id       string
	typ      string
	dct      string
	dver     string
	t        time.Time
	data     []byte
}

func dumpRoot(tb testing.TB) string {
	tb.Helper()
	if *replayDump != "" {
		return *replayDump
	}
	home, err := os.UserHomeDir()
	require.NoError(tb, err)
	return filepath.Join(home, "prod-dump-parquet")
}

// loadEvents reads up to n real rows from the dump via a bare DuckDB connection.
// It walks the tree for just enough files to cover n (≈128 rows/file) and reads
// them in one read_parquet() call.
func loadEvents(tb testing.TB, n int) []loadedEvent {
	tb.Helper()
	root := dumpRoot(tb)
	if _, err := os.Stat(root); err != nil {
		tb.Skipf("dump dir %s not present: %v", root, err)
	}

	maxFiles := n/100 + 50
	var files []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			files = append(files, p)
			if len(files) >= maxFiles {
				return filepath.SkipAll
			}
		}
		return nil
	})
	require.NoError(tb, err)
	require.NotEmpty(tb, files, "no parquet files under %s", root)

	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = "'" + strings.ReplaceAll(f, "'", "''") + "'"
	}
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(tb, err)
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck

	q := fmt.Sprintf(`SELECT subject, producer, id, type, data_content_type, data_version, "time", data
		FROM read_parquet([%s])
		WHERE data IS NOT NULL AND data_content_type = 'application/json'
		LIMIT %d`, strings.Join(quoted, ","), n)
	rows, err := db.QueryContext(context.Background(), q)
	require.NoError(tb, err)
	defer rows.Close() //nolint:errcheck

	var out []loadedEvent
	for rows.Next() {
		var e loadedEvent
		var data string
		require.NoError(tb, rows.Scan(&e.subject, &e.producer, &e.id, &e.typ, &e.dct, &e.dver, &e.t, &data))
		e.data = []byte(data)
		out = append(out, e)
	}
	require.NoError(tb, rows.Err())
	require.NotEmpty(tb, out, "loaded zero events")
	tb.Logf("loaded %d real events from %d files under %s", len(out), len(files), root)
	return out
}

// statusPayload rebuilds a default-module status wire payload sized to the real
// row's data bytes, reusing the real subject/producer/time for faithful
// cardinality. One signal entry is ~70 bytes; pad to ~targetBytes.
func statusPayload(e loadedEvent) []byte {
	ts := e.t.UTC().Format(time.RFC3339Nano)
	target := len(e.data)
	nSig := target / 70
	if nSig < 1 {
		nSig = 1
	}
	var sb strings.Builder
	sb.Grow(target + 256)
	sb.WriteString(`{"specversion":"1.0","id":`)
	sb.WriteString(strconv.Quote(e.id))
	sb.WriteString(`,"source":"` + replaySource + `","subject":`)
	sb.WriteString(strconv.Quote(e.subject))
	sb.WriteString(`,"producer":`)
	sb.WriteString(strconv.Quote(e.producer))
	sb.WriteString(`,"time":"` + ts + `","dataversion":`)
	sb.WriteString(strconv.Quote(e.dver))
	sb.WriteString(`,"data":{"signals":[`)
	for i := 0; i < nSig; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"name":"speed","timestamp":"`)
		sb.WriteString(ts)
		sb.WriteString(`","value":`)
		sb.WriteString(strconv.Itoa(i % 130))
		sb.WriteByte('}')
	}
	sb.WriteString(`]}}`)
	return []byte(sb.String())
}

// pipeline is a full in-process din: embedded NATS JetStream WAL + DuckLake +
// per-partition sinks + the connection HTTP handler.
type pipeline struct {
	url      string
	lk       *lake.Lake
	js       jetstream.JetStream
	cancel   context.CancelFunc
	sinkDone []chan struct{}
	httpSrv  *httptest.Server
	natsSrv  func()
	connCl   func()

	// for the prototype batch endpoint
	ctx        context.Context
	converter  *convert.Converter
	splitter   handler.Splitter
	partitions int
	batchSrv   *httptest.Server
}

func newPipeline(t *testing.T, partitions, writerConns int) *pipeline {
	t.Helper()
	dir := t.TempDir()
	natsSrv, err := natsembed.Run(natsembed.Config{StoreDir: filepath.Join(dir, "nats"), MaxStore: 1 << 40})
	require.NoError(t, err)
	conn, err := natsembed.Connect(natsSrv)
	require.NoError(t, err)
	js, err := jetstream.New(conn,
		jetstream.WithPublishAsyncMaxPending(4096),
		jetstream.WithPublishAsyncTimeout(10*time.Second))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cfg := stream.DefaultConfig()
	cfg.Partitions = partitions
	rawStreams, err := stream.EnsureStreams(ctx, js, cfg)
	require.NoError(t, err)

	lk, err := lake.Open(ctx, lake.Config{
		CatalogDSN: filepath.Join(dir, "meta.ducklake"),
		DataPath:   filepath.Join(dir, "data"),
		MaxConns:   partitions*writerConns + 2,
	})
	require.NoError(t, err)

	blobStore, err := fsstore.New(filepath.Join(dir, "blobs"))
	require.NoError(t, err)
	ccfg := convert.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}
	converter := convert.NewConverter(zerolog.Nop(), ccfg)
	splitter := split.New(blobStore, "cloudevent/blobs/", 1<<20)
	handlers := &handler.Handlers{
		Converter: converter,
		Splitter:  splitter,
		Publisher: stream.NewPublisher(js, partitions),
		Log:       zerolog.Nop(),
	}
	httpSrv := httptest.NewServer(sourceInjector(replaySource, handlers.Connection()))

	p := &pipeline{
		url: httpSrv.URL, lk: lk, js: js, cancel: cancel,
		httpSrv: httpSrv, natsSrv: natsSrv.Shutdown, connCl: conn.Close,
		ctx: ctx, converter: converter, splitter: splitter, partitions: partitions,
	}
	for i, rawStream := range rawStreams {
		durable := "parquet-sink"
		if len(rawStreams) > 1 {
			durable = fmt.Sprintf("parquet-sink-p%03d", i)
		}
		c, err := rawStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable: durable, AckPolicy: jetstream.AckExplicitPolicy,
			AckWait: 5 * time.Minute, MaxAckPending: 250_000,
		})
		require.NoError(t, err)
		writer, err := lk.NewWriterN(ctx, lake.RawTable, writerConns)
		require.NoError(t, err)
		done := make(chan struct{})
		p.sinkDone = append(p.sinkDone, done)
		go func() {
			defer close(done)
			defer writer.Close() //nolint:errcheck
			_ = sink.New(sink.Config{MaxAge: time.Second, MinFlushBytes: 1}, c, writer, zerolog.Nop()).Run(ctx)
		}()
	}
	return p
}

// rowCount returns the committed row count in the lake.
func (p *pipeline) rowCount(t *testing.T) int {
	t.Helper()
	var rows int
	require.NoError(t, p.lk.DB().QueryRowContext(context.Background(),
		"SELECT count(*) FROM lake.raw_events").Scan(&rows))
	return rows
}

// waitDurable polls until at least `expected` rows are committed (true
// end-to-end durable latency from `start`), then stops ingest. It avoids the
// sink's 5s FetchMaxWait drain tail that a cancel-then-wait would add. Returns
// the committed row count.
func (p *pipeline) waitDurable(t *testing.T, expected int, start time.Time, deadline time.Duration) (int, time.Duration) {
	t.Helper()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	timeout := time.After(deadline)
	// Real prod ids can repeat across files; JetStream MsgId dedup then collapses
	// them, so the committed count can settle just below `expected`. Stop on
	// either reaching expected OR the count going stable (no growth) — never hang
	// to the deadline over an off-by-a-few dedup.
	var rows, prev int
	stable := 0
	for {
		rows = p.rowCount(t)
		if rows >= expected {
			break
		}
		if rows > 0 && rows == prev {
			if stable++; stable >= 8 { // ~2s with no new rows ⇒ drained
				break
			}
		} else {
			// rows==0 is cold start (ingest hasn't committed yet), not "drained":
			// don't let the stable-count fire before the first batch lands.
			stable, prev = 0, rows
		}
		select {
		case <-tick.C:
		case <-timeout:
			t.Logf("waitDurable timed out at %d/%d rows", rows, expected)
			goto done
		}
	}
done:
	dur := time.Since(start)
	p.cancel()
	for _, d := range p.sinkDone {
		<-d
	}
	return p.rowCount(t), dur
}

func (p *pipeline) close() {
	// Cancel ingest and wait for the sinks to finish before tearing down the lake
	// and NATS — otherwise the sink goroutines keep running Run(ctx) against a
	// closed lake/connection (goroutine + DuckDB-conn leak, plus spurious errors).
	// Idempotent: callers that already ran waitDurable (which cancels + joins) see
	// an already-cancelled ctx and already-closed sinkDone channels here.
	p.cancel()
	for _, d := range p.sinkDone {
		<-d
	}
	p.httpSrv.Close()
	if p.batchSrv != nil {
		p.batchSrv.Close()
	}
	_ = p.lk.Close()
	p.connCl()
	p.natsSrv()
}

// batchURL mounts a prototype batch ingest endpoint that accepts a JSON array of
// device cloudevents in one request: convert each, split, publish every event
// async, then await all acks once before returning 200 (at-least-once preserved;
// a 200 still implies every event is JetStream-durable). This is the endpoint
// the report recommends — it amortizes HTTP overhead across the batch while the
// publish/lake stages run at their own (far higher) ceiling.
func (p *pipeline) batchURL(t *testing.T) string {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		source, _ := server.SourceFromContext(r.Context())
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(body, &arr); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		futures := make([]jetstream.PubAckFuture, 0, len(arr))
		for _, raw := range arr {
			events, err := p.converter.Convert(r.Context(), source, raw)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			for i := range events {
				stored, err := p.splitter.MaybeSplit(r.Context(), events[i])
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				b, err := stored.MarshalJSON()
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				msg := &nats.Msg{
					Subject: stream.Subject(&stored.CloudEventHeader, p.partitions),
					Data:    b,
					Header:  nats.Header{nats.MsgIdHdr: []string{stream.MsgID(&stored.CloudEventHeader)}},
				}
				f, err := p.js.PublishMsgAsync(msg)
				if err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				futures = append(futures, f)
			}
		}
		for _, f := range futures {
			select {
			case <-f.Ok():
			case err := <-f.Err():
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			case <-r.Context().Done():
				http.Error(w, "timeout", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	p.batchSrv = httptest.NewServer(sourceInjector(replaySource, h))
	return p.batchSrv.URL
}

// replayHTTPBatch posts payloads in batches of batchSize (JSON array per
// request) with conc keep-alive posters; returns events/s.
func replayHTTPBatch(t *testing.T, url string, payloads [][]byte, batchSize, conc int) (float64, int64) {
	t.Helper()
	// Pre-build the batch bodies once.
	var batches [][]byte
	for i := 0; i < len(payloads); i += batchSize {
		end := min(i+batchSize, len(payloads))
		var b bytes.Buffer
		b.WriteByte('[')
		for j := i; j < end; j++ {
			if j > i {
				b.WriteByte(',')
			}
			b.Write(payloads[j])
		}
		b.WriteByte(']')
		batches = append(batches, append([]byte(nil), b.Bytes()...))
	}
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns: conc, MaxIdleConnsPerHost: conc, MaxConnsPerHost: conc,
	}}
	var okEvents, failed atomic.Int64
	var idx atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := idx.Add(1) - 1
				if int(i) >= len(batches) {
					return
				}
				lo := int(i) * batchSize
				n := min(batchSize, len(payloads)-lo)
				resp, err := client.Post(url, "application/json", bytes.NewReader(batches[i]))
				if err != nil {
					failed.Add(int64(n))
					continue
				}
				if resp.StatusCode != http.StatusOK {
					failed.Add(int64(n))
				} else {
					okEvents.Add(int64(n))
				}
				_, _ = bytes.NewBuffer(nil).ReadFrom(resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	eps := float64(okEvents.Load()) / time.Since(start).Seconds()
	return eps, failed.Load()
}

// TestReplayHTTPBatch proves the headline optimization: a batch endpoint
// (N events per POST) amortizes HTTP overhead, lifting single-node throughput
// from the ~65k/s 1-event-per-POST wall toward the publish/lake ceiling.
func TestReplayHTTPBatch(t *testing.T) {
	if !*runReplay {
		t.Skip("pass -replay to run the ingestion benchmark")
	}
	events := loadEvents(t, *replayN)
	payloads := make([][]byte, len(events))
	for i, e := range events {
		payloads[i] = statusPayload(e)
	}
	t.Logf("%-12s %12s %10s", "batch size", "ev/s", "failed")
	for _, bs := range []int{1, 5, 10, 25, 50, 100, 200} {
		p := newPipeline(t, 4, 2)
		url := p.batchURL(t)
		eps, failed := replayHTTPBatch(t, url, payloads, bs, *replayConc)
		t.Logf("%-12d %12.0f %10d", bs, eps, failed)
		p.cancel()
		for _, d := range p.sinkDone {
			<-d
		}
		p.close()
	}
}

// replayHTTP fires payloads at the pipeline with `conc` keep-alive posters and
// returns accepted req/s (each 200 = a JetStream ack).
func replayHTTP(t *testing.T, url string, payloads [][]byte, conc int) (float64, int64) {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        conc,
		MaxIdleConnsPerHost: conc,
		MaxConnsPerHost:     conc,
	}}
	var posted, failed atomic.Int64
	var idx atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := idx.Add(1) - 1
				if int(i) >= len(payloads) {
					return
				}
				resp, err := client.Post(url, "application/json", bytes.NewReader(payloads[i]))
				if err != nil {
					failed.Add(1)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					failed.Add(1)
				} else {
					posted.Add(1)
				}
				_, _ = bytes.NewBuffer(nil).ReadFrom(resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	rps := float64(posted.Load()) / time.Since(start).Seconds()
	return rps, failed.Load()
}

// TestReplayHTTP measures the full HTTP ingest path, swept over WAL partitions
// and poster concurrency.
func TestReplayHTTP(t *testing.T) {
	if !*runReplay {
		t.Skip("pass -replay to run the ingestion benchmark")
	}
	events := loadEvents(t, *replayN)
	payloads := make([][]byte, len(events))
	var totalBytes int64
	for i, e := range events {
		payloads[i] = statusPayload(e)
		totalBytes += int64(len(payloads[i]))
	}
	t.Logf("payload bytes: avg %d over %d events (%.1f MiB total)",
		totalBytes/int64(len(payloads)), len(payloads), float64(totalBytes)/(1<<20))

	type cfg struct {
		partitions, conc int
	}
	configs := []cfg{
		{1, *replayConc},
		{4, *replayConc},
		{8, *replayConc},
		{4, 64},
		{4, 512},
	}
	// Acked-rate sweep (fast): each config measures HTTP→convert→JetStream-ack
	// throughput. Durable rate is measured once below (it is min(acked, lake);
	// the lake ceiling bench shows lake ≫ acked, so durable tracks acked).
	t.Logf("%-12s %-6s %12s %10s", "partitions", "conc", "acked ev/s", "failed")
	for _, c := range configs {
		p := newPipeline(t, c.partitions, 2)
		rps, failed := replayHTTP(t, p.url, payloads, c.conc)
		t.Logf("%-12d %-6d %12.0f %10d", c.partitions, c.conc, rps, failed)
		p.close()
	}

	// One end-to-end durable confirmation at the best-acked config: posts, then
	// polls until every row is committed to the lake (true durable latency).
	p := newPipeline(t, 4, 2)
	start := time.Now()
	rps, _ := replayHTTP(t, p.url, payloads, *replayConc)
	rows, dur := p.waitDurable(t, len(payloads), start, 3*time.Minute)
	t.Logf("durable confirm (p=4,c=%d): %d rows in %s → %.0f ev/s end-to-end (acked %.0f)",
		*replayConc, rows, dur.Round(time.Millisecond), float64(rows)/dur.Seconds(), rps)
	p.close()
}

// TestReplayLake measures the pure DuckLake parquet-write ceiling on the REAL
// dump bytes: WriteBundle straight to the lake, no HTTP, no NATS. This is the
// steady-state floor — the sink can never commit faster than this.
func TestReplayLake(t *testing.T) {
	if !*runReplay {
		t.Skip("pass -replay to run the ingestion benchmark")
	}
	events := loadEvents(t, *replayN)
	stored := storedFromDump(events)

	const bundleSize = 100_000 // the sink's MaxRowsPerFlush
	for _, writerConns := range []int{1, 2, 4, 8} {
		dir := t.TempDir()
		lk, err := lake.Open(context.Background(), lake.Config{
			CatalogDSN: filepath.Join(dir, "meta.ducklake"),
			DataPath:   filepath.Join(dir, "data"),
			MaxConns:   writerConns + 2,
		})
		require.NoError(t, err)
		writer, err := lk.NewWriterN(context.Background(), lake.RawTable, writerConns)
		require.NoError(t, err)

		// Bundle + commit across writerConns concurrently — exactly how the
		// per-partition sinks drive the writer. writeBundles (replay_opt_test.go)
		// is the shared helper used by the codec/bundle sweeps too.
		dur := writeBundles(t, writer, stored, bundleSize, writerConns)
		nBundles := (len(stored) + bundleSize - 1) / bundleSize
		eps := float64(len(stored)) / dur.Seconds()
		t.Logf("lake write ceiling: %d writer-conns → %.0f events/s (%d events, %d bundles)",
			writerConns, eps, len(stored), nBundles)
		_ = writer.Close()
		_ = lk.Close()
	}
}

// TestReplayStages attributes throughput per stage: convert-only (CPU ceiling),
// per-event synchronous ack (today's publishOne loop), and batched async ack
// (the pipelining a batch endpoint would unlock).
func TestReplayStages(t *testing.T) {
	if !*runReplay {
		t.Skip("pass -replay to run the ingestion benchmark")
	}
	events := loadEvents(t, *replayN)
	payloads := make([][]byte, len(events))
	for i, e := range events {
		payloads[i] = statusPayload(e)
	}
	conc := *replayConc

	// --- Stage 1: convert only (no NATS, no lake). CPU ceiling. ---
	conv := convert.NewConverter(zerolog.Nop(), convert.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT})
	{
		var done atomic.Int64
		var idx atomic.Int64
		var wg sync.WaitGroup
		start := time.Now()
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := idx.Add(1) - 1
					if int(i) >= len(payloads) {
						return
					}
					evs, err := conv.Convert(context.Background(), replaySource, payloads[i])
					if err == nil {
						done.Add(int64(len(evs)))
					}
				}
			}()
		}
		wg.Wait()
		eps := float64(done.Load()) / time.Since(start).Seconds()
		t.Logf("convert-only:        %.0f events/s (conc %d)", eps, conc)
	}

	// Build StoredEvents once for the publish benches.
	stored := make([]*cloudevent.StoredEvent, len(events))
	for i, e := range events {
		stored[i] = &cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: "1.0", ID: e.id, Source: replaySource,
				Subject: e.subject, Producer: e.producer, Time: e.t,
				Type: cloudevent.TypeStatus, DataContentType: "application/json", DataVersion: e.dver,
			},
			Data: e.data,
		}}
	}

	// The publish benches are fsync-bound; cap their event count so wall time
	// stays bounded regardless of -replay-n.
	pubN := min(len(stored), 40_000)
	pub := stored[:pubN]
	for _, partitions := range []int{1, 4} {
		// --- Stage 2: per-event synchronous ack (the handler's publishOne). ---
		epsSync := publishBench(t, partitions, conc, pub, false)
		// --- Stage 3: batched async ack (fire all, await once). ---
		epsBatch := publishBench(t, partitions, conc, pub, true)
		t.Logf("publish p=%d (n=%d):  per-event-ack %.0f ev/s | batched-async-ack %.0f ev/s (%.1fx)",
			partitions, pubN, epsSync, epsBatch, epsBatch/epsSync)
	}
}

// publishBench publishes stored events to a fresh embedded JetStream and returns
// events/s. batched=false awaits each ack (the handler model); batched=true
// fires all async then awaits PublishAsyncComplete once.
func publishBench(t *testing.T, partitions, conc int, stored []*cloudevent.StoredEvent, batched bool) float64 {
	t.Helper()
	dir := t.TempDir()
	srv, err := natsembed.Run(natsembed.Config{StoreDir: filepath.Join(dir, "nats"), MaxStore: 1 << 40})
	require.NoError(t, err)
	defer srv.Shutdown()
	conn, err := natsembed.Connect(srv)
	require.NoError(t, err)
	defer conn.Close()
	js, err := jetstream.New(conn,
		jetstream.WithPublishAsyncMaxPending(4096),
		jetstream.WithPublishAsyncTimeout(30*time.Second))
	require.NoError(t, err)
	ctx := context.Background()
	cfg := stream.DefaultConfig()
	cfg.Partitions = partitions
	_, err = stream.EnsureStreams(ctx, js, cfg)
	require.NoError(t, err)

	pub := stream.NewPublisher(js, partitions)
	start := time.Now()
	if batched {
		// Fire every publish async (bounded by WithPublishAsyncMaxPending), then
		// await all acks once — the pipelined model.
		for _, e := range stored {
			body, _ := e.MarshalJSON()
			msg := &nats.Msg{
				Subject: stream.Subject(&e.CloudEventHeader, partitions),
				Data:    body,
				Header:  nats.Header{nats.MsgIdHdr: []string{stream.MsgID(&e.CloudEventHeader)}},
			}
			for {
				if _, err := js.PublishMsgAsync(msg); err == nil {
					break
				}
				// pending full — drain a little
				select {
				case <-js.PublishAsyncComplete():
				case <-time.After(time.Millisecond):
				}
			}
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(60 * time.Second):
			t.Fatal("batched publish did not complete")
		}
	} else {
		// Per-event synchronous ack across conc workers (the publishOne loop, but
		// parallelized to be generous to the current model).
		var idx atomic.Int64
		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := idx.Add(1) - 1
					if int(i) >= len(stored) {
						return
					}
					_ = pub.Publish(ctx, stored[i])
				}
			}()
		}
		wg.Wait()
	}
	return float64(len(stored)) / time.Since(start).Seconds()
}
