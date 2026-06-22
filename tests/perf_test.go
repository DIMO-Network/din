// perf_test.go measures din's write path on local storage: the fsstore
// publish cost (fsync dominates small objects) and the full ingest
// pipeline — HTTP POST → convert → JetStream ack → batched sink → durable
// DuckLake commit — in events per second. Local NVMe and a file catalog
// stand in for S3 and Postgres, so these numbers are pipeline cost
// without network.
//
// Run: go test ./tests/ -run TestIngestPerformance -v -perf
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	"github.com/DIMO-Network/din/internal/sink"
	"github.com/DIMO-Network/din/internal/split"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var runPerf = flag.Bool("perf", false, "run the ingest performance gate")

// TestIngestPerformance_FSStorePublish measures the raw fsstore publish path
// (temp + write + fsync + rename) across bundle sizes.
func TestIngestPerformance_FSStorePublish(t *testing.T) {
	if !*runPerf {
		t.Skip("pass -perf to run the performance gate")
	}
	store, err := fsstore.New(t.TempDir())
	require.NoError(t, err)
	ctx := context.Background()

	for _, size := range []int{4 << 10, 1 << 20, 16 << 20, 128 << 20} {
		body := bytes.Repeat([]byte("x"), size)
		runs := 50
		if size >= 16<<20 {
			runs = 10
		}
		start := time.Now()
		for i := range runs {
			key := fmt.Sprintf("bench/size=%d/obj-%04d.bin", size, i)
			require.NoError(t, store.PutObject(ctx, key, body))
		}
		elapsed := time.Since(start)
		perOp := elapsed / time.Duration(runs)
		mbps := float64(size) * float64(runs) / elapsed.Seconds() / (1 << 20)
		t.Logf("fsstore publish %8s x%3d: %s/op, %.0f MiB/s", byteSize(size), runs, perOp.Round(time.Microsecond), mbps)
	}
}

// TestIngestPerformance runs the full in-process pipeline under concurrent
// device POSTs and reports accepted requests/sec (each 200 implies a
// JetStream ack) plus end-to-end events/sec to durable parquet.
func TestIngestPerformance(t *testing.T) {
	if !*runPerf {
		t.Skip("pass -perf to run the performance gate")
	}
	const (
		totalEvents = 20_000
		concurrency = 32
		vehicles    = 100
	)

	srv, err := natsembed.Run(natsembed.Config{StoreDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(srv.Shutdown)
	conn, err := natsembed.Connect(srv)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rawStreams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	lk := openTestLake(t)
	blobStore, err := fsstore.New(t.TempDir())
	require.NoError(t, err)
	cfg := convert.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}

	handlers := &handler.Handlers{
		Converter: convert.NewConverter(zerolog.Nop(), cfg),
		Splitter:  split.New(blobStore, "cloudevent/blobs/", 1<<20),
		Publisher: stream.NewPublisher(js, 1),
		Log:       zerolog.Nop(),
	}
	httpSrv := httptest.NewServer(sourceInjector("0xConnLicense", handlers.Connection()))
	t.Cleanup(httpSrv.Close)

	sinkConsumer, err := rawStreams[0].CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "parquet-sink", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 5 * time.Minute, MaxAckPending: 250_000,
	})
	require.NoError(t, err)
	writer, err := lk.NewWriter(ctx, lake.RawTable)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	sinkDone := make(chan struct{})
	go func() {
		defer close(sinkDone)
		_ = sink.New(sink.Config{MaxAge: time.Second, MinFlushBytes: 1}, sinkConsumer, writer, zerolog.Nop()).Run(ctx)
	}()

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: concurrency}}
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	var posted, failed atomic.Int64
	var wg sync.WaitGroup
	postStart := time.Now()
	for w := range concurrency {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < totalEvents; i += concurrency {
				subject := fmt.Sprintf("did:erc721:137:%s:%d", vehicleNFT.Hex(), i%vehicles)
				ts := base.Add(time.Duration(i) * time.Millisecond)
				payload, _ := json.Marshal(map[string]any{
					"type":        cloudevent.TypeStatus,
					"subject":     subject,
					"source":      "0xConnLicense",
					"producer":    subject,
					"id":          fmt.Sprintf("perf-%d", i),
					"specversion": cloudevent.SpecVersion,
					"time":        ts.Format(time.RFC3339Nano),
					"dataversion": "default/v1.0",
					"data": map[string]any{"signals": []map[string]any{
						{"name": "speed", "timestamp": ts.Format(time.RFC3339Nano), "value": float64(i % 130)},
					}},
				})
				resp, err := client.Post(httpSrv.URL, "application/json", bytes.NewReader(payload))
				if err != nil || resp.StatusCode != http.StatusOK {
					failed.Add(1)
					if resp != nil {
						_ = resp.Body.Close()
					}
					continue
				}
				_ = resp.Body.Close()
				posted.Add(1)
			}
		}(w)
	}
	wg.Wait()
	postDur := time.Since(postStart)
	require.Zero(t, failed.Load(), "no failed posts")
	rps := float64(posted.Load()) / postDur.Seconds()
	t.Logf("ingest: %d events POSTed (200 = JetStream-acked) in %s — %.0f req/s at concurrency %d",
		posted.Load(), postDur.Round(time.Millisecond), rps, concurrency)

	// Drain: cancel ctx so the sink flushes everything buffered, then count
	// rows committed to the lake.
	cancel()
	<-sinkDone
	durableDur := time.Since(postStart)

	var rows, snapshots int
	require.NoError(t, lk.DB().QueryRowContext(context.Background(),
		"SELECT count(*) FROM lake.raw_events WHERE type = 'dimo.status'").Scan(&rows))
	require.NoError(t, lk.DB().QueryRowContext(context.Background(),
		"SELECT count(*) FROM lake.snapshots()").Scan(&snapshots))
	require.GreaterOrEqual(t, rows, totalEvents, "every acked event is durable (redelivery may duplicate, never lose)")
	eps := float64(totalEvents) / durableDur.Seconds()
	t.Logf("durable: %d rows across %d snapshots %s after first POST — %.0f events/s end-to-end",
		rows, snapshots, durableDur.Round(time.Millisecond), eps)

	require.Greater(t, rps, 500.0, "perf gate: ingest must sustain >500 acked req/s")
	require.Greater(t, eps, 500.0, "perf gate: end-to-end durable throughput must exceed 500 events/s")
}

func byteSize(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	default:
		return fmt.Sprintf("%dKiB", n>>10)
	}
}
