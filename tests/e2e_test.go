// Package tests exercises the full ingest path in-process: HTTP handler →
// convert → split → JetStream → sink → DuckLake raw_events table, plus the
// decodestream bridge feeding triggers-compatible subjects.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/decodestream"
	"github.com/DIMO-Network/din/internal/fsstore"
	"github.com/DIMO-Network/din/internal/handler"
	"github.com/DIMO-Network/din/internal/lake"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/server"
	"github.com/DIMO-Network/din/internal/sink"
	"github.com/DIMO-Network/din/internal/split"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var vehicleNFT = common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF")

// openTestLake opens a Lake on a local file catalog — the same code path
// production uses, minus Postgres and S3.
func openTestLake(t *testing.T) *lake.Lake {
	t.Helper()
	dir := t.TempDir()
	l, err := lake.Open(context.Background(), lake.Config{
		CatalogDSN: filepath.Join(dir, "meta.ducklake"),
		DataPath:   filepath.Join(dir, "data"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// sourceInjector simulates the mTLS middleware: every request carries the
// connection license address the cert CN would provide.
func sourceInjector(source string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(server.WithSource(r.Context(), source)))
	})
}

func TestEndToEnd_DeviceToDuckLakeAndTriggers(t *testing.T) {
	srv, err := natsembed.Run(natsembed.Config{StoreDir: t.TempDir(), MaxStore: 1 << 40})
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
	convert.RegisterModules(cfg)

	handlers := &handler.Handlers{
		Converter: convert.NewConverter(zerolog.Nop(), cfg),
		Splitter:  split.New(blobStore, "cloudevent/blobs/", 1<<20),
		Publisher: stream.NewPublisher(js, 1),
		Log:       zerolog.Nop(),
	}
	httpSrv := httptest.NewServer(sourceInjector("0xConnLicense", handlers.Connection()))
	t.Cleanup(httpSrv.Close)

	// Sink.
	sinkConsumer, err := rawStreams[0].CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "parquet-sink", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 5 * time.Minute,
	})
	require.NoError(t, err)
	writer, err := lk.NewWriter(ctx, lake.RawTable)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	go func() {
		// MinFlushBytes: 1 so the soft-age trigger fires on these tiny buffers; the
		// default 16 MiB floor would hold them until the 5m hard cap (past Eventually).
		_ = sink.New(sink.Config{MaxAge: 200 * time.Millisecond, MinFlushBytes: 1}, sinkConsumer, writer, zerolog.Nop()).Run(ctx)
	}()

	// Decodestream bridge.
	bridge := decodestream.New(decodestream.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, js, zerolog.Nop())
	require.NoError(t, bridge.EnsureStreams(ctx))
	go func() { _ = bridge.Run(ctx) }()

	// POST a default-module status payload as a device would.
	subject := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	payload, _ := json.Marshal(map[string]any{
		"type":        cloudevent.TypeStatus,
		"subject":     subject,
		"source":      "0xConnLicense",
		"producer":    subject,
		"id":          "device-msg-1",
		"specversion": cloudevent.SpecVersion,
		"time":        ts.Format(time.RFC3339Nano),
		"dataversion": "default/v1.0",
		"data": map[string]any{
			"signals": []map[string]any{
				{"name": "speed", "timestamp": ts.Format(time.RFC3339Nano), "value": 72.5},
			},
		},
	})

	resp, err := http.Post(httpSrv.URL, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "device gets 200 only after JetStream ack")

	// The row lands in the lake, findable by the partition columns.
	query := `SELECT subject, id, data FROM lake.raw_events
		WHERE type = 'dimo.status' AND "time"::DATE = ?::DATE`
	require.Eventually(t, func() bool {
		var n int
		err := lk.DB().QueryRowContext(ctx,
			`SELECT count(*) FROM lake.raw_events`).Scan(&n)
		return err == nil && n == 1
	}, 10*time.Second, 100*time.Millisecond)

	var gotSubject, gotID, gotData string
	require.NoError(t, lk.DB().QueryRowContext(ctx, query, ts.Format("2006-01-02")).
		Scan(&gotSubject, &gotID, &gotData))
	assert.Equal(t, subject, gotSubject)
	assert.Equal(t, "device-msg-1", gotID)
	assert.JSONEq(t, `{"signals":[{"name":"speed","timestamp":"`+ts.Format(time.RFC3339Nano)+`","value":72.5}]}`,
		gotData, "raw payload stored verbatim — parse-on-read source of truth")

	// Decoded signal reaches the triggers-compatible subject.
	sigStream, err := js.Stream(ctx, decodestream.SignalsStreamName)
	require.NoError(t, err)
	cons, err := sigStream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "dimo.signals.speed",
	})
	require.NoError(t, err)
	msg, err := cons.Next(jetstream.FetchMaxWait(10 * time.Second))
	require.NoError(t, err)

	var signalCE vss.SignalCloudEvent
	require.NoError(t, json.Unmarshal(msg.Data(), &signalCE))
	signals := vss.UnpackSignals(signalCE)
	require.Len(t, signals, 1)
	assert.Equal(t, "speed", signals[0].Data.Name)
	assert.Equal(t, 72.5, signals[0].Data.ValueNumber)
	assert.Equal(t, subject, signalCE.Subject)
}
