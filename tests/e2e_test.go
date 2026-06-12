// Package tests exercises the full ingest path in-process: HTTP handler →
// convert → split → JetStream → parquet sink → bundle on the object store,
// plus the decodestream bridge feeding triggers-compatible subjects.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/decodestream"
	"github.com/DIMO-Network/din/internal/fsstore"
	"github.com/DIMO-Network/din/internal/handler"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/objstore"
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

// e2eStore wraps a production object store (fsstore here, s3client in the
// MinIO suite) with the listing/fetch helpers the e2e assertions use, so the
// e2e proves the sink → store → parquet decode round trip on a real backend.
type e2eStore struct {
	objstore.Store
}

func newE2EStore(t *testing.T) e2eStore {
	t.Helper()
	c, err := fsstore.New(t.TempDir())
	require.NoError(t, err)
	return e2eStore{Store: c}
}

func (s e2eStore) keys(prefix string) []string {
	objects, err := s.ListObjectsV2(context.Background(), prefix)
	if err != nil {
		return nil
	}
	out := make([]string, len(objects))
	for i, obj := range objects {
		out[i] = obj.Key
	}
	return out
}

func (s e2eStore) get(key string) []byte {
	body, err := s.GetObject(context.Background(), key, 0)
	if err != nil {
		return nil
	}
	return body
}

// sourceInjector simulates the mTLS middleware: every request carries the
// connection license address the cert CN would provide.
func sourceInjector(source string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(server.WithSource(r.Context(), source)))
	})
}

func TestEndToEnd_DeviceToParquetAndTriggers(t *testing.T) {
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

	rawStream, err := stream.EnsureStream(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	store := newE2EStore(t)
	cfg := convert.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}
	convert.RegisterModules(cfg)

	handlers := &handler.Handlers{
		Converter: convert.NewConverter(zerolog.Nop(), cfg),
		Splitter:  split.New(store, "cloudevent/blobs/", 1<<20),
		Publisher: stream.NewPublisher(js),
		Log:       zerolog.Nop(),
	}
	httpSrv := httptest.NewServer(sourceInjector("0xConnLicense", handlers.Connection()))
	t.Cleanup(httpSrv.Close)

	// Sink.
	sinkConsumer, err := rawStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "parquet-sink", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 5 * time.Minute,
	})
	require.NoError(t, err)
	go func() {
		_ = sink.New(sink.Config{MaxAge: 200 * time.Millisecond}, sinkConsumer, store, zerolog.Nop()).Run(ctx)
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

	// Raw parquet bundle lands in the right hive partition.
	wantPrefix := "raw/type=dimo.status/date=" + ts.Format("2006-01-02") + "/"
	require.Eventually(t, func() bool { return len(store.keys(wantPrefix)) == 1 }, 10*time.Second, 100*time.Millisecond)

	bundleKey := store.keys(wantPrefix)[0]
	body := store.get(bundleKey)
	events, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, subject, events[0].Subject)
	assert.Equal(t, "device-msg-1", events[0].ID)
	assert.JSONEq(t, `{"signals":[{"name":"speed","timestamp":"`+ts.Format(time.RFC3339Nano)+`","value":72.5}]}`,
		string(events[0].Data), "raw payload stored verbatim — parse-on-read source of truth")

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
