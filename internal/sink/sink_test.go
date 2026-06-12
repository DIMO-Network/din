package sink_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/sink"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) PutObject(_ context.Context, key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), body...)
	return nil
}

func (m *memStore) snapshot() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]byte, len(m.objects))
	for k, v := range m.objects {
		out[k] = v
	}
	return out
}

func setup(t *testing.T) (jetstream.JetStream, jetstream.Consumer) {
	t.Helper()
	srv, err := natsembed.Run(natsembed.Config{StoreDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(srv.Shutdown)
	conn, err := natsembed.Connect(srv)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	require.NoError(t, err)

	ctx := context.Background()
	streams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)
	cons, err := streams[0].CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "parquet-sink",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       5 * time.Minute,
		MaxAckPending: 250_000,
	})
	require.NoError(t, err)
	return js, cons
}

func event(id, ceType, subject string, ts time.Time) *cloudevent.StoredEvent {
	return &cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        ceType,
				Subject:     subject,
				Source:      "0xConn",
				Producer:    "0xDev",
				ID:          id,
				Time:        ts,
			},
			Data: json.RawMessage(`{"v":1}`),
		},
	}
}

func TestSink_WritesPartitionedBundlesAndAcks(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	store := newMemStore()
	ctx, cancel := context.WithCancel(context.Background())

	pub := stream.NewPublisher(js, 1)
	day1 := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	require.NoError(t, pub.Publish(ctx, event("e1", "dimo.status", "did:erc721:137:0xA:1", day1)))
	require.NoError(t, pub.Publish(ctx, event("e2", "dimo.status", "did:erc721:137:0xA:1", day2)))
	require.NoError(t, pub.Publish(ctx, event("e3", "dimo.fingerprint", "did:erc721:137:0xB:2", day2)))

	s := sink.New(sink.Config{MaxAge: 200 * time.Millisecond}, cons, store, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	require.Eventually(t, func() bool { return len(store.snapshot()) >= 3 }, 10*time.Second, 50*time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	objects := store.snapshot()
	require.Len(t, objects, 3, "one bundle per (type,date) partition")

	var prefixes []string
	for key, body := range objects {
		prefixes = append(prefixes, key)
		events, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
		require.NoError(t, err)
		require.NotEmpty(t, events)
		assert.True(t, strings.HasSuffix(key, ".parquet"))
	}
	joined := strings.Join(prefixes, "\n")
	assert.Contains(t, joined, "raw/type=dimo.status/date=2026-06-08/ingest-")
	assert.Contains(t, joined, "raw/type=dimo.status/date=2026-06-09/ingest-")
	assert.Contains(t, joined, "raw/type=dimo.fingerprint/date=2026-06-09/ingest-")
}

func TestSink_RowCountTriggerFlushesEarly(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	store := newMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	for i := range 10 {
		ev := event("evt-"+string(rune('a'+i)), "dimo.status", "did:erc721:137:0xA:1", ts.Add(time.Duration(i)*time.Second))
		require.NoError(t, pub.Publish(ctx, ev))
	}

	// MaxAge long; only the row trigger can flush.
	s := sink.New(sink.Config{MaxRowsPerFlush: 10, MaxAge: time.Hour}, cons, store, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	require.Eventually(t, func() bool { return len(store.snapshot()) == 1 }, 10*time.Second, 50*time.Millisecond)
	for _, body := range store.snapshot() {
		events, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
		require.NoError(t, err)
		assert.Len(t, events, 10)
	}
	cancel()
	require.NoError(t, <-done)
}

func TestSink_ShutdownFlushesBuffered(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	store := newMemStore()
	ctx, cancel := context.WithCancel(context.Background())

	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	require.NoError(t, pub.Publish(ctx, event("e-shutdown", "dimo.status", "did:erc721:137:0xA:1", ts)))

	// Triggers far away: only shutdown can flush.
	s := sink.New(sink.Config{MaxAge: time.Hour, MaxRowsPerFlush: 1 << 20}, cons, store, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Give the fetch loop time to deliver the message into the buffer.
	time.Sleep(2 * time.Second)
	cancel()
	require.NoError(t, <-done)

	require.Len(t, store.snapshot(), 1, "shutdown must flush buffered events")
}
