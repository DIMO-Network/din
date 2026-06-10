package stream_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newJetStream boots an embedded server and returns a JetStream context.
func newJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	srv, err := natsembed.Run(natsembed.Config{StoreDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(srv.Shutdown)

	conn, err := natsembed.Connect(srv)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	js, err := jetstream.New(conn)
	require.NoError(t, err)
	return js
}

func rawEvent(id, ceType, subject string) *cloudevent.RawEvent {
	return &cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        ceType,
			Subject:     subject,
			Source:      "0xConnLicense",
			Producer:    "0xDevice",
			ID:          id,
			Time:        time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		},
		Data: json.RawMessage(`{"speed":42}`),
	}
}

func TestEnsureStream_CreatesAndIsIdempotent(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	s, err := stream.EnsureStream(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)
	info, err := s.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, stream.StreamName, info.Config.Name)
	assert.Equal(t, []string{stream.SubjectWildcard}, info.Config.Subjects)
	assert.Equal(t, 48*time.Hour, info.Config.MaxAge)
	assert.Equal(t, 2*time.Minute, info.Config.Duplicates)

	_, err = stream.EnsureStream(ctx, js, stream.DefaultConfig())
	require.NoError(t, err, "EnsureStream must be idempotent")
}

func TestPublisher_PublishAndConsume(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	s, err := stream.EnsureStream(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	pub := stream.NewPublisher(js)
	ev := rawEvent("evt-1", "dimo.status", "did:erc721:137:0xA:1")
	require.NoError(t, pub.Publish(ctx, ev, ""))

	cons, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)

	assert.Equal(t, "in.raw.dimo_status.did:erc721:137:0xA:1", msg.Subject())
	assert.Equal(t, "dimo.status", msg.Headers().Get(stream.HeaderCEType))
	assert.Equal(t, "evt-1", msg.Headers().Get(stream.HeaderCEID))

	var got cloudevent.RawEvent
	require.NoError(t, json.Unmarshal(msg.Data(), &got))
	assert.Equal(t, ev.Key(), got.Key(), "event roundtrips intact")
	require.NoError(t, msg.Ack())
}

func TestPublisher_DuplicateMsgIDCollapses(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	s, err := stream.EnsureStream(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	pub := stream.NewPublisher(js)
	ev := rawEvent("evt-dup", "dimo.status", "did:erc721:137:0xA:1")
	require.NoError(t, pub.Publish(ctx, ev, ""))
	require.NoError(t, pub.Publish(ctx, ev, ""), "duplicate publish succeeds (idempotent ack)")

	info, err := s.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.State.Msgs, "duplicate window collapses retries to one stored message")
}

func TestPublisher_VoidsIDHeader(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	s, err := stream.EnsureStream(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	pub := stream.NewPublisher(js)
	ev := rawEvent("evt-tomb", "dimo.tombstone", "did:erc721:137:0xA:1")
	require.NoError(t, pub.Publish(ctx, ev, "voided-event-id"))

	cons, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, "voided-event-id", msg.Headers().Get(stream.HeaderVoidsID))
}
