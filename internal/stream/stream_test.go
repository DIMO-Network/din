package stream_test

import (
	"context"
	"encoding/json"
	"fmt"
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

func storedEvent(id, ceType, subject string) *cloudevent.StoredEvent {
	return &cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
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
		},
	}
}

func TestEnsureStream_CreatesAndIsIdempotent(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	streams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)
	require.Len(t, streams, 1)
	info, err := streams[0].Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, stream.StreamName, info.Config.Name)
	assert.Equal(t, []string{stream.SubjectWildcard}, info.Config.Subjects)
	assert.Equal(t, 48*time.Hour, info.Config.MaxAge)
	assert.Equal(t, 2*time.Minute, info.Config.Duplicates)

	_, err = stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err, "EnsureStream must be idempotent")
}

func TestPublisher_PublishAndConsume(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	streams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	pub := stream.NewPublisher(js, 1)
	ev := storedEvent("evt-1", "dimo.status", "did:erc721:137:0xA:1")
	require.NoError(t, pub.Publish(ctx, ev))

	cons, err := streams[0].CreateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
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

	streams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	pub := stream.NewPublisher(js, 1)
	ev := storedEvent("evt-dup", "dimo.status", "did:erc721:137:0xA:1")
	require.NoError(t, pub.Publish(ctx, ev))
	require.NoError(t, pub.Publish(ctx, ev), "duplicate publish succeeds (idempotent ack)")

	info, err := streams[0].Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.State.Msgs, "duplicate window collapses retries to one stored message")
}

func TestPublisher_VoidsIDHeader(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	streams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	pub := stream.NewPublisher(js, 1)
	ev := storedEvent("evt-tomb", "dimo.tombstone", "did:erc721:137:0xA:1")
	ev.VoidsID = "voided-event-id"
	ev.DataIndexKey = "blobs/did:erc721:137:0xA:1/2026/06/09/abc"
	require.NoError(t, pub.Publish(ctx, ev))

	cons, err := streams[0].CreateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	require.NoError(t, err)
	msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, "voided-event-id", msg.Headers().Get(stream.HeaderVoidsID))

	// ParseMsg reconstructs the StoredEvent, including storage metadata that
	// the RawEvent body cannot carry.
	got, err := stream.ParseMsg(msg.Headers(), msg.Data())
	require.NoError(t, err)
	assert.Equal(t, ev.Key(), got.Key())
	assert.Equal(t, "voided-event-id", got.VoidsID)
	assert.Equal(t, "blobs/did:erc721:137:0xA:1/2026/06/09/abc", got.DataIndexKey)
}

func TestPartitionedStreams_RouteBySubjectHash(t *testing.T) {
	t.Parallel()
	js := newJetStream(t)
	ctx := context.Background()

	cfg := stream.DefaultConfig()
	cfg.Partitions = 4
	streams, err := stream.EnsureStreams(ctx, js, cfg)
	require.NoError(t, err)
	require.Len(t, streams, 4)

	pub := stream.NewPublisher(js, 4)
	subjects := []string{
		"did:erc721:137:0xA:1", "did:erc721:137:0xA:2",
		"did:erc721:137:0xA:3", "did:erc721:137:0xA:4",
		"did:erc721:137:0xA:5", "did:erc721:137:0xA:6",
	}
	for i, subj := range subjects {
		ev := storedEvent(fmt.Sprintf("evt-%d", i), "dimo.status", subj)
		require.NoError(t, pub.Publish(ctx, ev))
		// Same subject re-published lands in the same partition (stickiness
		// is what preserves per-vehicle ordering).
		ev2 := storedEvent(fmt.Sprintf("evt-%d-b", i), "dimo.status", subj)
		require.NoError(t, pub.Publish(ctx, ev2))
	}

	total := 0
	for i, s := range streams {
		info, err := s.Info(ctx)
		require.NoError(t, err)
		total += int(info.State.Msgs)
		want := 0
		for _, subj := range subjects {
			if stream.Partition(subj, 4) == i {
				want += 2
			}
		}
		assert.Equal(t, want, int(info.State.Msgs), "partition %d message count", i)
	}
	assert.Equal(t, len(subjects)*2, total, "no message lost or double-routed")

	// Type filter matches partitioned subjects (partition token is last).
	cons, err := streams[stream.Partition(subjects[0], 4)].CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{stream.SubjectFilterForType("dimo.status")},
	})
	require.NoError(t, err)
	msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	require.NoError(t, err)
	require.NoError(t, msg.Ack())
}
