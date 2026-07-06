package sink_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/lake"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/sink"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memWriter records committed bundles; failures are injectable to test
// the no-ack-on-error path. failID makes WriteBundle reject any bundle that
// contains an event with that ID — a deterministic "poison row" the writer can
// never persist — to exercise per-event quarantine.
type memWriter struct {
	mu      sync.Mutex
	bundles [][]cloudevent.StoredEvent
	fail    error
	failID  string // deterministic poison: rejected with lake.ErrPoisonRow
	// transientID models an infra blip on one row: a NON-poison error, so the
	// sink must leave it for redelivery (never terminate it), even when siblings
	// in the same bundle commit.
	transientID string
}

func (m *memWriter) WriteBundle(_ context.Context, events []cloudevent.StoredEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	for _, ev := range events {
		if m.failID != "" && ev.ID == m.failID {
			return fmt.Errorf("poison row %s: %w", ev.ID, lake.ErrPoisonRow)
		}
		if m.transientID != "" && ev.ID == m.transientID {
			return fmt.Errorf("transient outage on %s", ev.ID) // NOT ErrPoisonRow
		}
	}
	m.bundles = append(m.bundles, append([]cloudevent.StoredEvent(nil), events...))
	return nil
}

func (m *memWriter) setFail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = err
}

func (m *memWriter) snapshot() [][]cloudevent.StoredEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]cloudevent.StoredEvent, len(m.bundles))
	copy(out, m.bundles)
	return out
}

func (m *memWriter) totalEvents() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, b := range m.bundles {
		n += len(b)
	}
	return n
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

func TestSink_CommitsBundleAndAcks(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	writer := &memWriter{}
	ctx, cancel := context.WithCancel(context.Background())

	pub := stream.NewPublisher(js, 1)
	day1 := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	require.NoError(t, pub.Publish(ctx, event("e1", "dimo.status", "did:erc721:137:0xA:1", day1)))
	require.NoError(t, pub.Publish(ctx, event("e2", "dimo.status", "did:erc721:137:0xA:1", day2)))
	require.NoError(t, pub.Publish(ctx, event("e3", "dimo.fingerprint", "did:erc721:137:0xB:2", day2)))

	s := sink.New(sink.Config{MaxAge: 200 * time.Millisecond, MinFlushBytes: 1}, cons, writer, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	require.Eventually(t, func() bool { return writer.totalEvents() == 3 }, 10*time.Second, 50*time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	// Mixed types and days ride in one bundle — partition splitting is
	// DuckLake's job, not the sink's.
	ids := map[string]bool{}
	for _, bundle := range writer.snapshot() {
		for _, ev := range bundle {
			ids[ev.ID] = true
		}
	}
	assert.Equal(t, map[string]bool{"e1": true, "e2": true, "e3": true}, ids)

	// Everything acked: a fresh consumer fetch returns nothing.
	info, err := cons.Info(context.Background())
	require.NoError(t, err)
	assert.Zero(t, info.NumAckPending, "all messages acked after commit")
	assert.Zero(t, info.NumPending)
}

func TestSink_RowCountTriggerFlushesEarly(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	writer := &memWriter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	for i := range 10 {
		ev := event("evt-"+string(rune('a'+i)), "dimo.status", "did:erc721:137:0xA:1", ts.Add(time.Duration(i)*time.Second))
		require.NoError(t, pub.Publish(ctx, ev))
	}

	// MaxAge long; only the row trigger can flush.
	s := sink.New(sink.Config{MaxRowsPerFlush: 10, MaxAge: time.Hour}, cons, writer, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	require.Eventually(t, func() bool { return len(writer.snapshot()) == 1 }, 10*time.Second, 50*time.Millisecond)
	assert.Len(t, writer.snapshot()[0], 10)
	cancel()
	require.NoError(t, <-done)
}

func TestSink_ShutdownFlushesBuffered(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	writer := &memWriter{}
	ctx, cancel := context.WithCancel(context.Background())

	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	require.NoError(t, pub.Publish(ctx, event("e-shutdown", "dimo.status", "did:erc721:137:0xA:1", ts)))

	// Triggers far away: only shutdown can flush.
	s := sink.New(sink.Config{MaxAge: time.Hour, MaxRowsPerFlush: 1 << 20}, cons, writer, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Give the fetch loop time to deliver the message into the buffer.
	time.Sleep(2 * time.Second)
	cancel()
	require.NoError(t, <-done)

	require.Equal(t, 1, writer.totalEvents(), "shutdown must flush buffered events")
}

func TestSink_FailedCommitLeavesMessagesUnacked(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	writer := &memWriter{}
	writer.setFail(errors.New("catalog down"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	require.NoError(t, pub.Publish(ctx, event("e-fail", "dimo.status", "did:erc721:137:0xA:1", ts)))

	s := sink.New(sink.Config{MaxAge: 100 * time.Millisecond, MinFlushBytes: 1}, cons, writer, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// The flush fails; the message must stay pending (unacked).
	require.Eventually(t, func() bool {
		info, err := cons.Info(context.Background())
		return err == nil && info.NumAckPending == 1
	}, 10*time.Second, 50*time.Millisecond)

	// Writer recovers; redelivery (AckWait) would eventually land it.
	// Don't wait for the 5m AckWait — just confirm nothing was committed.
	assert.Zero(t, writer.totalEvents())
	cancel()
	require.NoError(t, <-done)
}

// TestSink_PoisonRowQuarantined proves one unpersistable row no longer wedges a
// partition: the good events in its bundle still commit (via per-event
// isolation) and the poison row is terminated, so the queue fully drains. On
// the old code this bundle redelivered forever (SR review #1).
func TestSink_PoisonRowQuarantined(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	writer := &memWriter{failID: "poison"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	require.NoError(t, pub.Publish(ctx, event("good1", "dimo.status", "did:erc721:137:0xA:1", ts)))
	require.NoError(t, pub.Publish(ctx, event("poison", "dimo.status", "did:erc721:137:0xA:1", ts)))
	require.NoError(t, pub.Publish(ctx, event("good2", "dimo.status", "did:erc721:137:0xA:1", ts)))

	s := sink.New(sink.Config{MaxAge: 200 * time.Millisecond, MinFlushBytes: 1}, cons, writer, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Two good events commit; the poison row is terminated, so nothing is left
	// pending — the old whole-bundle-redelivers behavior would never drain.
	require.Eventually(t, func() bool {
		info, err := cons.Info(context.Background())
		return err == nil && writer.totalEvents() == 2 &&
			info.NumAckPending == 0 && info.NumPending == 0
	}, 15*time.Second, 100*time.Millisecond)
	cancel()
	require.NoError(t, <-done)

	ids := map[string]bool{}
	for _, b := range writer.snapshot() {
		for _, ev := range b {
			ids[ev.ID] = true
		}
	}
	assert.Equal(t, map[string]bool{"good1": true, "good2": true}, ids,
		"good events commit, poison row excluded")
}

// TestSink_TransientRowAmongCommittedNotDropped is the data-loss regression: a
// row that fails with a TRANSIENT (non-poison) error must be left for redelivery
// even when sibling rows in the same bundle commit. The old isolate Term'd any
// failure once committed>0 ("a sibling committed, so the writer is healthy, so
// this row is poison") — which permanently dropped healthy, never-persisted
// events on a mid-bundle S3/catalog blip. The bisect now keys the Term decision
// on lake.ErrPoisonRow, so a transient single-row failure survives.
func TestSink_TransientRowAmongCommittedNotDropped(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	writer := &memWriter{transientID: "blip"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	require.NoError(t, pub.Publish(ctx, event("good1", "dimo.status", "did:erc721:137:0xA:1", ts)))
	require.NoError(t, pub.Publish(ctx, event("blip", "dimo.status", "did:erc721:137:0xA:1", ts)))
	require.NoError(t, pub.Publish(ctx, event("good2", "dimo.status", "did:erc721:137:0xA:1", ts)))

	s := sink.New(sink.Config{MaxAge: 200 * time.Millisecond, MinFlushBytes: 1}, cons, writer, zerolog.Nop())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// The two good rows commit; the transient row is NOT terminated — it stays
	// pending (un-acked) for redelivery. Under the old code it would have been
	// Term'd (committed>0), so NumAckPending would drop to 0 and it would be lost.
	require.Eventually(t, func() bool {
		info, err := cons.Info(context.Background())
		return err == nil && writer.totalEvents() == 2 && info.NumAckPending == 1
	}, 15*time.Second, 100*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	// The transient row never committed and was never terminated.
	ids := map[string]bool{}
	for _, b := range writer.snapshot() {
		for _, ev := range b {
			ids[ev.ID] = true
		}
	}
	assert.Equal(t, map[string]bool{"good1": true, "good2": true}, ids,
		"good rows commit; the transient row is neither committed nor dropped")
}

// TestSink_DurablyBrokenWriterRestartsPod pins the H15 backstop: when EVERY
// commit fails for CommitFailureWindow (a failover-poisoned pinned catalog
// connection never heals in-process), Run must return an error so the pod
// restarts and re-pins its writer connections — instead of grinding
// redeliveries forever while Ready.
func TestSink_DurablyBrokenWriterRestartsPod(t *testing.T) {
	t.Parallel()
	js, cons := setup(t)
	writer := &memWriter{}
	writer.setFail(errors.New("catalog failover: connection poisoned"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := sink.New(sink.Config{
		MaxAge:              50 * time.Millisecond,
		MaxAgeHard:          100 * time.Millisecond,
		MinFlushBytes:       1,
		CommitFailureWindow: 300 * time.Millisecond,
	}, cons, writer, zerolog.Nop())

	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// A failed bundle waits for JetStream redelivery (AckWait), so the streak
	// builds from NEW traffic: keep publishing so fresh bundles keep failing
	// past the window — exactly the steady-state shape of a poisoned conn.
	pub := stream.NewPublisher(js, 1)
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	go func() {
		for i := 0; ctx.Err() == nil && i < 200; i++ {
			_ = pub.Publish(ctx, event(fmt.Sprintf("e-wedge-%d", i), "dimo.status", "did:erc721:137:0xA:1", ts))
			time.Sleep(50 * time.Millisecond)
		}
	}()

	select {
	case err := <-done:
		require.Error(t, err, "Run must surface the durably-broken writer")
		assert.Contains(t, err.Error(), "committed nothing")
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not trip the commit-failure backstop")
	}
}
