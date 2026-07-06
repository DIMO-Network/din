package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// TestEnsureStreams_UpgradesExistingStreamToDiscardNew pins the PROD UPGRADE
// PATH: streams created by the pre-H12 binary (JetStream default DiscardOld,
// no MaxBytes) must be updatable in place by the new EnsureStreams — the next
// deploy flips them to DiscardNew without recreation or backlog loss.
func TestEnsureStreams_UpgradesExistingStreamToDiscardNew(t *testing.T) {
	srv, err := natsembed.Run(natsembed.Config{StoreDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(srv.Shutdown)
	conn, err := natsembed.Connect(srv)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	require.NoError(t, err)
	ctx := context.Background()

	// Old-binary shape: same stream, JetStream default discard (DiscardOld).
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "INGEST_RAW",
		Subjects:  []string{"in.raw.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    48 * time.Hour,
	})
	require.NoError(t, err)
	// Backlog exists before the upgrade.
	_, err = js.Publish(ctx, "in.raw.dimo.status.d1", []byte("pre-upgrade"))
	require.NoError(t, err)

	// New binary boots: CreateOrUpdateStream must UPDATE in place.
	streams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err, "upgrade of an existing DiscardOld stream must succeed in place")
	info, err := streams[0].Info(ctx)
	require.NoError(t, err)
	require.Equal(t, jetstream.DiscardNew, info.Config.Discard, "existing stream flipped to DiscardNew")
	require.EqualValues(t, 1, info.State.Msgs, "pre-upgrade backlog survives the config update")
}
