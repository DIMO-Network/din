package natsembed_test

import (
	"context"
	"testing"
	"time"

	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun_RequiresStoreDir(t *testing.T) {
	t.Parallel()
	_, err := natsembed.Run(natsembed.Config{})
	require.Error(t, err)
}

// TestRun_MessagesSurviveRestart proves the single-node durability story:
// JetStream file store under StoreDir outlives a server restart.
func TestRun_MessagesSurviveRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	srv, err := natsembed.Run(natsembed.Config{StoreDir: dir})
	require.NoError(t, err)

	conn, err := natsembed.Connect(srv)
	require.NoError(t, err)
	js, err := jetstream.New(conn)
	require.NoError(t, err)

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "PERSIST",
		Subjects: []string{"persist.>"},
		Storage:  jetstream.FileStorage,
	})
	require.NoError(t, err)
	_, err = js.Publish(ctx, "persist.one", []byte("survives"))
	require.NoError(t, err)

	conn.Close()
	srv.Shutdown()
	srv.WaitForShutdown()

	srv2, err := natsembed.Run(natsembed.Config{StoreDir: dir, StartTimeout: 15 * time.Second})
	require.NoError(t, err)
	t.Cleanup(srv2.Shutdown)

	conn2, err := natsembed.Connect(srv2)
	require.NoError(t, err)
	t.Cleanup(conn2.Close)
	js2, err := jetstream.New(conn2)
	require.NoError(t, err)

	s, err := js2.Stream(ctx, "PERSIST")
	require.NoError(t, err)
	info, err := s.Info(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.State.Msgs)
}
