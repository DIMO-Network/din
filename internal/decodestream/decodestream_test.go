package decodestream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/decodestream"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var vehicleNFT = common.HexToAddress("0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF")

func vehicleDID(tokenID int) string {
	return fmt.Sprintf("did:erc721:137:%s:%d", vehicleNFT.Hex(), tokenID)
}

func setup(t *testing.T) jetstream.JetStream {
	t.Helper()
	srv, err := natsembed.Run(natsembed.Config{StoreDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(srv.Shutdown)
	conn, err := natsembed.Connect(srv)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	require.NoError(t, err)

	_, err = stream.EnsureStreams(context.Background(), js, stream.DefaultConfig())
	require.NoError(t, err)
	return js
}

// rawStatus builds a dimo.status event in the default-module signal format.
func rawStatus(id, subject string, ts time.Time, signals ...map[string]any) *cloudevent.StoredEvent {
	payload, _ := json.Marshal(map[string]any{"signals": signals})
	return &cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        cloudevent.TypeStatus,
			Subject:     subject,
			Source:      "0xUnregisteredSourceFallsBackToDefault",
			Producer:    subject,
			ID:          id,
			Time:        ts,
			DataVersion: "default/v1.0",
		},
		Data: payload,
	}}
}

func TestBridge_StatusToPerNameSignalSubjects(t *testing.T) {
	t.Parallel()
	js := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bridge := decodestream.New(decodestream.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
	}, js, zerolog.Nop())
	require.NoError(t, bridge.EnsureStreams(ctx))

	done := make(chan error, 1)
	go func() { done <- bridge.Run(ctx) }()

	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	pub := stream.NewPublisher(js, 1)
	require.NoError(t, pub.Publish(ctx, rawStatus("evt-1", vehicleDID(42), ts,
		map[string]any{"name": "speed", "timestamp": ts.Format(time.RFC3339Nano), "value": 88.5},
		map[string]any{"name": "speed", "timestamp": ts.Add(time.Second).Format(time.RFC3339Nano), "value": 90.0},
	)))

	sigStream, err := js.Stream(ctx, decodestream.SignalsStreamName)
	require.NoError(t, err)
	cons, err := sigStream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "dimo.signals.speed",
	})
	require.NoError(t, err)

	msg, err := cons.Next(jetstream.FetchMaxWait(10 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, "dimo.signals.speed", msg.Subject())

	var signalCE vss.SignalCloudEvent
	require.NoError(t, json.Unmarshal(msg.Data(), &signalCE))
	assert.Equal(t, cloudevent.TypeSignals, signalCE.Type)
	assert.Equal(t, vehicleDID(42), signalCE.Subject)

	signals := vss.UnpackSignals(signalCE)
	require.Len(t, signals, 2)
	assert.Equal(t, "speed", signals[0].Data.Name)
	assert.Equal(t, 88.5, signals[0].Data.ValueNumber)
	assert.Equal(t, 90.0, signals[1].Data.ValueNumber)

	cancel()
	require.NoError(t, <-done)
}

func TestBridge_IgnoresNonVehicleSubjects(t *testing.T) {
	t.Parallel()
	js := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bridge := decodestream.New(decodestream.Config{
		ChainID:           137,
		VehicleNFTAddress: vehicleNFT,
	}, js, zerolog.Nop())
	require.NoError(t, bridge.EnsureStreams(ctx))

	done := make(chan error, 1)
	go func() { done <- bridge.Run(ctx) }()

	ts := time.Now().UTC().Add(-time.Minute)
	pub := stream.NewPublisher(js, 1)
	// Wrong contract address: must not produce signals.
	require.NoError(t, pub.Publish(ctx, rawStatus("evt-x", "did:erc721:137:0x0000000000000000000000000000000000000001:7", ts,
		map[string]any{"name": "speed", "timestamp": ts.Format(time.RFC3339Nano), "value": 1.0},
	)))

	time.Sleep(2 * time.Second)
	sigStream, err := js.Stream(ctx, decodestream.SignalsStreamName)
	require.NoError(t, err)
	info, err := sigStream.Info(ctx)
	require.NoError(t, err)
	assert.Zero(t, info.State.Msgs, "non-vehicle subjects must not decode")

	cancel()
	require.NoError(t, <-done)
}

func TestBridge_PrunesDuplicateSignals(t *testing.T) {
	t.Parallel()
	js := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bridge := decodestream.New(decodestream.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}, js, zerolog.Nop())
	require.NoError(t, bridge.EnsureStreams(ctx))
	done := make(chan error, 1)
	go func() { done <- bridge.Run(ctx) }()

	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	dup := map[string]any{"name": "speed", "timestamp": ts.Format(time.RFC3339Nano), "value": 50.0}
	pub := stream.NewPublisher(js, 1)
	require.NoError(t, pub.Publish(ctx, rawStatus("evt-dup", vehicleDID(43), ts, dup, dup)))

	sigStream, err := js.Stream(ctx, decodestream.SignalsStreamName)
	require.NoError(t, err)
	cons, err := sigStream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "dimo.signals.speed",
	})
	require.NoError(t, err)
	msg, err := cons.Next(jetstream.FetchMaxWait(10 * time.Second))
	require.NoError(t, err)

	var signalCE vss.SignalCloudEvent
	require.NoError(t, json.Unmarshal(msg.Data(), &signalCE))
	assert.Len(t, vss.UnpackSignals(signalCE), 1, "exact duplicates pruned")

	cancel()
	require.NoError(t, <-done)
}
