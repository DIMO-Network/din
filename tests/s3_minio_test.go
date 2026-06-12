// s3_minio_test.go exercises din's S3 code paths against a real S3 API by
// launching a throwaway MinIO server (single binary, no Docker). Two suites:
//
//   - TestMinIO_S3ClientParity pins s3client semantics that the fsstore
//     mirrors (lexicographic listing, key-prefix — not directory — list
//     semantics, maxSize enforcement, quiet deletes of missing keys) on a
//     real S3 implementation instead of the in-package fake.
//   - TestMinIO_EndToEnd_DeviceToParquet runs the full ingest pipeline
//     (device POST → JetStream → parquet sink) with the s3client store, so
//     the bundle decode round trip is proven over the wire.
//
// Both skip when the minio binary is not on PATH or in -short mode, keeping
// CI without MinIO green.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/handler"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/s3client"
	"github.com/DIMO-Network/din/internal/sink"
	"github.com/DIMO-Network/din/internal/split"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	minioCreds  = "minioadmin"
	minioRegion = "us-east-1"
)

// startMinIO launches a MinIO server on a free localhost port with a
// t.TempDir() data directory and returns its http:// endpoint. The test is
// skipped when minio is not installed (or in -short mode) so suites stay
// green on machines without it.
func startMinIO(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping MinIO integration test in -short mode")
	}
	bin, err := exec.LookPath("minio")
	if err != nil {
		t.Skip("minio binary not on PATH; install with `brew install minio/stable/minio`")
	}

	// Pick a free port: bind :0, read it back, release it for minio.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	cmd := exec.Command(bin, "server", t.TempDir(), "--address", addr)
	cmd.Env = append(os.Environ(),
		"MINIO_ROOT_USER="+minioCreds,
		"MINIO_ROOT_PASSWORD="+minioCreds,
		"MINIO_BROWSER=off",
	)
	require.NoError(t, cmd.Start(), "starting minio server")
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	endpoint := "http://" + addr
	healthURL := endpoint + "/minio/health/live"
	require.Eventually(t, func() bool {
		resp, err := http.Get(healthURL) //nolint:gosec // local test server
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond, "minio did not report ready at %s", healthURL)

	return endpoint
}

// createMinIOBucket provisions bucket on the MinIO endpoint using a raw AWS
// SDK client (path-style + custom endpoint, same wiring s3client.New uses).
func createMinIOBucket(t *testing.T, endpoint, bucket string) {
	t.Helper()
	raw := s3.New(s3.Options{
		Region:       minioRegion,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(minioCreds, minioCreds, ""),
	})
	_, err := raw.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "creating bucket %s", bucket)
}

// newMinIOClient builds the production s3client against the MinIO endpoint.
func newMinIOClient(t *testing.T, endpoint, bucket string) *s3client.Client {
	t.Helper()
	client, err := s3client.New(context.Background(), s3client.Config{
		Bucket:          bucket,
		Region:          minioRegion,
		AccessKeyID:     minioCreds,
		SecretAccessKey: minioCreds,
		Endpoint:        endpoint,
	})
	require.NoError(t, err)
	return client
}

// TestMinIO_S3ClientParity pins s3client behavior against real S3 semantics:
// the same surface the fsstore unit tests pin on the filesystem backend.
func TestMinIO_S3ClientParity(t *testing.T) {
	endpoint := startMinIO(t)
	ctx := context.Background()
	const bucket = "din-parity"
	createMinIOBucket(t, endpoint, bucket)
	client := newMinIOClient(t, endpoint, bucket)

	// Seed objects deliberately out of lexicographic order, spanning two
	// type= "directories" plus an unrelated prefix.
	seed := map[string][]byte{
		"raw/type=dimo.status/date=2026-06-09/b.parquet": []byte("status-day2"),
		"raw/type=dimo.events/date=2026-06-09/a.parquet": []byte("events-day2"),
		"raw/type=dimo.status/date=2026-06-08/a.parquet": []byte("status-day1"),
		"cloudevent/blobs/payload-1":                     bytes.Repeat([]byte{0xAB}, 4096),
	}
	for key, body := range seed {
		require.NoError(t, client.PutObject(ctx, key, body), "put %s", key)
	}

	// Get round-trips bytes exactly.
	const statusKey = "raw/type=dimo.status/date=2026-06-08/a.parquet"
	got, err := client.GetObject(ctx, statusKey, 0)
	require.NoError(t, err)
	assert.Equal(t, seed[statusKey], got)

	// maxSize: exactly the object size passes, one byte under fails.
	size := int64(len(seed[statusKey]))
	got, err = client.GetObject(ctx, statusKey, size)
	require.NoError(t, err)
	assert.Equal(t, seed[statusKey], got)
	_, err = client.GetObject(ctx, statusKey, size-1)
	require.ErrorContains(t, err, "exceeds max size", "real S3 returns Content-Length so the pre-read guard fires")

	// Missing key surfaces an error (NoSuchKey under the wrap).
	_, err = client.GetObject(ctx, "raw/no-such-key", 0)
	require.Error(t, err)

	// List semantics: prefixes are key prefixes, not directories. "raw/type="
	// spans both event types, and S3 returns keys in lexicographic order —
	// dimo.events before dimo.status, dates ascending within a type.
	objects, err := client.ListObjectsV2(ctx, "raw/type=")
	require.NoError(t, err)
	keys := make([]string, len(objects))
	for i, obj := range objects {
		keys[i] = obj.Key
		assert.Equal(t, int64(len(seed[obj.Key])), obj.Size, "size of %s", obj.Key)
	}
	assert.Equal(t, []string{
		"raw/type=dimo.events/date=2026-06-09/a.parquet",
		"raw/type=dimo.status/date=2026-06-08/a.parquet",
		"raw/type=dimo.status/date=2026-06-09/b.parquet",
	}, keys, "S3 lists in lexicographic key order across type= partitions")

	// Narrower prefix stays within one type.
	objects, err = client.ListObjectsV2(ctx, "raw/type=dimo.status/")
	require.NoError(t, err)
	require.Len(t, objects, 2)

	// Unknown prefix lists empty, not an error (matches fsstore).
	objects, err = client.ListObjectsV2(ctx, "raw/type=dimo.unknown/")
	require.NoError(t, err)
	assert.Empty(t, objects)

	// DeleteObjects tolerates missing keys (S3 quiet-delete) and removes the
	// rest; an empty key slice is a no-op.
	require.NoError(t, client.DeleteObjects(ctx, nil))
	err = client.DeleteObjects(ctx, []string{
		"raw/type=dimo.events/date=2026-06-09/a.parquet",
		"raw/type=dimo.missing/never-existed.parquet",
	})
	require.NoError(t, err, "deleting a missing key alongside a real one must not fail")
	objects, err = client.ListObjectsV2(ctx, "raw/")
	require.NoError(t, err)
	require.Len(t, objects, 2, "only the two status objects remain")
}

// TestMinIO_EndToEnd_DeviceToParquet is the din e2e (device POST → convert →
// split → JetStream → parquet sink) wired to the s3client store on MinIO
// instead of fsstore: the raw bundle must land in the right hive partition
// on real S3 and decode back to the original event.
func TestMinIO_EndToEnd_DeviceToParquet(t *testing.T) {
	endpoint := startMinIO(t)
	const bucket = "din-e2e"
	createMinIOBucket(t, endpoint, bucket)
	store := e2eStore{Store: newMinIOClient(t, endpoint, bucket)}

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

	sinkConsumer, err := rawStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "parquet-sink-minio", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 5 * time.Minute,
	})
	require.NoError(t, err)
	go func() {
		_ = sink.New(sink.Config{MaxAge: 200 * time.Millisecond}, sinkConsumer, store, zerolog.Nop()).Run(ctx)
	}()

	// POST a default-module status payload as a device would.
	subject := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	payload, _ := json.Marshal(map[string]any{
		"type":        cloudevent.TypeStatus,
		"subject":     subject,
		"source":      "0xConnLicense",
		"producer":    subject,
		"id":          "device-msg-minio-1",
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

	// Raw parquet bundle lands in the right hive partition on MinIO.
	wantPrefix := "raw/type=dimo.status/date=" + ts.Format("2006-01-02") + "/"
	require.Eventually(t, func() bool { return len(store.keys(wantPrefix)) == 1 }, 10*time.Second, 100*time.Millisecond,
		"sink must write exactly one bundle under %s", wantPrefix)

	bundleKey := store.keys(wantPrefix)[0]
	body := store.get(bundleKey)
	require.NotEmpty(t, body, "bundle %s must be fetchable from MinIO", bundleKey)
	events, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "bundle fetched from MinIO must decode")
	require.Len(t, events, 1)
	assert.Equal(t, subject, events[0].Subject)
	assert.Equal(t, "device-msg-minio-1", events[0].ID)
	assert.JSONEq(t, `{"signals":[{"name":"speed","timestamp":"`+ts.Format(time.RFC3339Nano)+`","value":72.5}]}`,
		string(events[0].Data), "raw payload stored verbatim on S3 — parse-on-read source of truth")
}
