// s3_minio_test.go exercises din's S3 code paths against a real S3 API by
// launching a throwaway MinIO server (single binary, no Docker). Two suites:
//
//   - TestMinIO_S3ClientParity pins s3client semantics that the fsstore
//     mirrors (lexicographic listing, key-prefix — not directory — list
//     semantics) on a real S3 implementation instead of the in-package
//     fake. s3client serves the blob bucket.
//   - TestMinIO_EndToEnd_DeviceToDuckLake runs the full ingest pipeline
//     (device POST → JetStream → sink → DuckLake commit) with the lake's
//     DATA_PATH on MinIO, then a maintenance cycle, proving the
//     httpfs/secret wiring writes real parquet over the wire.
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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/fsstore"
	"github.com/DIMO-Network/din/internal/handler"
	"github.com/DIMO-Network/din/internal/lake"
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
}

// TestMinIO_EncryptedLake_RoundTrip exercises the production encryption path the
// unit tests can't reach: an s3:// DATA_PATH driving INSTALL httpfs + CREATE
// SECRET + ATTACH ... ENCRYPTED. The lake reads its own data back over the wire
// (the per-file key comes from the catalog), but the parquet object sitting in
// MinIO does not read as plain parquet — proving at-rest encryption on real S3.
func TestMinIO_EncryptedLake_RoundTrip(t *testing.T) {
	endpoint := startMinIO(t)
	const bucket = "din-enc"
	createMinIOBucket(t, endpoint, bucket)
	ctx := context.Background()

	lk, err := lake.Open(ctx, lake.Config{
		CatalogDSN:        filepath.Join(t.TempDir(), "meta.ducklake"),
		DataPath:          "s3://" + bucket + "/lake/",
		S3Region:          minioRegion,
		S3AccessKeyID:     minioCreds,
		S3SecretAccessKey: minioCreds,
		S3Endpoint:        endpoint,
		Encrypted:         true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	w, err := lk.NewWriter(ctx, lake.RawTable)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	base := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	bundle := make([]cloudevent.StoredEvent, 0, 3000)
	for i := range 3000 {
		bundle = append(bundle, cloudevent.StoredEvent{RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        "dimo.status",
				Subject:     fmt.Sprintf("did:erc721:137:0xVeh:%d", i%64),
				Source:      "0xConn", Producer: "0xDev",
				ID:   fmt.Sprintf("evt-%05d", i),
				Time: base.Add(time.Duration(i) * time.Second),
			},
			Data: json.RawMessage(`{"v":1}`),
		}})
	}
	require.NoError(t, w.WriteBundle(ctx, bundle))

	// Flush inlined data out to parquet objects on MinIO.
	require.NoError(t, lake.NewMaintainer(lk, lake.MaintConfig{}, zerolog.Nop()).Cycle(ctx))

	// Transparent read through the catalog decrypts.
	var n int
	require.NoError(t, lk.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, 3000, n, "encrypted lake must read its own data back over httpfs")

	// Find a parquet object on MinIO and prove it's encrypted at rest: reading it
	// directly (the catalog key isn't used) must fail specifically on encryption.
	s3Client := newMinIOClient(t, endpoint, bucket)
	objects, err := s3Client.ListObjectsV2(ctx, "lake/")
	require.NoError(t, err)
	var parquetKey string
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetKey = obj.Key
			break
		}
	}
	require.NotEmpty(t, parquetKey, "maintenance must write a parquet object to the lake DATA_PATH")

	var dummy int
	rerr := lk.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(*) FROM read_parquet('s3://%s/%s')", bucket, parquetKey)).Scan(&dummy)
	require.Error(t, rerr, "the parquet object in the bucket must not read as plain parquet")
	assert.Contains(t, strings.ToLower(rerr.Error()), "encrypt",
		"raw read must fail specifically because the file is encrypted")
}

// TestMinIO_EndToEnd_DeviceToDuckLake is the din e2e (device POST → convert
// → split → JetStream → sink → DuckLake commit) with the lake's DATA_PATH
// on MinIO: the row must be queryable after ack, and a maintenance cycle
// must materialize it to a parquet object on real S3 (small commits inline
// into the catalog until maintenance flushes them).
func TestMinIO_EndToEnd_DeviceToDuckLake(t *testing.T) {
	endpoint := startMinIO(t)
	const bucket = "din-e2e"
	createMinIOBucket(t, endpoint, bucket)

	lk, err := lake.Open(context.Background(), lake.Config{
		CatalogDSN:        filepath.Join(t.TempDir(), "meta.ducklake"),
		DataPath:          "s3://" + bucket + "/lake/",
		S3Region:          minioRegion,
		S3AccessKeyID:     minioCreds,
		S3SecretAccessKey: minioCreds,
		S3Endpoint:        endpoint,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = lk.Close() })

	blobStore, err := fsstore.New(t.TempDir())
	require.NoError(t, err)

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

	rawStreams, err := stream.EnsureStreams(ctx, js, stream.DefaultConfig())
	require.NoError(t, err)

	cfg := convert.Config{ChainID: 137, VehicleNFTAddress: vehicleNFT}
	convert.RegisterModules(cfg)

	handlers := &handler.Handlers{
		Converter: convert.NewConverter(zerolog.Nop(), cfg),
		Splitter:  split.New(blobStore, "cloudevent/blobs/", 1<<20),
		Publisher: stream.NewPublisher(js, 1),
		Log:       zerolog.Nop(),
	}
	httpSrv := httptest.NewServer(sourceInjector("0xc0dec0dec0dec0dec0dec0dec0dec0dec0dec0de", handlers.Connection()))
	t.Cleanup(httpSrv.Close)

	sinkConsumer, err := rawStreams[0].CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "parquet-sink-minio", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 5 * time.Minute,
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

	// POST a default-module status payload as a device would.
	subject := fmt.Sprintf("did:erc721:137:%s:42", vehicleNFT.Hex())
	ts := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	payload, _ := json.Marshal(map[string]any{
		"type":        cloudevent.TypeStatus,
		"subject":     subject,
		"source":      "0xc0dec0dec0dec0dec0dec0dec0dec0dec0dec0de",
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

	// The committed row is queryable through the lake.
	require.Eventually(t, func() bool {
		var n int
		err := lk.DB().QueryRowContext(ctx, "SELECT count(*) FROM lake.raw_events").Scan(&n)
		return err == nil && n == 1
	}, 10*time.Second, 100*time.Millisecond)

	var gotSubject, gotID, gotData string
	require.NoError(t, lk.DB().QueryRowContext(ctx,
		`SELECT subject, id, data FROM lake.raw_events WHERE type = 'dimo.status'`).
		Scan(&gotSubject, &gotID, &gotData))
	assert.Equal(t, subject, gotSubject)
	assert.Equal(t, "device-msg-minio-1", gotID)
	assert.JSONEq(t, `{"signals":[{"name":"speed","timestamp":"`+ts.Format(time.RFC3339Nano)+`","value":72.5}]}`,
		gotData, "raw payload stored verbatim — parse-on-read source of truth")

	// Maintenance materializes the inlined row to parquet on MinIO.
	require.NoError(t, lake.NewMaintainer(lk, lake.MaintConfig{}, zerolog.Nop()).Cycle(ctx))

	s3Client := newMinIOClient(t, endpoint, bucket)
	objects, err := s3Client.ListObjectsV2(ctx, "lake/")
	require.NoError(t, err)
	var parquetKeys []string
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetKeys = append(parquetKeys, obj.Key)
		}
	}
	require.NotEmpty(t, parquetKeys, "maintenance must write parquet to the lake DATA_PATH on MinIO")

	var n int
	require.NoError(t, lk.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, 1, n, "row survives materialization")
}
