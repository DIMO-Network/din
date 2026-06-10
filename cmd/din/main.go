// din — DIMO Ingest Node. Single binary replacing dis + dps +
// parquet-processor: HTTP ingest (mTLS + JWT) → NATS JetStream WAL →
// hive-partitioned raw parquet on S3, with self-compaction and an optional
// decoded-stream bridge for vehicle-triggers-api.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DIMO-Network/din/internal/attest"
	"github.com/DIMO-Network/din/internal/compact"
	"github.com/DIMO-Network/din/internal/config"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/decodestream"
	"github.com/DIMO-Network/din/internal/fsstore"
	"github.com/DIMO-Network/din/internal/handler"
	"github.com/DIMO-Network/din/internal/natsembed"
	"github.com/DIMO-Network/din/internal/objstore"
	"github.com/DIMO-Network/din/internal/s3client"
	"github.com/DIMO-Network/din/internal/server"
	"github.com/DIMO-Network/din/internal/sink"
	"github.com/DIMO-Network/din/internal/split"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Str("app", "din").Logger()
	if err := run(log); err != nil {
		log.Fatal().Err(err).Msg("din exited with error")
	}
}

func run(log zerolog.Logger) error {
	settings, err := config.Load()
	if err != nil {
		return err
	}
	if level, err := zerolog.ParseLevel(settings.LogLevel); err == nil {
		zerolog.SetGlobalLevel(level)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// NATS: embedded for single-node, external cluster otherwise.
	var conn *nats.Conn
	switch settings.NATSMode {
	case "embedded":
		srv, err := natsembed.Run(natsembed.Config{StoreDir: settings.NATSStoreDir, Port: -1})
		if err != nil {
			return err
		}
		defer srv.Shutdown()
		if conn, err = natsembed.Connect(srv); err != nil {
			return err
		}
	default:
		if conn, err = nats.Connect(settings.NATSURL); err != nil {
			return err
		}
	}
	defer conn.Close()

	// Bound in-flight async publishes: when JetStream stalls, publishers
	// queue here instead of growing the client buffer without limit; the
	// HTTP write timeout is the backstop that turns the stall into 503s.
	js, err := jetstream.New(conn, jetstream.WithPublishAsyncMaxPending(4096))
	if err != nil {
		return err
	}
	streamCfg := stream.DefaultConfig()
	streamCfg.Replicas = settings.NATSReplicas
	rawStream, err := stream.EnsureStream(ctx, js, streamCfg)
	if err != nil {
		return err
	}

	// Storage. Blobs (externalized >1MB payloads) live in their own bucket
	// like dis's BLOB_BUCKET; falling back to the parquet bucket would
	// split durable documents across two locations. Each bucket value picks
	// its backend independently: absolute path or file:// → local
	// filesystem, anything else → S3.
	store, err := newObjectStore(ctx, settings, settings.ParquetBucket)
	if err != nil {
		return err
	}
	if settings.BlobBucket == "" {
		return errors.New("BLOB_BUCKET is required")
	}
	blobStore, err := newObjectStore(ctx, settings, settings.BlobBucket)
	if err != nil {
		return err
	}

	// Conversion + attestation.
	convertCfg := convert.Config{
		ChainID:               settings.ChainID,
		VehicleNFTAddress:     settings.VehicleNFTAddress,
		AftermarketNFTAddress: settings.AftermarketNFTAddress,
		SyntheticNFTAddress:   settings.SyntheticNFTAddress,
	}
	converter := convert.NewConverter(log, convertCfg)
	verifier, err := attest.NewVerifier(settings.RPCURL, log)
	if err != nil {
		return err
	}

	handlers := &handler.Handlers{
		Converter:           converter,
		Attest:              verifier,
		Splitter:            split.New(blobStore, settings.BlobPrefix, settings.DocumentSizeLimit),
		Publisher:           stream.NewPublisher(js),
		ValidateFingerprint: settings.FingerprintValidation,
		Log:                 log,
	}

	connectionSrv, err := server.NewConnectionServer(server.ConnectionConfig{
		Addr:           settings.ConnectionAddr,
		TLSCertFile:    settings.TLSCertFile,
		TLSKeyFile:     settings.TLSKeyFile,
		ClientCAFiles:  []string{settings.TLSClientCAFile},
		MaxBodyBytes:   settings.MaxBodyBytes,
		RateLimitRPS:   settings.RateLimitRPS,
		RateLimitBurst: settings.RateLimitBurst,
		Logger:         log,
	}, postOnly(handlers.Connection()))
	if err != nil {
		return err
	}
	attestationSrv, err := server.NewAttestationServer(server.AttestationConfig{
		Addr:                   settings.AttestationAddr,
		TokenExchangeIssuer:    settings.TokenExchangeIssuer,
		TokenExchangeKeySetURL: settings.TokenExchangeKeySetURL,
		MaxBodyBytes:           settings.MaxBodyBytes,
		RateLimitRPS:           settings.RateLimitRPS,
		RateLimitBurst:         settings.RateLimitBurst,
		Logger:                 log,
	}, postOnly(handlers.Attestation()))
	if err != nil {
		return err
	}
	opsSrv := server.NewOpsServer(server.OpsConfig{Addr: settings.OpsAddr})

	// Sink consumer.
	sinkConsumer, err := rawStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "parquet-sink",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       5 * time.Minute,
		MaxAckPending: 250_000,
	})
	if err != nil {
		return err
	}

	group, gctx := errgroup.WithContext(ctx)

	group.Go(func() error { return serveHTTP(gctx, connectionSrv, true, log) })
	group.Go(func() error { return serveHTTP(gctx, attestationSrv, false, log) })
	group.Go(func() error { return serveHTTP(gctx, opsSrv, false, log) })
	group.Go(func() error { return sink.New(sink.Config{}, sinkConsumer, store, log).Run(gctx) })

	if settings.CompactorEnabled {
		compactStore := &compactStoreAdapter{client: store}
		group.Go(func() error {
			return compact.New(compact.Config{DecodedPrefix: settings.DecodedPrefix}, compactStore, log).Run(gctx)
		})
	}
	if settings.DecodeStreamEnabled {
		bridge := decodestream.New(decodestream.Config{
			ChainID:           settings.ChainID,
			VehicleNFTAddress: settings.VehicleNFTAddress,
			Replicas:          settings.NATSReplicas,
		}, js, log)
		if err := bridge.EnsureStreams(ctx); err != nil {
			return err
		}
		group.Go(func() error { return bridge.Run(gctx) })
	}

	log.Info().Str("connection", settings.ConnectionAddr).Str("attestation", settings.AttestationAddr).
		Str("nats", settings.NATSMode).Msg("din started")
	return group.Wait()
}

// postOnly rejects non-POST requests, mirroring dis's allowed_verbs: [POST].
func postOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// newObjectStore picks the storage backend from the bucket value: local
// filesystem for path-like values, S3 otherwise.
func newObjectStore(ctx context.Context, settings config.Settings, bucket string) (objstore.Store, error) {
	if objstore.IsLocalPath(bucket) {
		return fsstore.New(objstore.LocalRoot(bucket))
	}
	return s3client.New(ctx, s3client.Config{
		Bucket:          bucket,
		Region:          settings.S3Region,
		AccessKeyID:     settings.S3AccessKeyID,
		SecretAccessKey: settings.S3SecretAccessKey,
		Endpoint:        settings.S3Endpoint,
	})
}

// compactStoreAdapter narrows a Store to the compactor's surface.
type compactStoreAdapter struct {
	client objstore.Store
}

func (a *compactStoreAdapter) List(ctx context.Context, prefix string) ([]compact.ObjectInfo, error) {
	objects, err := a.client.ListObjectsV2(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]compact.ObjectInfo, len(objects))
	for i, obj := range objects {
		out[i] = compact.ObjectInfo{Key: obj.Key, Size: obj.Size}
	}
	return out, nil
}

func (a *compactStoreAdapter) GetObject(ctx context.Context, key string) ([]byte, error) {
	return a.client.GetObject(ctx, key, 0)
}

func (a *compactStoreAdapter) PutObject(ctx context.Context, key string, body []byte) error {
	return a.client.PutObject(ctx, key, body)
}

func (a *compactStoreAdapter) DeleteObject(ctx context.Context, key string) error {
	return a.client.DeleteObjects(ctx, []string{key})
}

// serveHTTP runs srv until ctx cancels, then shuts it down gracefully.
func serveHTTP(ctx context.Context, srv *http.Server, useTLS bool, log zerolog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		var err error
		if useTLS {
			err = srv.ListenAndServeTLS("", "")
		} else {
			err = srv.ListenAndServe()
		}
		if !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Warn().Err(err).Str("addr", srv.Addr).Msg("graceful shutdown failed")
		}
		return nil
	}
}
