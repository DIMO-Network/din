// din — DIMO Ingest Node. Single binary replacing dis + dps +
// parquet-processor: HTTP ingest (mTLS + JWT) → NATS JetStream WAL →
// DuckLake raw_events table (partitioned parquet on S3 tracked by a SQL
// catalog), with built-in lake maintenance and an optional decoded-stream
// bridge for vehicle-triggers-api.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DIMO-Network/din/internal/attest"
	"github.com/DIMO-Network/din/internal/config"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/decodestream"
	"github.com/DIMO-Network/din/internal/fsstore"
	"github.com/DIMO-Network/din/internal/handler"
	"github.com/DIMO-Network/din/internal/lake"
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

	// Build-time/ops subcommands; the bare binary runs the service.
	if len(os.Args) > 1 {
		if err := runSubcommand(os.Args[1:], log); err != nil {
			log.Fatal().Err(err).Str("subcommand", os.Args[1]).Msg("din exited with error")
		}
		return
	}

	if err := run(log); err != nil {
		log.Fatal().Err(err).Msg("din exited with error")
	}
}

func runSubcommand(args []string, log zerolog.Logger) error {
	switch args[0] {
	case "install-duckdb-extensions":
		if len(args) != 2 {
			return errors.New("usage: din install-duckdb-extensions <dir>")
		}
		return lake.InstallExtensions(context.Background(), args[1])
	case "maintain":
		// One maintenance cycle, then exit — for a k8s CronJob when the
		// ingest deployment runs more than one replica (exactly one
		// maintenance process may run per catalog).
		return maintainOnce(log)
	case "lake-backfill":
		// One-time registration of legacy DIS bundles into the lake.
		if len(args) != 2 {
			return errors.New("usage: din lake-backfill <s3://bucket/prefix/ | /abs/dir>")
		}
		return lakeBackfill(args[1], log)
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

// lakeBackfill lists parquet bundles under source and registers them into
// raw_events. Idempotent — rerun after any failure; already-registered
// files are skipped.
func lakeBackfill(source string, log zerolog.Logger) error {
	settings, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var bucket, prefix, uriBase string
	if after, ok := strings.CutPrefix(source, "s3://"); ok {
		bucket, prefix, _ = strings.Cut(after, "/")
		uriBase = "s3://" + bucket + "/"
	} else if objstore.IsLocalPath(source) {
		bucket = source
		uriBase = strings.TrimSuffix(objstore.LocalRoot(source), "/") + "/"
	} else {
		return fmt.Errorf("source must be s3://bucket/prefix/ or an absolute path, got %q", source)
	}
	store, err := newObjectStore(ctx, settings, bucket)
	if err != nil {
		return err
	}
	objects, err := store.ListObjectsV2(ctx, prefix)
	if err != nil {
		return fmt.Errorf("listing %s: %w", source, err)
	}
	files := make([]string, 0, len(objects))
	for _, obj := range objects {
		files = append(files, uriBase+obj.Key)
	}
	log.Info().Int("objects", len(files)).Str("source", source).Msg("backfill source listed")

	lk, err := lake.Open(ctx, lakeConfig(settings))
	if err != nil {
		return err
	}
	defer lk.Close() //nolint:errcheck
	res, err := lk.Backfill(ctx, files, log)
	if err != nil {
		return err
	}
	log.Info().Int("registered", res.Registered).Int("skipped", res.Skipped).Msg("backfill complete")
	return nil
}

func maintainOnce(log zerolog.Logger) error {
	settings, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lk, err := lake.Open(ctx, lakeConfig(settings))
	if err != nil {
		return err
	}
	defer lk.Close() //nolint:errcheck
	return lake.NewMaintainer(lk, maintConfig(settings), log).Cycle(ctx)
}

func lakeConfig(settings config.Settings) lake.Config {
	return lake.Config{
		CatalogDSN:        settings.LakeCatalogDSN,
		DataPath:          settings.LakeDataPath,
		S3Region:          settings.S3Region,
		S3AccessKeyID:     settings.S3AccessKeyID,
		S3SecretAccessKey: settings.S3SecretAccessKey,
		S3Endpoint:        settings.S3Endpoint,
		MemoryLimit:       settings.LakeMemoryLimit,
		Threads:           settings.LakeThreads,
		TargetFileSize:    settings.LakeTargetFileSize,
		ExtensionDir:      settings.LakeExtensionDir,
		MaxConns:          settings.NATSStreamPartitions + 2,
	}
}

// maintConfig builds the maintainer config from settings. Single source of
// truth so every caller plumbs the same fields — notably ConsumerStaleness,
// which gates how long a lagging consumer (dq) is protected from snapshot
// expiry before its cursor range is reclaimed (SR-2).
func maintConfig(settings config.Settings) lake.MaintConfig {
	return lake.MaintConfig{
		Interval:          settings.LakeMaintInterval,
		SnapshotKeep:      settings.LakeSnapshotKeep,
		ConsumerStaleness: settings.LakeConsumerStaleness,
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
	streamCfg.Partitions = settings.NATSStreamPartitions
	rawStreams, err := stream.EnsureStreams(ctx, js, streamCfg)
	if err != nil {
		return err
	}

	// Storage. Raw events live in the DuckLake; blobs (externalized >1MB
	// payloads) keep their own bucket like dis's BLOB_BUCKET — durable
	// documents must not split across two locations.
	lk, err := lake.Open(ctx, lakeConfig(settings))
	if err != nil {
		return err
	}
	defer lk.Close() //nolint:errcheck

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
		Publisher:           stream.NewPublisher(js, settings.NATSStreamPartitions),
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

	group, gctx := errgroup.WithContext(ctx)

	group.Go(func() error { return serveHTTP(gctx, connectionSrv, true, log) })
	group.Go(func() error { return serveHTTP(gctx, attestationSrv, false, log) })
	group.Go(func() error { return serveHTTP(gctx, opsSrv, false, log) })

	// One sink per WAL partition: disjoint subjects, independent flush
	// pipelines — throughput scales with the partition count. Each sink
	// gets its own writer connection; concurrent DuckLake appends never
	// conflict.
	for i, rawStream := range rawStreams {
		durable := "parquet-sink"
		if len(rawStreams) > 1 {
			durable = fmt.Sprintf("parquet-sink-p%03d", i)
		}
		sinkConsumer, err := rawStream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:       durable,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       5 * time.Minute,
			MaxAckPending: 250_000,
		})
		if err != nil {
			return err
		}
		writer, err := lk.NewWriter(ctx, lake.RawTable)
		if err != nil {
			return err
		}
		defer writer.Close() //nolint:errcheck
		group.Go(func() error { return sink.New(sink.Config{}, sinkConsumer, writer, log).Run(gctx) })
	}

	if settings.LakeMaintenanceEnabled {
		maintainer := lake.NewMaintainer(lk, maintConfig(settings), log)
		group.Go(func() error { return maintainer.Run(gctx) })
	}
	if settings.DecodeStreamEnabled {
		bridge := decodestream.New(decodestream.Config{
			ChainID:           settings.ChainID,
			VehicleNFTAddress: settings.VehicleNFTAddress,
			Replicas:          settings.NATSReplicas,
			StreamPartitions:  settings.NATSStreamPartitions,
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
