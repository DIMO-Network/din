// Package config loads din's runtime configuration from the environment.
// Variable names stay dis-compatible wherever the semantics carried over.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Settings is the full runtime configuration.
type Settings struct {
	LogLevel string

	// HTTP servers.
	ConnectionAddr  string // DIS_CONNECTION_ADDRESS
	AttestationAddr string // DIS_ATTESTATION_ADDRESS
	OpsAddr         string
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	MaxBodyBytes    int64
	RateLimitRPS    float64
	RateLimitBurst  int

	// Auth.
	TokenExchangeIssuer    string
	TokenExchangeKeySetURL string

	// Chain / module registry.
	ChainID               uint64 // DIMO_REGISTRY_CHAIN_ID
	VehicleNFTAddress     common.Address
	AftermarketNFTAddress common.Address
	SyntheticNFTAddress   common.Address
	RPCURL                string // for ERC-1271 attestation checks

	// NATS.
	NATSMode     string // embedded | external
	NATSURL      string
	NATSStoreDir string
	NATSReplicas int
	// NATSStreamPartitions splits INGEST_RAW into N streams by subject
	// hash. Changing it re-routes subjects; drain the old streams first.
	NATSStreamPartitions int
	// NATSStreamMaxBytes caps each partition stream's on-disk size (bytes), a
	// hard backstop so a prolonged sink stall can't grow the WAL without bound
	// before MaxAge (48h) reclaims it. 0 (default) = unlimited.
	NATSStreamMaxBytes int64

	// Storage.
	BlobBucket        string
	BlobPrefix        string
	DocumentSizeLimit int // DOCUMENT_SIZE_THRESHOLD
	S3Region          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Endpoint        string
	// S3KMSKeyID is the customer-managed KMS key ARN for S3 SSE-KMS, applied to
	// both the blob-store PutObject and DuckLake's httpfs Parquet writes. Empty
	// leaves the bucket default (SSE-S3) in place — no per-object KMS key.
	S3KMSKeyID string // S3_KMS_KEY_ID

	// DuckLake. The catalog database is PostgreSQL in production
	// (multi-process writes) or a local file for dev/test; DataPath is
	// where Parquet lands and is immutable once the catalog exists.
	LakeCatalogDSN     string // DUCKLAKE_CATALOG_DSN
	LakeDataPath       string // DUCKLAKE_DATA_PATH: s3://bucket/prefix/ or absolute path
	LakeTempDirectory  string // DUCKDB_TEMP_DIRECTORY: DuckDB spill volume (e.g. /tmp/duckdb)
	LakeMemoryLimit    string // DUCKDB_MEMORY_LIMIT, e.g. "1GB"
	LakeThreads        int    // DUCKDB_THREADS
	LakeTargetFileSize string // LAKE_TARGET_FILE_SIZE, e.g. "512MB"
	LakeParquetVersion string // LAKE_PARQUET_VERSION: "1" or "2" (default 2)
	LakeCompression    string // LAKE_COMPRESSION: snappy (default) | zstd | lz4 | uncompressed
	LakeExtensionDir   string // DUCKDB_EXTENSION_DIR: pre-baked DuckDB extensions
	// LakeEncryptionEnabled turns on DuckLake's ENCRYPTED mode: every data file
	// is written with Parquet AES-GCM encryption and its per-file key is kept in
	// the catalog, so a leaked DataPath bucket is useless ciphertext without
	// catalog access. The catalog is the trust root once this is on. Decided at
	// creation; treat as immutable for a given catalog.
	LakeEncryptionEnabled bool // LAKE_ENCRYPTION_ENABLED

	// Lake maintenance (compaction, snapshot expiry, file cleanup).
	// Run exactly one maintenance process per catalog. SnapshotKeep
	// must exceed the slowest downstream consumer's lag: expiring a
	// snapshot a consumer has not read truncates its change feed.
	LakeMaintenanceEnabled bool          // LAKE_MAINTENANCE_ENABLED
	LakeMaintInterval      time.Duration // LAKE_MAINTENANCE_INTERVAL
	LakeSnapshotKeep       time.Duration // LAKE_SNAPSHOT_RETENTION
	// LakeConsumerStaleness is how long a downstream consumer may go
	// without reporting progress before expiry stops protecting its
	// cursor. Must exceed a healthy consumer's reporting gap, stay well
	// below LakeSnapshotKeep.
	LakeConsumerStaleness time.Duration // LAKE_CONSUMER_STALENESS
	// LakeWriterConnections is how many pinned DuckDB connections each sink's
	// writer round-robins bundles across, so several bundles' S3 uploads overlap.
	// >1 raises per-partition write throughput; the DuckDB pool is sized for it.
	LakeWriterConnections int // LAKE_WRITER_CONNECTIONS

	// Sink (ingest batching). Zero takes the sink package default. Exposed to
	// tune flush size vs latency — e.g. raise SinkMaxAgeHard to cut tiny Parquet
	// files on low-traffic partitions, at the cost of higher latency-to-durable.
	SinkMaxRowsPerFlush  uint64        // SINK_MAX_ROWS_PER_FLUSH
	SinkMaxBytesPerFlush uint64        // SINK_MAX_BYTES_PER_FLUSH
	SinkMinFlushBytes    uint64        // SINK_MIN_FLUSH_BYTES
	SinkMaxAge           time.Duration // SINK_MAX_AGE
	SinkMaxAgeHard       time.Duration // SINK_MAX_AGE_HARD
	SinkWorkers          uint64        // SINK_WORKERS
	SinkDrainTimeout     time.Duration // SINK_DRAIN_TIMEOUT

	// Modules.
	DecodeStreamEnabled bool

	// Validation.
	FingerprintValidation bool
	AllowableTimeSkew     time.Duration
}

// Load reads Settings from the environment, applying defaults.
func Load() (Settings, error) {
	s := Settings{
		LogLevel:               env("LOG_LEVEL", "info"),
		ConnectionAddr:         env("DIS_CONNECTION_ADDRESS", "0.0.0.0:9443"),
		AttestationAddr:        env("DIS_ATTESTATION_ADDRESS", "0.0.0.0:9442"),
		OpsAddr:                env("OPS_ADDRESS", "0.0.0.0:8080"),
		TLSCertFile:            os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:             os.Getenv("TLS_KEY_FILE"),
		TLSClientCAFile:        os.Getenv("TLS_CA_CERT_FILE"),
		TokenExchangeIssuer:    os.Getenv("TOKEN_EXCHANGE_ISSUER_URL"),
		TokenExchangeKeySetURL: os.Getenv("TOKEN_EXCHANGE_JWK_KEY_SET_URL"),
		RPCURL:                 os.Getenv("RPC_URL"),
		NATSMode:               env("NATS_MODE", "external"),
		NATSURL:                env("NATS_URL", "nats://localhost:4222"),
		NATSStoreDir:           env("NATS_STORE_DIR", "/data/nats"),
		BlobBucket:             os.Getenv("BLOB_BUCKET"),
		BlobPrefix:             env("BLOB_PREFIX", "cloudevent/blobs/"),
		LakeCatalogDSN:         os.Getenv("DUCKLAKE_CATALOG_DSN"),
		LakeDataPath:           os.Getenv("DUCKLAKE_DATA_PATH"),
		LakeTempDirectory:      os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		LakeMemoryLimit:        os.Getenv("DUCKDB_MEMORY_LIMIT"),
		LakeTargetFileSize:     env("LAKE_TARGET_FILE_SIZE", "512MB"),
		LakeParquetVersion:     env("LAKE_PARQUET_VERSION", "2"),
		LakeCompression:        env("LAKE_COMPRESSION", "snappy"),
		LakeExtensionDir:       os.Getenv("DUCKDB_EXTENSION_DIR"),
		S3Region:               os.Getenv("S3_AWS_REGION"),
		S3AccessKeyID:          os.Getenv("S3_AWS_ACCESS_KEY_ID"),
		S3SecretAccessKey:      os.Getenv("S3_AWS_SECRET_ACCESS_KEY"),
		S3Endpoint:             os.Getenv("S3_ENDPOINT"),
		S3KMSKeyID:             os.Getenv("S3_KMS_KEY_ID"),
	}

	var err error
	if s.ChainID, err = envUint("DIMO_REGISTRY_CHAIN_ID", 137); err != nil {
		return s, err
	}
	if s.VehicleNFTAddress, err = envAddress("VEHICLE_NFT_ADDRESS"); err != nil {
		return s, err
	}
	if s.AftermarketNFTAddress, err = envAddress("AFTERMARKET_NFT_ADDRESS"); err != nil {
		return s, err
	}
	if s.SyntheticNFTAddress, err = envAddress("SYNTHETIC_NFT_ADDRESS"); err != nil {
		return s, err
	}

	docLimit, err := envUint("DOCUMENT_SIZE_THRESHOLD", 1<<20)
	if err != nil {
		return s, err
	}
	s.DocumentSizeLimit = int(docLimit)

	maxBody, err := envUint("MAX_BODY_BYTES", 32<<20)
	if err != nil {
		return s, err
	}
	s.MaxBodyBytes = int64(maxBody)

	rps, err := envUint("RATE_LIMIT_RPS", 0)
	if err != nil {
		return s, err
	}
	s.RateLimitRPS = float64(rps)
	burst, err := envUint("RATE_LIMIT_BURST", 100)
	if err != nil {
		return s, err
	}
	s.RateLimitBurst = int(burst)

	replicas, err := envUint("NATS_REPLICAS", 1)
	if err != nil {
		return s, err
	}
	s.NATSReplicas = int(replicas)

	parts, err := envUint("NATS_STREAM_PARTITIONS", 1)
	if err != nil {
		return s, err
	}
	if parts < 1 || parts > 256 {
		return s, fmt.Errorf("NATS_STREAM_PARTITIONS must be 1..256, got %d", parts)
	}
	s.NATSStreamPartitions = int(parts)

	maxBytes, err := envUint("NATS_STREAM_MAX_BYTES", 0)
	if err != nil {
		return s, err
	}
	s.NATSStreamMaxBytes = int64(maxBytes)

	s.DecodeStreamEnabled = envBool("DECODESTREAM_ENABLED", true)
	s.LakeMaintenanceEnabled = envBool("LAKE_MAINTENANCE_ENABLED", false)
	s.LakeEncryptionEnabled = envBool("LAKE_ENCRYPTION_ENABLED", false)
	s.FingerprintValidation = envBool("FINGERPRINT_VALIDATION", true)

	threads, err := envUint("DUCKDB_THREADS", 0)
	if err != nil {
		return s, err
	}
	s.LakeThreads = int(threads)
	if s.LakeMaintInterval, err = envDuration("LAKE_MAINTENANCE_INTERVAL", 15*time.Minute); err != nil {
		return s, err
	}
	if s.LakeSnapshotKeep, err = envDuration("LAKE_SNAPSHOT_RETENTION", 72*time.Hour); err != nil {
		return s, err
	}
	if s.LakeConsumerStaleness, err = envDuration("LAKE_CONSUMER_STALENESS", time.Hour); err != nil {
		return s, err
	}
	writerConns, err := envUint("LAKE_WRITER_CONNECTIONS", 2)
	if err != nil {
		return s, err
	}
	s.LakeWriterConnections = max(int(writerConns), 1) // keep the pool-size math consistent with the writer

	// Sink knobs default to 0 → the sink package applies its own defaults.
	if s.SinkMaxRowsPerFlush, err = envUint("SINK_MAX_ROWS_PER_FLUSH", 0); err != nil {
		return s, err
	}
	if s.SinkMaxBytesPerFlush, err = envUint("SINK_MAX_BYTES_PER_FLUSH", 0); err != nil {
		return s, err
	}
	if s.SinkMinFlushBytes, err = envUint("SINK_MIN_FLUSH_BYTES", 0); err != nil {
		return s, err
	}
	if s.SinkMaxAge, err = envDuration("SINK_MAX_AGE", 0); err != nil {
		return s, err
	}
	if s.SinkMaxAgeHard, err = envDuration("SINK_MAX_AGE_HARD", 0); err != nil {
		return s, err
	}
	if s.SinkWorkers, err = envUint("SINK_WORKERS", 0); err != nil {
		return s, err
	}
	if s.SinkDrainTimeout, err = envDuration("SINK_DRAIN_TIMEOUT", 0); err != nil {
		return s, err
	}

	skew := env("ALLOWABLE_TIME_SKEW", "5m")
	if s.AllowableTimeSkew, err = time.ParseDuration(skew); err != nil {
		return s, fmt.Errorf("parsing ALLOWABLE_TIME_SKEW: %w", err)
	}

	if s.LakeCatalogDSN == "" {
		return s, errors.New("DUCKLAKE_CATALOG_DSN is required (PostgreSQL DSN, or a local catalog file path for single-node)")
	}
	if s.LakeDataPath == "" {
		return s, errors.New("DUCKLAKE_DATA_PATH is required (s3://bucket/prefix/ or absolute local path)")
	}
	// Paths must be unambiguous: relative values would silently resolve
	// against the working directory.
	for name, v := range map[string]string{"DUCKLAKE_DATA_PATH": s.LakeDataPath, "BLOB_BUCKET": s.BlobBucket} {
		if strings.HasPrefix(v, ".") {
			return s, fmt.Errorf("%s must not be a relative path, got %q", name, v)
		}
	}
	if s.S3KMSKeyID != "" && !isKMSKeyARN(s.S3KMSKeyID) {
		// DuckLake's S3 secret KMS_KEY_ID wants a full ARN (the stricter of the two
		// consumers), so require that form. Catches a pasted bare key-id or a typo
		// here instead of as a runtime PutObject 400 / a CrashLooping write tier.
		return s, fmt.Errorf("S3_KMS_KEY_ID must be a KMS key ARN (arn:aws:kms:...), got %q", s.S3KMSKeyID)
	}
	if err := s.validateMaintenance(); err != nil {
		return s, err
	}
	return s, nil
}

// isKMSKeyARN reports whether v looks like a KMS key or alias ARN
// (arn:<partition>:kms:...). Deliberately loose on the tail so key and alias
// ARNs across aws / aws-us-gov / aws-cn partitions all pass.
func isKMSKeyARN(v string) bool {
	return strings.HasPrefix(v, "arn:") && strings.Contains(v, ":kms:")
}

// validateMaintenance checks maintenance-tuning invariants. ConsumerStaleness
// must stay below SnapshotKeep: if a consumer may go un-reported for longer than
// snapshots are kept, the expiry floor can never protect its cursor and ranges
// it has not read are reclaimed — silent data loss for that consumer (SR-15).
func (s Settings) validateMaintenance() error {
	if s.LakeConsumerStaleness >= s.LakeSnapshotKeep {
		return fmt.Errorf("LAKE_CONSUMER_STALENESS (%s) must be less than LAKE_SNAPSHOT_RETENTION (%s)",
			s.LakeConsumerStaleness, s.LakeSnapshotKeep)
	}
	return nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", key, err)
	}
	return d, nil
}

// envAddress parses a required, non-zero Ethereum address. A zero address
// would silently mint DIDs against 0x000...0 and corrupt every subject.
func envAddress(key string) (common.Address, error) {
	v := os.Getenv(key)
	if v == "" {
		return common.Address{}, fmt.Errorf("%s is required", key)
	}
	if !common.IsHexAddress(v) {
		return common.Address{}, fmt.Errorf("%s is not a valid address: %q", key, v)
	}
	addr := common.HexToAddress(v)
	if addr == (common.Address{}) {
		return common.Address{}, fmt.Errorf("%s must not be the zero address", key)
	}
	return addr, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envUint(key string, def uint64) (uint64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", key, err)
	}
	return n, nil
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
