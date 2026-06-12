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
	Environment string
	LogLevel    string

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

	// Storage.
	ParquetBucket     string
	BlobBucket        string
	BlobPrefix        string
	DecodedPrefix     string // must match dq materializer's DECODED_PREFIX
	DocumentSizeLimit int    // DOCUMENT_SIZE_THRESHOLD
	S3Region          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Endpoint        string

	// Modules.
	DecodeStreamEnabled bool
	CompactorEnabled    bool

	// Validation.
	FingerprintValidation bool
	AllowableTimeSkew     time.Duration
}

// Load reads Settings from the environment, applying defaults.
func Load() (Settings, error) {
	s := Settings{
		Environment:            env("ENVIRONMENT", "dev"),
		LogLevel:               env("LOG_LEVEL", "info"),
		ConnectionAddr:         env("DIS_CONNECTION_ADDRESS", "0.0.0.0:9443"),
		AttestationAddr:        env("DIS_ATTESTATION_ADDRESS", "0.0.0.0:9442"),
		OpsAddr:                env("OPS_ADDRESS", "0.0.0.0:8080"),
		TLSCertFile:            os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:             os.Getenv("TLS_KEY_FILE"),
		TLSClientCAFile:        os.Getenv("TLS_CA_CERT_FILE"),
		TokenExchangeIssuer:    os.Getenv("TOKEN_EXCHANGE_ISSUER"),
		TokenExchangeKeySetURL: os.Getenv("TOKEN_EXCHANGE_KEY_SET_URL"),
		RPCURL:                 os.Getenv("RPC_URL"),
		NATSMode:               env("NATS_MODE", "external"),
		NATSURL:                env("NATS_URL", "nats://localhost:4222"),
		NATSStoreDir:           env("NATS_STORE_DIR", "/data/nats"),
		ParquetBucket:          os.Getenv("PARQUET_BUCKET"),
		BlobBucket:             os.Getenv("BLOB_BUCKET"),
		BlobPrefix:             env("BLOB_PREFIX", "cloudevent/blobs/"),
		DecodedPrefix:          env("DECODED_PREFIX", "decoded/v1/"),
		S3Region:               os.Getenv("S3_AWS_REGION"),
		S3AccessKeyID:          os.Getenv("S3_AWS_ACCESS_KEY_ID"),
		S3SecretAccessKey:      os.Getenv("S3_AWS_SECRET_ACCESS_KEY"),
		S3Endpoint:             os.Getenv("S3_ENDPOINT"),
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

	s.DecodeStreamEnabled = envBool("DECODESTREAM_ENABLED", true)
	s.CompactorEnabled = envBool("COMPACTOR_ENABLED", true)
	s.FingerprintValidation = envBool("FINGERPRINT_VALIDATION", true)

	skew := env("ALLOWABLE_TIME_SKEW", "5m")
	if s.AllowableTimeSkew, err = time.ParseDuration(skew); err != nil {
		return s, fmt.Errorf("parsing ALLOWABLE_TIME_SKEW: %w", err)
	}

	if s.ParquetBucket == "" {
		return s, errors.New("PARQUET_BUCKET is required (S3 bucket name or absolute local path)")
	}
	// Buckets are either S3 names or absolute local paths; relative paths
	// would silently resolve against the working directory (and dq's local
	// detection would miss them), so reject them here.
	for name, v := range map[string]string{"PARQUET_BUCKET": s.ParquetBucket, "BLOB_BUCKET": s.BlobBucket} {
		if strings.HasPrefix(v, ".") {
			return s, fmt.Errorf("%s must be an S3 bucket name or absolute path, got relative path %q", name, v)
		}
	}
	return s, nil
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
