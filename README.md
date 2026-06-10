# din — DIMO Ingest Node

Single Go binary replacing dis + dps + parquet-processor: HTTP ingest (mTLS + JWT) → NATS JetStream WAL → hive-partitioned raw cloudevent parquet on object storage, with self-compaction and an optional decoded-stream bridge for vehicle-triggers-api.

## Storage backends

The backend is inferred from the bucket settings — no separate switch:

| Value | Backend |
|---|---|
| `my-bucket` | S3 (AWS credential chain or `S3_AWS_*` keys) |
| `/data/pipeline` or `file:///data/pipeline` | Local filesystem |

Relative paths are rejected at startup. Parquet and blob buckets pick their backends independently. Filesystem writes are crash-safe: temp file + fsync + atomic rename, so the durable-on-ack contract holds on disk like it does on S3.

## Single-node quickstart (no S3, no NATS cluster)

```bash
NATS_MODE=embedded \
NATS_STORE_DIR=/data/nats \
PARQUET_BUCKET=/data/pipeline \
BLOB_BUCKET=/data/pipeline-blobs \
COMPACTOR_ENABLED=true \
DIMO_REGISTRY_CHAIN_ID=137 \
VEHICLE_NFT_ADDRESS=0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF \
AFTERMARKET_NFT_ADDRESS=<addr> \
SYNTHETIC_NFT_ADDRESS=<addr> \
TLS_CERT_FILE=<cert> TLS_KEY_FILE=<key> TLS_CA_CERT_FILE=<ca> \
go run ./cmd/din
```

Point dq at the same root (`PARQUET_BUCKET=/data/pipeline`, `QUERY_BACKEND=duckdb`, `MATERIALIZER_ENABLED=true`) for a complete one-box pipeline. Scaling out is the same binaries with S3 bucket names and an external NATS cluster — object storage is the only shared state.

## Build and test

```bash
go build ./... && go test ./...
```

Currently builds against a local cloudevent checkout (`replace => ../cloudevent`) until the parquet encoder options branch is released.
