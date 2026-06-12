# din — DIMO Ingest Node

Single Go binary replacing dis + dps + parquet-processor: HTTP ingest (mTLS + JWT) → NATS JetStream WAL → [DuckLake](https://ducklake.select) `raw_events` table (partitioned parquet on object storage, tracked by a SQL catalog), with built-in lake maintenance and an optional decoded-stream bridge for vehicle-triggers-api.

## Clone layout (required until cloudevent is released)

din builds against a local cloudevent checkout via a `replace` directive. Clone them as siblings:

```bash
git clone git@github.com:DIMO-Network/cloudevent.git
git clone git@github.com:DIMO-Network/din.git
cd din && go build ./... && go test ./...
```

Building needs CGO (the embedded DuckDB): a C/C++ toolchain must be present (`xcode-select --install` on macOS, `build-essential` on Debian). `duckdb-go` ships prebuilt static DuckDB bindings, so nothing else to install.

## Storage layout

Raw events live in a DuckLake: parquet files under `LAKE_DATA_PATH`, tracked by the catalog database at `LAKE_CATALOG_DSN`. The catalog is the source of truth — which files exist, which snapshot they belong to, partition/sort metadata. Readers (dq) attach the same catalog read-only; never enumerate the data path directly.

| Setting | Value | Meaning |
|---|---|---|
| `LAKE_CATALOG_DSN` | `postgres://…` | production: multi-process catalog |
| | `/data/lake/meta.ducklake` | single-node/dev: local file catalog |
| `LAKE_DATA_PATH` | `s3://bucket/lake/` or `/data/lake/data` | where parquet lands; **immutable once the catalog exists** |

The table is partitioned by `(type, day(time))` and sorted by `(subject, time)`. Bundles smaller than DuckLake's inlining threshold are stored as catalog rows and materialized to parquet by maintenance.

Blobs (>1MB payloads) keep their own bucket: `BLOB_BUCKET` is an S3 bucket name or absolute local path (filesystem writes are crash-safe: temp file + fsync + atomic rename).

### Maintenance

`ducklake_merge_adjacent_files` + snapshot expiry + file cleanup replace the old compactor, manifests, and watermark protocol. Merging preserves the snapshot change feed, so it needs no coordination with readers; the one contract left is `LAKE_SNAPSHOT_RETENTION` (default 72h), which must exceed the slowest consumer's lag.

Run **exactly one** maintenance process per catalog:

- single-node: `LAKE_MAINTENANCE_ENABLED=true` in the service (runs every `LAKE_MAINTENANCE_INTERVAL`, default 15m), or
- multi-replica: the chart's `maintenance.enabled` CronJob runs `din maintain` (one cycle per invocation).

## Run it locally (verified single-node quickstart)

No S3, no Postgres, no NATS cluster, no Kubernetes. You need a TLS keypair for the mTLS port (self-signed is fine locally — the cert CN plays the connection-license role):

```bash
D=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout $D/key.pem -out $D/cert.pem -days 365 -nodes -subj "/CN=0xYourConnLicense"

NATS_MODE=embedded NATS_STORE_DIR=$D/nats \
LAKE_CATALOG_DSN=$D/lake/meta.ducklake LAKE_DATA_PATH=$D/lake/data \
LAKE_MAINTENANCE_ENABLED=true \
BLOB_BUCKET=$D/pipeline-blobs \
TLS_CERT_FILE=$D/cert.pem TLS_KEY_FILE=$D/key.pem TLS_CA_CERT_FILE=$D/cert.pem \
RPC_URL=https://polygon-rpc.com \
TOKEN_EXCHANGE_ISSUER=https://auth.dev.dimo.zone \
TOKEN_EXCHANGE_KEY_SET_URL=https://auth.dev.dimo.zone/keys \
VEHICLE_NFT_ADDRESS=0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF \
AFTERMARKET_NFT_ADDRESS=0x9c94C395cBcBDe662235E0A9d3bB87Ad708561BA \
SYNTHETIC_NFT_ADDRESS=0x4804e8D1661cd1a1e5dDdE1ff458A7f878c0aC6D \
go run ./cmd/din
```

You should see `din started` with `connection: 0.0.0.0:9443`, `attestation: 0.0.0.0:9442`. Send a device payload over the mTLS port (the same cert doubles as the client cert):

```bash
SUBJECT="did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -sk --cert $D/cert.pem --key $D/key.pem https://localhost:9443 \
  -H 'Content-Type: application/json' -d '{
    "specversion":"1.0","type":"dimo.status","id":"local-1",
    "source":"0xYourConnLicense","subject":"'$SUBJECT'","producer":"'$SUBJECT'",
    "time":"'$NOW'","dataversion":"default/v1.0",
    "data":{"signals":[{"name":"speed","timestamp":"'$NOW'","value":42.5}]}}'
```

A 200 means the event is JetStream-acked; within ~1 minute (sink `MaxAge`) the row is committed to the lake. Query it with the DuckDB CLI:

```bash
duckdb -c "INSTALL ducklake; ATTACH 'ducklake:$D/lake/meta.ducklake' AS lake (READ_ONLY); SELECT * FROM lake.raw_events;"
```

Scaling out is the same binary with a PostgreSQL `LAKE_CATALOG_DSN`, an S3 `LAKE_DATA_PATH`, and an external NATS cluster (`NATS_MODE=external NATS_URL=...`) — the catalog and object storage are the only shared state.

### Environment reference

| Env | Required | Notes |
|---|---|---|
| `LAKE_CATALOG_DSN` | yes | PostgreSQL DSN, or local catalog file path (single-node) |
| `LAKE_DATA_PATH` | yes | `s3://bucket/prefix/` or absolute local path |
| `BLOB_BUCKET` | yes | bucket/path for >1MB payloads |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` / `TLS_CA_CERT_FILE` | yes | mTLS device port :9443 |
| `VEHICLE_NFT_ADDRESS` / `AFTERMARKET_NFT_ADDRESS` / `SYNTHETIC_NFT_ADDRESS` | yes | DID validation (values above = Polygon mainnet) |
| `RPC_URL` | yes | attestation ERC-1271 checks |
| `TOKEN_EXCHANGE_ISSUER` / `TOKEN_EXCHANGE_KEY_SET_URL` | yes | JWT auth on :9442 (dev dex shown above; prod: `https://auth.dimo.zone`) |
| `NATS_MODE` | no | `embedded` (single-node) or `external` (default) + `NATS_URL` |
| `LAKE_MAINTENANCE_ENABLED` | no | default false; one maintenance process per catalog |
| `LAKE_MAINTENANCE_INTERVAL` / `LAKE_SNAPSHOT_RETENTION` | no | defaults 15m / 72h; retention must exceed consumer lag |
| `LAKE_MEMORY_LIMIT` / `LAKE_THREADS` / `LAKE_TARGET_FILE_SIZE` | no | DuckDB/DuckLake tuning (e.g. `1GB`, `4`, `512MB`) |
| `LAKE_EXTENSION_DIR` | no | pre-baked DuckDB extensions (set in the container image) |
| `DECODESTREAM_ENABLED` | no | default true |
| `DIMO_REGISTRY_CHAIN_ID` | no | default 137 |

### Subcommands

```bash
din maintain                          # one lake maintenance cycle (CronJob entrypoint)
din install-duckdb-extensions <dir>   # bake extensions into the image at build time
```

## Tests

```bash
go test ./...                                            # unit + e2e (no Docker needed)
go test ./tests/ -run TestIngestPerformance -v -perf     # throughput gates
```
