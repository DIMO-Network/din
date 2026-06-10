# din — DIMO Ingest Node

Single Go binary replacing dis + dps + parquet-processor: HTTP ingest (mTLS + JWT) → NATS JetStream WAL → hive-partitioned raw cloudevent parquet on object storage, with self-compaction and an optional decoded-stream bridge for vehicle-triggers-api.

## Clone layout (required until cloudevent is released)

din builds against a local cloudevent checkout via a `replace` directive. Clone them as siblings:

```bash
git clone git@github.com:DIMO-Network/cloudevent.git && git -C cloudevent checkout feat/parquet-sort-zstd-bloom
git clone git@github.com:DIMO-Network/din.git
cd din && go build ./... && go test ./...
```

## Storage backends

The backend is inferred from the bucket settings — no separate switch:

| Value | Backend |
|---|---|
| `my-bucket` | S3 (AWS credential chain or `S3_AWS_*` keys) |
| `/data/pipeline` or `file:///data/pipeline` | Local filesystem |

Relative paths are rejected at startup. Parquet and blob buckets pick their backends independently. Filesystem writes are crash-safe: temp file + fsync + atomic rename, so the durable-on-ack contract holds on disk like it does on S3.

## Run it locally (verified single-node quickstart)

No S3, no NATS cluster, no Kubernetes. You need a TLS keypair for the mTLS port (self-signed is fine locally — the cert CN plays the connection-license role):

```bash
D=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout $D/key.pem -out $D/cert.pem -days 365 -nodes -subj "/CN=0xYourConnLicense"

NATS_MODE=embedded NATS_STORE_DIR=$D/nats \
PARQUET_BUCKET=$D/pipeline BLOB_BUCKET=$D/pipeline-blobs \
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

A 200 means the event is JetStream-acked; within ~1 minute (sink `MaxAge`) the bundle lands at `$D/pipeline/raw/type=dimo.status/date=YYYY-MM-DD/ingest-*.parquet`. Point dq at the same root (`PARQUET_BUCKET=$D/pipeline`) for the query side.

Scaling out is the same binary with S3 bucket names and an external NATS cluster (`NATS_MODE=external NATS_URL=...`) — object storage is the only shared state.

### Environment reference

| Env | Required | Notes |
|---|---|---|
| `PARQUET_BUCKET` | yes | S3 bucket or absolute local path |
| `BLOB_BUCKET` | yes | bucket/path for >1MB payloads |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` / `TLS_CA_CERT_FILE` | yes | mTLS device port :9443 |
| `VEHICLE_NFT_ADDRESS` / `AFTERMARKET_NFT_ADDRESS` / `SYNTHETIC_NFT_ADDRESS` | yes | DID validation (values above = Polygon mainnet) |
| `RPC_URL` | yes | attestation ERC-1271 checks |
| `TOKEN_EXCHANGE_ISSUER` / `TOKEN_EXCHANGE_KEY_SET_URL` | yes | JWT auth on :9442 (dev dex shown above; prod: `https://auth.dimo.zone`) |
| `NATS_MODE` | no | `embedded` (single-node) or `external` (default) + `NATS_URL` |
| `COMPACTOR_ENABLED` / `DECODESTREAM_ENABLED` | no | both default true |
| `DECODED_PREFIX` | no | default `decoded/v1/` — must match dq's |
| `DIMO_REGISTRY_CHAIN_ID` | no | default 137 |

## Tests

```bash
go test ./...                                            # unit + e2e (no Docker needed)
go test ./tests/ -run TestIngestPerformance -v -perf     # throughput gates
```
