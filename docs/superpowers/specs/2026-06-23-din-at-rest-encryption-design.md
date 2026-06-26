# din — At-Rest Encryption Design

**Status:** Implemented on branch `feat/at-rest-encryption`
**Date:** 2026-06-23 (design), 2026-06-26 (implemented)
**Scope:** din ingest write path → DuckLake-on-S3, the blob store, and the Postgres catalog

## Problem

din writes vehicle telemetry (`raw_events`) to S3 as DuckLake parquet and keeps
catalog metadata in Postgres. None of it is encrypted at rest:

- S3 parquet bundles are plaintext (zstd is compression, not encryption).
- The blob store for payloads over 1MB, written through `internal/s3client`, is plaintext.
- The Postgres catalog is plaintext, and the DuckLake S3 secret (access key and
  secret key) sits in the DuckDB secret catalog in the clear.

So a leaked bucket, a stolen backup, or a compromised storage provider hands
over readable telemetry. That's what we're fixing.

## Goal and threat model

We're protecting against leaked storage. If a bucket leaks, a backup walks out
the door, or the storage provider is compromised, the data should be unreadable.
The DIMO platform itself keeps reading normally: dq still materializes,
telemetry-api still serves, SACD data-sharing still works. This is not a
user-sovereign or zero-knowledge design, and it isn't trying to be.

### Decisions locked during brainstorming

| Fork | Decision | Consequence |
|---|---|---|
| Threat model | At-rest / leaked storage | Platform keeps transparent reads; no per-user keys. |
| Key custody | Layered: DuckLake `ENCRYPTED` plus SSE-KMS | A leaked bucket on its own is useless ciphertext; KMS adds a storage-layer defense and an audit trail. The PG catalog becomes the trust root. |
| Migration | None — greenfield | din and dq aren't deployed anywhere yet, so there's no plaintext backlog. Encryption is on from the first write. No phased rollout, no compaction migration, no backlog gauge. |

### Out of scope

Per-vehicle or PIN-derived user-sovereign keys, client-side encryption, and
per-column keys. The appendix explains why. The short version: they fight din's
many-subjects-per-bundle packing and the platform's need to read the data, and a
short PIN on its own is trivial to brute-force offline.

## Architecture

Two encryption layers that don't depend on each other, plus a fix for the blob
path that neither layer would otherwise cover, plus hardening for the catalog
since it's about to hold the keys.

```
device → mTLS → din → NATS → sink → DuckLake appender ─┐
                                                        ├─► S3 parquet  [A: DuckLake ENCRYPTED] + [B: SSE-KMS]
                              payload >1MB → s3client ──┘─► S3 blob     [B: SSE-KMS]   ← the gap today
catalog (PG): per-file AES-GCM keys + S3 secret (plaintext today)      [C: harden — TLS, at-rest, credential_chain]
```

### Layer A — DuckLake `ENCRYPTED`

This is the layer that makes a leaked bucket worthless on its own.

Add the `ENCRYPTED` flag to the DuckLake ATTACH in `internal/lake/lake.go` (the
`ATTACH IF NOT EXISTS 'ducklake:…' AS lake (DATA_PATH '…')` around line 135).
From then on every new data file gets its own parquet AES-GCM key, and DuckLake
stores that key in the catalog (`ducklake_data_file.encryption_key`, so in
Postgres). Reads stay transparent because DuckLake pulls the key back out of the
catalog automatically.

Gate it behind `LAKE_ENCRYPTION_ENABLED`, defaulting off in code and on in the
prod chart values, the same way we rolled `LAKE_PARQUET_VERSION` and
`LAKE_TARGET_FILE_SIZE`. It's applied at attach time, on every boot. The payoff:
S3 alone is ciphertext, and you need catalog access to decrypt.

A few things to write into the code and runbook so nobody trips on them later:

- Crypto adds roughly 2.5x to parquet read and write. Measure it on a real din
  bundle before flipping prod.
- The crypto provider needs to be OpenSSL (httpfs loads it). The bundled MbedTLS
  path can't write encrypted files.
- Confirm the pinned DuckDB 1.5.3 is past advisory GHSA-vmp8-hg63-v2hp.
- DuckDB's own docs say its encryption isn't NIST-validated yet. That's fine
  here because it's one layer on top of KMS, which is FIPS-capable, but it's
  worth saying out loud.

### Layer B — S3 SSE-KMS

This is the storage-layer defense and the CloudTrail audit trail. There are two
separate paths writing to S3, and both need it.

The parquet writes go through httpfs, so add `KMS_KEY_ID` to the
`CREATE OR REPLACE SECRET lake_s3` statement in `internal/lake/lake.go`
(`s3SecretSQL`, around lines 408–437). That's the only SSE knob httpfs gives us.

The blob path is the gap. `internal/s3client/s3client.go` `PutObject` (around
lines 92–108) writes the over-1MB payloads with no SSE at all, and Layer A
doesn't reach the blob bucket. Set `ServerSideEncryption: aws:kms`,
`SSEKMSKeyId`, and `BucketKeyEnabled: true` on the PutObjectInput. Skip this and
the large payloads stay plaintext.

On the infra side, turn on SSE-KMS as the bucket default and enable S3 Bucket
Keys to cut KMS request volume and cost when object counts get high.

Config: `S3_KMS_KEY_ID` for the key ARN, `S3_SSE_MODE` (`none` or `aws:kms`).

### Layer C — harden the Postgres catalog

Once Layer A is on, the per-file keys live in PG (`ducklake_data_file.encryption_key`),
which means the catalog is the trust root: anyone who can read it can decrypt
every file. So we lock it down.

- Put the catalog DSN on `sslmode=verify-full`. There's no TLS in the ATTACH today.
- In prod, switch the S3 secret to `PROVIDER credential_chain` (IRSA) so we stop
  persisting static S3 keys in the secret catalog. We're storing them in the
  clear today, and din already supports the credential_chain provider.
- Require managed at-rest encryption on the PG instance (RDS or Cloud SQL
  storage encryption) and restrict catalog access by network and IAM.
- Fold all of this into `docs/catalog-postgres-maintenance.md`.

One consequence is big enough to call out on its own: the PG catalog backup is
now data-critical, not just metadata. If the `encryption_key` rows are lost and
there's no backup, the encrypted parquet is gone for good. Catalog backups stop
being a convenience and become the thing standing between you and permanent data
loss.

## Migration: none (greenfield)

din and dq haven't been deployed anywhere, so there's no plaintext backlog to
migrate. Encryption is on from the first write. That kills the whole phased-rollout
and natural-compaction story the earlier draft carried: there's nothing to drain,
so no `din_lake_unencrypted_files` gauge either. Set `LAKE_ENCRYPTION_ENABLED`
before the first catalog is created and leave it on. The flag stays in the code as
a safety valve and to keep the existing tests (which inspect raw parquet) running
unencrypted, but operationally it's on everywhere from day one.

## Testing

din has a `./tests/` integration package, so run `go test ./...`, not just
`./internal/... ./cmd/...`.

What's implemented:

- `TestWriter_Encrypted` (`internal/lake/encryption_test.go`): writes a bundle
  with `Encrypted: true`, confirms the lake reads it back through the catalog
  (3000 rows), and confirms the raw parquet file on disk does *not* read as plain
  parquet (`read_parquet` on it errors). A non-encrypted control proves the error
  is the encryption, not an unrelated read failure. This is the at-rest proof.
- `TestPutObject_SSEKMS` / `TestPutObject_NoKMS` (`internal/s3client`): a
  configured `KMSKeyID` puts SSE-KMS + Bucket Key on the PutObjectInput; an empty
  one leaves the SSE fields unset so the bucket default applies. Uses the existing
  `fakeAPI`, no live MinIO needed (MinIO isn't AWS KMS).
- Existing `TestOpen_BootstrapIdempotent` still passes, so `ENCRYPTED` on the
  ATTACH doesn't churn snapshots.

Still worth doing before heavy prod load: measure the ~2.5x parquet crypto
overhead against the sink flush knobs (`SINK_MIN_FLUSH_BYTES`, `SINK_MAX_AGE_HARD`)
on a representative bundle.

## Rollout

Greenfield, so there's no phased cutover — `LAKE_ENCRYPTION_ENABLED` is on in both
`values.yaml` and `values-prod.yaml` from the first deploy.

`ENCRYPTED` is sticky to the catalog, not a runtime toggle. Verified empirically
against DuckDB 1.5.x:

- Turning it **on** for a catalog that was first created plaintext fails the
  ATTACH hard — `Failed to set encryption - the database is not encrypted but we
  requested an encrypted database` — so the pod CrashLoops. The flag must be set
  before the catalog is first created.
- Turning it **off** on a catalog created encrypted is a silent no-op: DuckLake
  keeps writing encrypted files. You cannot roll back to plaintext by flipping the
  flag. (Upside: the maintainer can never silently downgrade compacted files to
  plaintext, even if it somehow attached without the flag — the catalog forces
  encryption.)

So real rollback is a catalog migration (fresh unencrypted catalog + re-ingest),
not a flag flip. It's moot while nothing is deployed, but don't carry a
"flip-to-roll-back" assumption into any environment that already has a catalog —
flipping the base-chart default to `true` over a pre-existing plaintext catalog
CrashLoops every pod.

`S3_KMS_KEY_ID` is empty in the charts by default (SSE-KMS inert, bucket-default
SSE applies). See "Before enabling S3_KMS_KEY_ID" below — that path was reviewed
and deliberately left as-is, with its preconditions deferred to ops.

### Risks

- KMS sits on the write path. If KMS goes down, SSE-KMS PutObjects fail and the
  sink can stall. Bucket Keys cut the call volume and the existing s3client
  retry and timeout help, but verify the failure behavior before prod.
- Losing the keys means losing the data. See the Layer C note.
- DuckDB's encryption isn't NIST-validated. The KMS layer underneath it is
  FIPS-capable, which is why this is acceptable.

## Coverage (gaps the review surfaced, now closed)

The first review left four gaps. All are addressed; what remains is stated plainly.

- **Blobs — FIXED (client-side encryption).** Layer A makes parquet proof against a
  raw-object leak *and* a leaked S3 read credential (decrypt needs the catalog).
  Blobs aren't in the lake, so they used to get only S3 SSE — transparent to an S3
  GET credential. Now `internal/blobcrypt` seals blob payloads with AES-256-GCM
  before upload (key in the pod secret, never the bucket), bringing blobs to parquet
  parity. Gated by `BLOB_ENCRYPTION_KEY`; empty leaves the old SSE-only behavior.
- **Reader (dq) — FIXED (separate PR).** dq reads the encrypted catalog
  transparently and decrypts blobs with a byte-identical `blobcrypt` (pinned by a
  shared golden vector). Its materializer attaches `ENCRYPTED` so it can't create a
  plaintext catalog din would reject; read-only query pods read transparently. See
  the dq branch `feat/at-rest-encryption`.
- **Prod s3:// path — FIXED (tested + verified).** `TestMinIO_EncryptedLake_RoundTrip`
  drives the real `s3://` → httpfs + CREATE SECRET + ATTACH `ENCRYPTED` path against
  a live MinIO: the lake reads its data back, but the parquet object in the bucket
  won't read as plain parquet. Passes locally where the `minio` binary is present.
- **Backfill — flagged.** Legacy DIS bundles added via `ducklake_add_data_files`
  into an encrypted catalog stay unencrypted at their source path (verified:
  registration + read-back work, null `encryption_key`) until the maintainer
  compacts them. `Backfill` now logs a warning when the catalog is encrypted. Moot
  while nothing is backfilled; inherent to register-by-reference otherwise.

Residual (unchanged): a leaked S3 *write* credential can still tamper (encryption is
confidentiality, not write-authz); KMS-on-the-DuckLake-secret remains deferred (see
below); the PG catalog backup is data-critical.

## Resolved during implementation

- One shared `S3_KMS_KEY_ID` for both parquet and blob writes (not separate keys).
  Simpler, and there's no threat-model reason to split them here.
- `LAKE_ENCRYPTION_ENABLED` defaults off in code (so the parquet-inspecting unit
  tests stay unencrypted) and is set on in both chart values.
- The SSE-KMS test uses the in-package `fakeAPI` to assert the PutObjectInput
  headers — no MinIO/KES needed.

## Before enabling `S3_KMS_KEY_ID` (reviewed, deferred to ops)

The KMS_KEY_ID on the DuckLake secret was reviewed and left as-is: it's
config-guarded and empty by default, so it can't break the default deploy. But
the parquet path carries real landmines that must be cleared before any operator
sets the key in a real environment. The blob path (s3client) is native AWS SDK
and unaffected by these.

- **Boot CrashLoop risk.** The `CREATE OR REPLACE SECRET` carrying `KMS_KEY_ID`
  runs in the bootstrap loop *outside* `retryCatalog`. If the pinned DuckDB build
  rejects the option, `Open()` returns an error and every pod (ingest + the
  maintenance Deployment) CrashLoops the moment the key is populated. Validate the
  option against the exact DuckDB build first; there is no SSE-S3 fallback.
- **REGION + PROVIDER.** DuckDB's documented SSE-KMS recipe carries `REGION` and
  uses the credential chain. din emits `KMS_KEY_ID` under `PROVIDER config` and
  omits `REGION` when `S3Region` is unset — set `S3_AWS_REGION` whenever the KMS
  key is set, or expect runtime 400s on the Parquet flush.
- **Custom endpoint incompatibility.** Don't set `S3_KMS_KEY_ID` together with a
  MinIO/LocalStack `S3_ENDPOINT` — httpfs will send `aws:kms` headers a non-AWS
  endpoint can't honor and writes wedge.
- **Redundancy + read coupling.** On the parquet objects this is a *second*
  at-rest layer on top of Layer A (already AES-GCM encrypted), and it makes every
  read depend on the KMS key staying enabled and the reader holding `kms:Decrypt`.
  Consider covering the parquet storage layer with S3 bucket-default SSE instead,
  and reserving `S3_KMS_KEY_ID` for the blob path that has no other at-rest layer.

Other ops items:

- Set the real KMS key ARN in `values-prod.yaml` (or via secret/overlay).
- Put `sslmode=verify-full` on the catalog DSN secret and confirm managed PG
  at-rest encryption is on — the catalog is the trust root now.
- Confirm the prod container image links OpenSSL (DuckDB's encrypted-write path
  needs it; the MbedTLS build can't write encrypted files). Local tests encrypt
  fine, so the dev toolchain already links it.
- Consider validating `S3_KMS_KEY_ID` looks like a KMS ARN at config load so a
  typo fails fast instead of as a runtime PutObject 400.

## Appendix — why the PIN / user-sovereign idea was dropped

The original ask floated a per-user PIN that "encrypts the data to that user."
Two facts about how din is built ruled it out for this round.

First, there's no native per-vehicle key to hang it on. DuckLake makes one key
per file, and din deliberately packs many vehicle DIDs into each ~128MB bundle.
Real per-user crypto would mean either one file per vehicle, which destroys the
bundle packing din was tuned for and brings back tiny-file churn, or a custom
app-level envelope on the payload columns. DuckDB's parquet encryption can't
help either, since it has no per-column keys (`column_keys` is unimplemented).

Second, a key only the owner holds locks the platform out. If decryption needs
the owner's PIN, dq can't materialize, telemetry-api can't serve, and SACD
data-sharing breaks. A workable user-factor model is an envelope, where the PIN
is one wrapping factor (stretched with Argon2id), KMS is another, and authorized
grants re-wrap the data key. PIN-only doesn't work: a 4-to-6 digit PIN is easy
to grind offline, so it has to be Argon2id-stretched and either KMS-wrapped or
rate-limited server-side to mean anything.

It's reasonable future work as a second phase over a sensitive subset like
precise location. It's just not part of the at-rest goal we picked here.
