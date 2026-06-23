# din — At-Rest Encryption Design

**Status:** Plan (approved as design; not yet scheduled for implementation)
**Date:** 2026-06-23
**Scope:** din ingest write path → DuckLake-on-S3 + blob store + Postgres catalog

---

## Problem

din writes vehicle-telemetry `raw_events` to S3 as DuckLake parquet and stores
catalog metadata in Postgres. Today there is **zero at-rest encryption** at any
stage:

- S3 parquet bundles: plaintext (only zstd compression).
- Blob store (payloads >1MB, written via `internal/s3client`): plaintext.
- Postgres catalog: plaintext metadata; the DuckLake S3 secret (access key +
  secret key) is **stored plaintext** in the DuckDB secret catalog.
- No KMS integration, no key management, no SSE on any PutObject.

A leaked S3 bucket, stolen backup, or compromised storage provider currently
yields fully readable telemetry.

## Goal & threat model

**Protect against at-rest / leaked storage.** A leaked S3 bucket, stolen
backup, or compromised storage provider must yield unreadable data. The DIMO
platform (dq materializer, telemetry-api, SACD data-sharing) must keep reading
normally — this is *not* a user-sovereign / zero-knowledge design.

### Decisions locked during brainstorming

| Fork | Decision | Consequence |
|---|---|---|
| Threat model | **At-rest / leaked storage** | Platform keeps transparent read; no per-user keys. |
| Key custody | **Layered: DuckLake `ENCRYPTED` + SSE-KMS** | Leaked bucket alone is useless ciphertext; KMS adds storage-substrate defense + audit. PG catalog becomes the trust root. |
| Migration | **Forward + natural compaction** | New writes encrypted at cutover; maintainer compaction rewrites recent partitions encrypted; cold partitions age out via retention. No expensive one-time rewrite. |

### Explicitly out of scope

Per-vehicle / PIN-derived **user-sovereign** keys, client-side encryption (CSE),
and per-column keys. Rationale captured in the appendix — they conflict with
din's many-subjects-per-bundle packing and with the platform's transparent-read
requirement, and a low-entropy PIN alone is offline-brute-forceable.

---

## Architecture

Two independent at-rest layers, plus closing the blob-path gap, plus hardening
the new trust root (PG).

```
device → mTLS → din → NATS → sink → DuckLake appender ─┐
                                                        ├─► S3 parquet  [A: DuckLake ENCRYPTED] + [B: SSE-KMS]
                              payload >1MB → s3client ──┘─► S3 blob     [B: SSE-KMS]   ← the hole today
catalog (PG): per-file AES-GCM keys + S3 secret (plaintext today)      [C: harden — TLS, at-rest, credential_chain]
```

### Layer A — DuckLake `ENCRYPTED` (the strong "untrusted bucket" property)

- Add the `ENCRYPTED` flag to the DuckLake ATTACH in `internal/lake/lake.go`
  (the `ATTACH IF NOT EXISTS 'ducklake:…' AS lake (DATA_PATH '…')` at ~line 135).
- Every newly written data file gets a fresh parquet AES-GCM key. DuckLake
  stores it in the catalog (`ducklake_data_file.encryption_key`, i.e. in
  Postgres). Reads are transparent — DuckLake fetches the key from the catalog.
- Env-gate `LAKE_ENCRYPTION_ENABLED` (default **off** in code, **on** in prod
  chart values — same rollout pattern as `LAKE_PARQUET_VERSION` /
  `LAKE_TARGET_FILE_SIZE`). Applied at attach, every boot.
- **Effect:** a leaked S3 bucket alone is useless ciphertext; decryption
  requires PG catalog access.

**Caveats to document in code/runbook:**
- ~2.5× parquet read/write crypto overhead (measure on a representative din
  bundle before prod cutover).
- Confirm **OpenSSL** is the crypto provider (httpfs pulls it in); the bundled
  MbedTLS write path is disabled.
- Verify the pinned DuckDB 1.5.3 is past advisory **GHSA-vmp8-hg63-v2hp**.
- DuckDB states its native encryption "does not yet meet official NIST
  requirements" — acceptable here as *one layer* alongside KMS (which is
  FIPS-capable), but recorded explicitly.

### Layer B — S3 SSE-KMS (storage substrate + CloudTrail audit) — both write paths

Two distinct paths write to S3; both must be covered.

1. **DuckLake parquet writes (via httpfs):** add `KMS_KEY_ID` to the
   `CREATE OR REPLACE SECRET lake_s3` statement in `internal/lake/lake.go`
   (`s3SecretSQL`, ~lines 408–437). This is the only SSE knob httpfs exposes.
2. **Blob path — the gap:** `internal/s3client/s3client.go` `PutObject`
   (~lines 92–108) writes payloads >1MB with **no SSE**, and DuckLake
   `ENCRYPTED` does **not** reach the blob bucket. Add to the PutObjectInput:
   `ServerSideEncryption: aws:kms`, `SSEKMSKeyId`, `BucketKeyEnabled: true`.
   Without this, large payloads remain plaintext.
3. **Bucket-level (ops/IaC):** enable SSE-KMS as bucket default + **S3 Bucket
   Keys** to cut KMS request volume/cost on high object counts.

**Env:** `S3_KMS_KEY_ID` (shared key ARN), `S3_SSE_MODE` (`none` | `aws:kms`).

### Layer C — harden the Postgres catalog (now the trust root)

With `ENCRYPTED`, per-file keys live in PG `ducklake_data_file.encryption_key` —
anyone with catalog read access can decrypt every file. Therefore:

- Catalog DSN → `sslmode=verify-full` (no TLS visible in the ATTACH today).
- Prod: switch the S3 secret to `PROVIDER credential_chain` (IRSA) so **no
  static S3 keys are persisted plaintext** in the secret catalog (they are
  today). din already supports the credential_chain provider.
- Require managed PG at-rest encryption (RDS/Cloud SQL storage encryption) and
  network/IAM-restricted catalog access.
- Extend `docs/catalog-postgres-maintenance.md` with the above.

**Operational consequence (critical):** the PG catalog backup becomes
**data-critical**, not just metadata. Lose the `encryption_key` rows with no PG
backup → the encrypted parquet is **unrecoverable**. Backup discipline is now a
data-loss concern.

---

## Migration — forward + natural compaction

- New writes are encrypted from cutover.
- The maintainer's existing merge/compaction cycles rewrite recent partitions →
  those files become encrypted on rewrite.
- Cold partitions that never re-merge stay plaintext until retention deletes
  them. Mixed-state reads work transparently (DuckLake reads each file per its
  own key presence).
- **Observability:** add a `din_lake_unencrypted_files` gauge (count of
  `ducklake_data_file` rows with null/empty `encryption_key`) to watch the
  backlog drain.

---

## Testing

> din has a `./tests/` integration package — run `go test ./...`, not just
> `./internal/... ./cmd/...`.

- ATTACH + `ENCRYPTED` → write a bundle → assert the raw S3 object is **not** a
  valid plaintext parquet, but DuckLake reads it back correctly, and
  `ducklake_data_file.encryption_key` is populated.
- **Mixed-state read:** one plaintext file + one encrypted file in the same
  table both read correctly.
- s3client SSE-KMS: assert the PutObjectInput carries the SSE params. MinIO ≠
  AWS KMS, so the `s3_minio` path likely needs a mock/fake for the SSE
  assertion (flag during implementation).
- Boot idempotency: `ENCRYPTED` on the ATTACH must not churn snapshots
  (cf. `TestOpen_BootstrapIdempotent`).
- Perf: measure the ~2.5× crypto overhead on the write hot path vs the sink
  flush knobs (`MinFlushBytes`, `MaxAgeHard`).

---

## Rollout — two phases, both env-gated for instant rollback

1. **SSE-KMS first** (zero-risk): bucket default + s3client PutObject params +
   DuckLake secret `KMS_KEY_ID`. Covers everything (including blobs)
   server-side immediately.
2. **DuckLake `ENCRYPTED` + PG hardening** (the strong property). Rollback =
   set the flag off → new files write plaintext again; previously-encrypted
   files stay readable as long as their keys remain in the catalog. **Never
   purge `encryption_key` rows.**

### Risks

- **KMS on the write path:** a KMS outage makes SSE-KMS PutObjects fail →
  potential sink stall. S3 Bucket Keys (fewer KMS calls) + the existing
  s3client retry/timeout mitigate; verify failure behavior during
  implementation.
- **Key loss = data loss:** see Layer C operational consequence.
- **NIST caveat:** DuckDB native encryption is not NIST-validated; mitigated by
  the KMS layer being FIPS-capable.

---

## Open questions for the implementation phase

- Single shared `S3_KMS_KEY_ID` for parquet + blob, or separate keys/ARNs?
- Default value of `LAKE_ENCRYPTION_ENABLED` in non-prod overlays (off vs on).
- Whether to add an alert (not just a gauge) if `din_lake_unencrypted_files`
  stops draining.
- Exact MinIO test strategy for the SSE-KMS assertion (mock vs KES).

---

## Appendix — why the PIN / user-sovereign idea was dropped

The original prompt floated a per-user PIN that "encrypts the data to that
user." Two architectural facts ruled it out for this iteration:

1. **No native per-vehicle key.** DuckLake generates one key *per file*, and
   din packs many vehicle DIDs into each ~128 MB bundle. Real per-user crypto
   would require either one file per vehicle (shattering the bundle packing din
   was tuned to achieve — tiny-file churn) or a custom app-level envelope on the
   payload columns. DuckDB parquet modular encryption also has no per-column
   keys (`column_keys` unimplemented).
2. **A user-only key locks out the platform.** If only the owner's PIN
   decrypts, dq can't materialize, telemetry-api can't serve, and SACD
   data-sharing to authorized apps breaks. A realistic user-factor model is an
   *envelope* (PIN as one wrapping factor via Argon2id, KMS as another,
   authorized grants re-wrap the DEK) — not PIN-only. A 4–6 digit PIN alone is
   offline-brute-forceable and must be Argon2id-stretched **and** KMS-wrapped or
   server-rate-limited to mean anything.

This is viable future work as a layered phase 2 on a sensitive subset (e.g.
precise location), but is out of scope for the at-rest goal chosen here.
