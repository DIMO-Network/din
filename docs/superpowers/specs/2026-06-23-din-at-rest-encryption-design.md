# din — At-Rest Encryption Design

**Status:** Plan (design approved, implementation not yet scheduled)
**Date:** 2026-06-23
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
| Migration | Forward plus natural compaction | New writes are encrypted at cutover; the maintainer's compaction rewrites recent partitions encrypted; cold partitions age out under retention. No expensive one-time rewrite. |

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

## Migration: forward plus natural compaction

New writes are encrypted from cutover. The maintainer's existing merge and
compaction cycles rewrite recent partitions, and those rewrites come out
encrypted. Cold partitions that never get re-merged stay plaintext until
retention deletes them. Reads don't care either way: DuckLake handles each file
according to whether it has a key.

To watch the plaintext backlog drain, add a `din_lake_unencrypted_files` gauge
counting `ducklake_data_file` rows with a null or empty `encryption_key`.

## Testing

din has a `./tests/` integration package, so run `go test ./...`, not just
`./internal/... ./cmd/...`.

- ATTACH with `ENCRYPTED`, write a bundle, then check the raw S3 object doesn't
  parse as plaintext parquet, that DuckLake reads it back correctly, and that
  `ducklake_data_file.encryption_key` is populated.
- Read a table that has one plaintext file and one encrypted file. Both should
  come back fine.
- Check that s3client's PutObjectInput carries the SSE params. MinIO isn't AWS
  KMS, so the `s3_minio` path probably needs a mock or fake for that assertion;
  sort that out during implementation.
- Confirm `ENCRYPTED` on the ATTACH doesn't churn snapshots, the way
  `TestOpen_BootstrapIdempotent` already checks.
- Measure the ~2.5x crypto overhead against the sink flush knobs
  (`MinFlushBytes`, `MaxAgeHard`) so it doesn't surprise the write hot path.

## Rollout

Two phases, both behind env flags so we can back either one out fast.

1. SSE-KMS first. Bucket default, the s3client PutObject params, and the
   DuckLake secret `KMS_KEY_ID`. This covers everything (blobs included)
   server-side right away and carries essentially no risk.
2. DuckLake `ENCRYPTED` plus the PG hardening. This is the layer that does the
   real work. To roll back, flip the flag off and new files write plaintext
   again; files already encrypted stay readable as long as their keys are in the
   catalog. Never purge `encryption_key` rows.

### Risks

- KMS sits on the write path. If KMS goes down, SSE-KMS PutObjects fail and the
  sink can stall. Bucket Keys cut the call volume and the existing s3client
  retry and timeout help, but verify the failure behavior before prod.
- Losing the keys means losing the data. See the Layer C note.
- DuckDB's encryption isn't NIST-validated. The KMS layer underneath it is
  FIPS-capable, which is why this is acceptable.

## Open questions for implementation

- One `S3_KMS_KEY_ID` shared by parquet and blob, or separate keys?
- Default for `LAKE_ENCRYPTION_ENABLED` in the non-prod overlays.
- Do we want an alert if `din_lake_unencrypted_files` stops draining, or is the
  gauge enough?
- The exact MinIO approach for the SSE-KMS test (mock vs KES).

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
