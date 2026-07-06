# Catalog backup & restore (DuckLake / Postgres) — the data-loss runbook

**Read this before enabling `LAKE_ENCRYPTION_ENABLED` anywhere real.**

## Why the catalog is data-critical

The Postgres catalog is not metadata you can rebuild — with at-rest encryption
on, it IS the data:

- `ducklake_data_file.encryption_key` holds the **only copy** of each parquet
  file's AES key. S3 holds ciphertext. **Catalog loss = permanent loss of the
  entire lake**, regardless of S3 durability.
- The NATS WAL cannot replay lost writes: messages are acked once the DuckLake
  commit lands, so anything committed after your last usable catalog state is
  gone from the WAL too.
- The orphan sweep (`ducklake_delete_orphaned_files`, `LAKE_ORPHAN_RETENTION`,
  default 7d) **actively deletes** any S3 file the catalog doesn't reference.
  After a point-in-time restore to T−Δ, every file written in (T−Δ, T] is an
  unreferenced orphan whose key was in the lost catalog delta — and the next
  maintenance cycle past the window destroys those bytes permanently (B6).

## Backup requirements (non-negotiable with encryption on)

1. **Continuous archiving / PITR** (WAL-G, RDS PITR, Cloud SQL PITR — whatever
   the infra provides). Nightly dumps are NOT enough: a dump's RPO is up to
   24h of per-file keys.
2. **Verified restores**: exercise a restore into a scratch instance on a
   schedule. An unverified backup is a hypothesis.
3. **Backup-age alert**: alert when the newest recoverable point is older than
   1h. (Infra-side — Prometheus postgres exporter `pg_last_wal_receive_lsn`
   lag on the replica, or the managed service's backup-age metric. din cannot
   see this from inside.)
4. `LAKE_ORPHAN_RETENTION` must comfortably exceed **worst-case
   restore-detection time** — the time from "catalog corrupted/lost" to
   "restore completed and verified", including a weekend. Default is 7 days;
   the only cost of a longer window is transient orphan disk.

## Post-restore procedure (PITR to T−Δ)

Order matters — step 1 before anything else:

1. **Freeze the orphan sweep**: set `LAKE_ORPHAN_RETENTION=-1s` on the
   maintenance Deployment (maintain.go skips `delete_orphaned_files` and logs
   a warning every cycle). Files written after T−Δ are now safe from deletion
   while you decide their fate.
2. **Stop writers** (din ingest pods, dq materializer) until the catalog state
   is verified — concurrent commits against a just-restored catalog make the
   audit ambiguous.
3. **Assess the gap**: list S3 objects newer than T−Δ under the lake data
   path vs `ducklake_data_file`. Three classes:
   - *Unencrypted era files* (`LAKE_ENCRYPTION_ENABLED` was off when written):
     recoverable — re-register via `ducklake_add_data_files` (the DIS-backfill
     path) after schema-compat check.
   - *Encrypted files whose keys are in the restored catalog* (written before
     T−Δ, orphaned for other reasons): already referenced or re-registrable.
   - *Encrypted files written after T−Δ*: **unrecoverable** (keys lost).
     Record the subject/time ranges from filenames/partitions for the gap
     report; devices with retention may re-send.
4. **Re-point consumers**: dq's cursor (`lake.ingest_progress`) came back at
   its T−Δ value — it will re-read from there; the insert anti-join dedups
   re-decoded rows. Verify `meta.din_consumer_progress` floor rows are sane.
5. **Restart writers**, watch `din_lake_*` health gauges + dq decode lag.
6. **Unfreeze the sweep**: restore `LAKE_ORPHAN_RETENTION` to its normal value
   only after re-registration is complete and the audit is clean.

## Ops notes

- Treat catalog credentials/DSN with `sslmode=verify-full` — the catalog is
  the trust root for every reader.
- The maintenance singleton is the only process that may run the sweep;
  see `charts/din/templates/maintenance-deployment.yaml`.
- Related: `docs/catalog-postgres-maintenance.md` (autovacuum/sizing),
  `LAKE_SNAPSHOT_RETENTION` (change-feed history), consumer floor
  (`meta.din_consumer_progress`).
