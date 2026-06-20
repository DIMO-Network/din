# DuckLake Postgres catalog maintenance

DuckLake keeps **all** table metadata in the catalog database — for din/dq that
is Postgres. Every write/snapshot inserts rows into the `ducklake_*` metadata
tables (`ducklake_snapshot`, `ducklake_data_file`, `ducklake_file_column_stats`,
`ducklake_delete_file`, …) and every `ducklake_expire_snapshots` +
`ducklake_cleanup_old_files` pass deletes them. That is high-churn OLTP on a
handful of tables, so the catalog — not S3, not DuckDB — is the metadata hot
path. DuckLake's own [recommended maintenance](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance)
says to run `VACUUM` on the Postgres catalog periodically.

din's in-process maintenance (`merge_adjacent_files` → `expire_snapshots` →
`cleanup_old_files` → `delete_orphaned_files`) keeps the **data files** and
DuckLake snapshots bounded. It does **not** manage Postgres-side table bloat —
that is the database's job (autovacuum), and the defaults are too lax for these
churn rates. Apply the two steps below once per catalog database.

## 1. Aggressive autovacuum on the `ducklake_*` tables (one-time, idempotent)

Run as a Postgres superuser / the catalog owner against the catalog database.
Re-running is safe; it only resets per-table storage parameters.

```sql
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE tablename LIKE 'ducklake\_%'
  LOOP
    EXECUTE format(
      'ALTER TABLE %I.%I SET ('
      || 'autovacuum_vacuum_scale_factor = 0.02, '
      || 'autovacuum_analyze_scale_factor = 0.02, '
      || 'autovacuum_vacuum_insert_scale_factor = 0.02, '  -- PG13+
      || 'autovacuum_vacuum_cost_delay = 2'                 -- ms; 0 = no throttle
      || ')', r.schemaname, r.tablename);
  END LOOP;
END $$;
```

This makes autovacuum trigger at ~2% dead/changed tuples instead of the 20%
default, so the metadata tables stay small and index-only scans stay fast.

## 2. Periodic VACUUM (ANALYZE) safety net

Autovacuum handles steady state; schedule an explicit pass (e.g. a daily k8s
CronJob or the platform's managed-Postgres maintenance window) so a burst of
expiry deletes can't outrun it:

```sql
VACUUM (ANALYZE);   -- whole catalog DB; online, non-blocking. NOT inside a txn.
```

Do **not** use `VACUUM FULL` (takes an ACCESS EXCLUSIVE lock and blocks din's
writers). Plain `VACUUM` is online.

Watch `pg_stat_user_tables` for the `ducklake_*` tables: `n_dead_tup` should stay
small and `last_autovacuum` should be recent. Rising dead tuples = tighten the
scale factors above or shorten the VACUUM schedule.

## 3. Connection budget (read replica)

Every DuckDB connection that attaches the catalog opens Postgres connections.
At steady state that is: din ingest (`replicaCount: 2`) + din maintenance (1) +
dq materializer (1) + the dq query fleet (HPA 2–10 × `DUCKDB_MAX_CONNS`, default
6) — i.e. dozens of catalog connections, dominated by the autoscaled **read-only**
query fleet.

- Size Postgres `max_connections` for the peak, and/or front the catalog with
  **pgbouncer** (transaction pooling) for the query fleet.
- The query fleet attaches read-only (`DUCKLAKE_READ_ONLY=true`) and can read a
  **Postgres read replica** via `DUCKLAKE_CATALOG_READ_DSN` — point it at a
  replica to keep the autoscaled read load off the primary that din and the
  materializer write. The single-writer materializer and din always use the
  primary.
```
