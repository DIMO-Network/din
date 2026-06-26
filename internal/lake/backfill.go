package lake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// backfillBatch bounds files per per-file registration transaction: one
// snapshot per batch instead of per file keeps a multi-year backfill from
// minting millions of snapshots. Only the per-file fallback path uses it; the
// glob fast path commits one snapshot per prefix.
const backfillBatch = 1000

// maxFilesPerGlob caps how many files a single glob CALL registers. A glob
// reads every footer it matches before committing, so an unbounded prefix
// (millions of files) would balloon memory; above the cap we fall back to the
// per-file batched path, which bounds the working set to backfillBatch.
const maxFilesPerGlob = 250_000

// BackfillResult reports one Backfill invocation.
type BackfillResult struct {
	Registered int
	Skipped    int
}

// Backfill registers externally written parquet files into raw_events
// without copying them — the path for absorbing historical DIS bundles
// (cloudevent/valid/YYYY/MM/DD/batch-*.parquet), which share the
// raw_events schema by construction.
//
// Files are grouped by their parent prefix (the S3 "directory" / local dir)
// and each prefix registers in a single ducklake_add_data_files glob CALL.
// One glob lets DuckDB list the prefix and read every parquet footer in
// parallel — over S3, httpfs hides the per-request round-trip latency that
// makes a serial CALL-per-file pathologically slow (each file is its own
// HTTP GET). On local NVMe this is ~17x faster than per-file; over S3 the
// gap is far larger. Because the CALL matches a glob, it registers every
// .parquet physically under the prefix — for immutable day-partition dumps
// that is exactly the listed set.
//
// Idempotent: a prefix all of whose files the catalog already tracks is
// skipped, so reruns resume where a crashed run stopped (the glob CALL +
// COMMIT is atomic — a prefix is fully registered or not at all). A prefix
// that is only partially registered (a crash under the old per-file code, or
// files appended to an already-backfilled day) cannot use the glob — it would
// double-register the tracked files — so it falls back to per-file
// registration of just the new files. Registration transfers ownership to
// DuckLake — later merges may rewrite the data into DATA_PATH and delete the
// originals.
func (l *Lake) Backfill(ctx context.Context, files []string, log zerolog.Logger) (BackfillResult, error) {
	var res BackfillResult
	existing, err := l.registeredFiles(ctx)
	if err != nil {
		return res, err
	}

	conn, err := l.db.Conn(ctx)
	if err != nil {
		return res, fmt.Errorf("lake backfill: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	// Pause the maintainer's per-cycle layout re-assert BEFORE we RESET, so a
	// concurrent maintenance cycle can't re-partition the table out from under the
	// registration window and abort it. Refreshed per prefix below; a crashed
	// backfill's heartbeat goes stale and the maintainer resumes (SR review #9).
	if err := l.heartbeatBackfillPause(ctx); err != nil {
		return res, fmt.Errorf("lake backfill: signaling maintenance pause: %w", err)
	}
	defer func() { _ = l.clearBackfillPause(context.WithoutCancel(ctx)) }()

	// Legacy bundles span multiple (type, day) values per file, which a
	// partitioned table refuses to register. Partitioning only governs
	// newly written data, so drop it for the registration window and
	// restore it after; maintenance later rewrites merged files into the
	// partitioned layout. Native writes landing meanwhile are merely
	// unpartitioned (prunable by stats), never incorrect.
	if _, err := conn.ExecContext(ctx, "ALTER TABLE lake.raw_events RESET PARTITIONED BY"); err != nil {
		return res, fmt.Errorf("lake backfill: dropping partitioning: %w", err)
	}
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx),
			`ALTER TABLE lake.raw_events SET PARTITIONED BY (type, day("time"))`); err != nil {
			log.Error().Err(err).Msg("restoring partitioning failed — rerun: ALTER TABLE raw_events SET PARTITIONED BY (type, day(time))")
		}
	}()

	groups, order := groupParquetByPrefix(files)
	start := time.Now()
	for _, prefix := range order {
		group := groups[prefix]
		newFiles := make([]string, 0, len(group))
		for _, f := range group {
			if existing[f] {
				continue
			}
			newFiles = append(newFiles, f)
		}
		already := len(group) - len(newFiles)
		if len(newFiles) == 0 {
			res.Skipped += already
			continue
		}

		// Glob fast path only when the whole prefix is new and globbable; a
		// partial prefix would re-register the tracked files, an unsafe prefix
		// (glob metacharacters in the path) would match the wrong set, and an
		// oversized prefix would blow memory — all fall back to per-file.
		if already == 0 && globSafe(prefix) && len(group) <= maxFilesPerGlob {
			if err := l.registerGlob(ctx, conn, prefix, log); err != nil {
				return res, err
			}
			res.Registered += len(group)
		} else {
			n, err := l.registerFiles(ctx, conn, newFiles, log)
			if err != nil {
				res.Registered += n
				return res, err
			}
			res.Registered += n
			res.Skipped += already
		}
		log.Info().Int("registered", res.Registered).Int("skipped", res.Skipped).
			Str("prefix", prefix).Msg("backfill progress")
	}
	log.Info().Int("registered", res.Registered).Int("skipped", res.Skipped).
		Dur("took", time.Since(start)).Msg("backfill done")
	return res, nil
}

// registerGlob registers every parquet file under prefix in one transaction
// via a single glob CALL — DuckDB reads the matched footers in parallel.
func (l *Lake) registerGlob(ctx context.Context, conn *sql.Conn, prefix string, log zerolog.Logger) error {
	// Keep the maintenance pause fresh per prefix. If the refresh fails the pause
	// can go stale (>30m), letting the maintainer resume re-asserting partitioning
	// into this RESET window and abort an in-flight add_data_files (recoverable on
	// rerun) — so surface a failing heartbeat instead of swallowing it silently.
	if herr := l.heartbeatBackfillPause(ctx); herr != nil {
		log.Warn().Err(herr).Msg("backfill pause heartbeat refresh failed; maintainer may resume re-asserting layout")
	}
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("lake backfill begin: %w", err)
	}
	// allow_missing tolerates legacy bundles written before a column was added to
	// raw_events (notably voids_id, the tombstone pointer): the missing column
	// reads as NULL, exactly "not voided". Without it ducklake_add_data_files
	// hard-fails on any pre-voids_id file.
	q := fmt.Sprintf("CALL ducklake_add_data_files('lake', %s, %s, allow_missing => true)",
		sqlString(RawTable), sqlString(prefix+"*.parquet"))
	if _, err := conn.ExecContext(ctx, q); err != nil {
		// ROLLBACK under an uncancellable ctx: duckdb-go short-circuits ExecContext
		// once ctx is done, so a ctx-scoped ROLLBACK on an interrupted backfill would
		// no-op and leave the txn open on this conn — wedging the deferred partition
		// restore (mirrors writer.go's WriteBundle).
		if _, rbErr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK"); rbErr != nil {
			return fmt.Errorf("registering glob %s: %w (rollback also failed: %w)", prefix, err, rbErr)
		}
		return fmt.Errorf("registering glob %s: %w", prefix, err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		// A failed COMMIT can leave the txn open; reset it (uncancellable) so the
		// conn — and the deferred SET PARTITIONED BY restore that runs on it — isn't
		// left wedged. Ignore the rollback error (DuckDB may have auto-aborted).
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return fmt.Errorf("lake backfill commit: %w", err)
	}
	return nil
}

// registerFiles registers an explicit file list one CALL at a time, batched at
// backfillBatch per transaction — the fallback for prefixes the glob path can't
// take. Returns the count committed so a mid-run failure reports real progress.
func (l *Lake) registerFiles(ctx context.Context, conn *sql.Conn, files []string, log zerolog.Logger) (int, error) {
	var registered int
	for i := 0; i < len(files); i += backfillBatch {
		end := min(i+backfillBatch, len(files))
		batch := files[i:end]
		if herr := l.heartbeatBackfillPause(ctx); herr != nil {
			log.Warn().Err(herr).Msg("backfill pause heartbeat refresh failed; maintainer may resume re-asserting layout")
		}
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			return registered, fmt.Errorf("lake backfill begin: %w", err)
		}
		for _, f := range batch {
			q := fmt.Sprintf("CALL ducklake_add_data_files('lake', %s, %s, allow_missing => true)",
				sqlString(RawTable), sqlString(f))
			if _, err := conn.ExecContext(ctx, q); err != nil {
				// Uncancellable ROLLBACK — see registerGlob; a ctx-scoped ROLLBACK
				// no-ops once ctx is done and leaves the txn open on this conn.
				if _, rbErr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK"); rbErr != nil {
					return registered, fmt.Errorf("registering %s: %w (rollback also failed: %w)", f, err, rbErr)
				}
				return registered, fmt.Errorf("registering %s: %w", f, err)
			}
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			// Reset a possibly-open txn (uncancellable) so the conn stays reusable.
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
			return registered, fmt.Errorf("lake backfill commit: %w", err)
		}
		registered += len(batch)
	}
	return registered, nil
}

// groupParquetByPrefix buckets parquet paths by their parent prefix
// (everything up to and including the last '/'), preserving first-seen prefix
// order so registration follows the caller's listing. Non-parquet paths are
// dropped, mirroring the suffix filter the per-file path applied.
func groupParquetByPrefix(files []string) (map[string][]string, []string) {
	groups := make(map[string][]string)
	var order []string
	for _, f := range files {
		if !strings.HasSuffix(f, ".parquet") {
			continue
		}
		prefix := ""
		if i := strings.LastIndex(f, "/"); i >= 0 {
			prefix = f[:i+1]
		}
		if _, ok := groups[prefix]; !ok {
			order = append(order, prefix)
		}
		groups[prefix] = append(groups[prefix], f)
	}
	return groups, order
}

// globSafe reports whether prefix can be safely suffixed with "*.parquet" and
// passed to a glob CALL — a path already containing glob metacharacters would
// match an unintended set.
func globSafe(prefix string) bool {
	return !strings.ContainsAny(prefix, "*?[")
}

// registeredFiles returns every data-file path the catalog tracks for
// raw_events — the idempotence set for reruns.
func (l *Lake) registeredFiles(ctx context.Context) (map[string]bool, error) {
	rows, err := l.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT data_file FROM ducklake_list_files('lake', %s)", sqlString(RawTable)))
	if err != nil {
		return nil, fmt.Errorf("lake backfill: listing registered files: %w", err)
	}
	defer rows.Close() //nolint:errcheck
	out := map[string]bool{}
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		out[path] = true
	}
	return out, rows.Err()
}
