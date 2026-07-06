package lake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// backfillBatch bounds files per per-file registration transaction (the
// partial-prefix fallback): one snapshot per batch instead of per file keeps a
// multi-year backfill from minting millions of snapshots.
const backfillBatch = 1000

// backfillFilesPerSnapshot bounds files per ducklake_add_data_files CALL on the
// fast (whole-new-prefix) path so a large backfill mints many bounded snapshots
// instead of one fat snapshot. dq's readDelta materializes a snapshot WHOLE, so a
// single quarter-million-file snapshot OOM-crash-loops the consumer (S2). A var so
// a test can lower it to assert multi-snapshot batching cheaply.
var backfillFilesPerSnapshot = 2000

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
// and each wholly-new prefix registers via ducklake_add_data_files over an
// explicit LIST of its paths, batched at backfillFilesPerSnapshot files per
// CALL/snapshot. One CALL reads its batch's parquet footers in parallel — over
// S3, httpfs hides the per-request round-trip latency that makes a serial
// CALL-per-file pathologically slow (each file is its own HTTP GET) — while the
// per-CALL batch cap keeps any single snapshot small enough that a
// whole-snapshot consumer (dq's readDelta) can materialize it without OOM (S2).
//
// Idempotent: files the catalog already tracks are filtered out, so reruns
// resume where a crashed run stopped (each batch's CALL + COMMIT is atomic — a
// batch is fully registered or not at all, and already-registered files are
// skipped next time). A prefix that is only partially registered (a crash, or
// files appended to an already-backfilled day) registers just its new files via
// the per-file batched fallback — the whole-prefix list path would re-register
// the tracked files. Registration transfers ownership to DuckLake — later merges
// may rewrite the data into DATA_PATH and delete the originals.
func (l *Lake) Backfill(ctx context.Context, files []string, log zerolog.Logger) (BackfillResult, error) {
	var res BackfillResult
	if l.encrypted {
		// ducklake_add_data_files registers legacy parquet by reference: those files
		// keep their original (unencrypted) bytes at their source path and read with
		// a null encryption_key until the maintainer rewrites them into encrypted
		// files during compaction. Flag the window so it isn't mistaken for at-rest
		// coverage of the backfilled data.
		log.Warn().Msg("backfilling into an ENCRYPTED catalog: registered legacy files stay unencrypted at their source until the maintainer compacts them")
	}
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

		// Fast path when the whole prefix is new: register its explicit file list
		// in bounded snapshot-batches. A partially-registered prefix would
		// re-register the tracked files this way, so it falls back to per-file
		// registration of just the new files.
		if already == 0 {
			n, err := l.registerListed(ctx, conn, group, log)
			res.Registered += n
			if err != nil {
				return res, err
			}
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

// registerListed registers an explicit file list, batched at
// backfillFilesPerSnapshot files per ducklake_add_data_files CALL/snapshot.
// Passing an explicit LIST literal (not a per-prefix glob) keeps DuckDB's
// parallel-footer-read speed while the batch cap bounds each snapshot so a
// whole-snapshot consumer (dq's readDelta) can't be handed a multi-GiB snapshot
// that OOM-crash-loops it (S2). Each batch is its own transaction/snapshot;
// already-registered files are filtered out by the caller, so a crash mid-list
// resumes from the first un-committed batch. Returns the count committed so a
// mid-run failure reports real progress.
func (l *Lake) registerListed(ctx context.Context, conn *sql.Conn, files []string, log zerolog.Logger) (int, error) {
	var registered int
	for i := 0; i < len(files); i += backfillFilesPerSnapshot {
		end := min(i+backfillFilesPerSnapshot, len(files))
		batch := files[i:end]
		// Keep the maintenance pause fresh per batch. If the refresh fails the pause
		// can go stale (>30m), letting the maintainer resume re-asserting partitioning
		// into this RESET window and abort an in-flight add_data_files (recoverable on
		// rerun) — so surface a failing heartbeat instead of swallowing it silently.
		if herr := l.heartbeatBackfillPause(ctx); herr != nil {
			log.Warn().Err(herr).Msg("backfill pause heartbeat refresh failed; maintainer may resume re-asserting layout")
		}
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			return registered, fmt.Errorf("lake backfill begin: %w", err)
		}
		if _, err := conn.ExecContext(ctx, addDataFilesListSQL(batch)); err != nil {
			// ROLLBACK under an uncancellable ctx: duckdb-go short-circuits ExecContext
			// once ctx is done, so a ctx-scoped ROLLBACK on an interrupted backfill would
			// no-op and leave the txn open on this conn — wedging the deferred partition
			// restore (mirrors writer.go's WriteBundle).
			if _, rbErr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK"); rbErr != nil {
				return registered, fmt.Errorf("registering batch of %d files: %w (rollback also failed: %w)", len(batch), err, rbErr)
			}
			return registered, fmt.Errorf("registering batch of %d files: %w", len(batch), err)
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			// A failed COMMIT can leave the txn open; reset it (uncancellable) so the
			// conn — and the deferred SET PARTITIONED BY restore that runs on it — isn't
			// left wedged. Ignore the rollback error (DuckDB may have auto-aborted).
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
			return registered, fmt.Errorf("lake backfill commit: %w", err)
		}
		registered += len(batch)
	}
	return registered, nil
}

// addDataFilesListSQL builds one ducklake_add_data_files CALL over an explicit
// list of paths. allow_missing tolerates legacy bundles written before a column
// was added to raw_events (notably voids_id, the tombstone pointer): the missing
// column reads as NULL, exactly "not voided". Without it ducklake_add_data_files
// hard-fails on any pre-voids_id file.
func addDataFilesListSQL(files []string) string {
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = sqlString(f)
	}
	return fmt.Sprintf("CALL ducklake_add_data_files('lake', %s, [%s], allow_missing => true)",
		sqlString(RawTable), strings.Join(quoted, ", "))
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
