package lake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// backfillBatch bounds files per registration transaction: one snapshot
// per batch instead of per file keeps a multi-year backfill from minting
// millions of snapshots.
const backfillBatch = 1000

// BackfillResult reports one Backfill invocation.
type BackfillResult struct {
	Registered int
	Skipped    int
}

// Backfill registers externally written parquet files into raw_events
// without copying them — the path for absorbing historical DIS bundles
// (cloudevent/valid/YYYY/MM/DD/batch-*.parquet), which share the
// raw_events schema by construction. Idempotent: files whose paths the
// catalog already tracks are skipped, so reruns resume where a crashed
// run stopped. Registration transfers ownership to DuckLake — later
// merges may rewrite the data into DATA_PATH and delete the originals.
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

	var pending []string
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			return fmt.Errorf("lake backfill begin: %w", err)
		}
		for _, f := range pending {
			q := fmt.Sprintf("CALL ducklake_add_data_files('lake', %s, %s)",
				sqlString(RawTable), sqlString(f))
			if _, err := conn.ExecContext(ctx, q); err != nil {
				if _, rbErr := conn.ExecContext(ctx, "ROLLBACK"); rbErr != nil {
					return fmt.Errorf("registering %s: %w (rollback also failed: %v)", f, err, rbErr)
				}
				return fmt.Errorf("registering %s: %w", f, err)
			}
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("lake backfill commit: %w", err)
		}
		res.Registered += len(pending)
		log.Info().Int("registered", res.Registered).Int("skipped", res.Skipped).
			Msg("backfill progress")
		pending = pending[:0]
		return nil
	}

	start := time.Now()
	for _, f := range files {
		if !strings.HasSuffix(f, ".parquet") {
			continue
		}
		if existing[f] {
			res.Skipped++
			continue
		}
		pending = append(pending, f)
		if len(pending) >= backfillBatch {
			if err := flush(); err != nil {
				return res, err
			}
		}
	}
	if err := flush(); err != nil {
		return res, err
	}
	log.Info().Int("registered", res.Registered).Int("skipped", res.Skipped).
		Dur("took", time.Since(start)).Msg("backfill done")
	return res, nil
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
