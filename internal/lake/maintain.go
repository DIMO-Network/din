package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
)

// Maintainer periodically runs DuckLake's housekeeping: materialize
// inlined rows, merge small Parquet files, expire old snapshots, and
// delete the files those snapshots pinned plus any orphans from crashed
// writes. It replaces din's previous compactor wholesale — merging
// preserves the snapshot change feed, so unlike the old watermark
// protocol no coordination with downstream readers is needed. The one
// surviving contract is retention: SnapshotKeep must exceed the slowest
// consumer's lag.
//
// Run exactly one Maintainer per catalog. Concurrent ingest appends are
// safe alongside it; a second maintenance process would only burn
// metadata-commit retries and S3 churn.
type Maintainer struct {
	lake *Lake
	cfg  MaintConfig
	log  zerolog.Logger
}

// MaintConfig tunes the maintenance cadence. Zero values take defaults.
type MaintConfig struct {
	// Interval between maintenance cycles.
	Interval time.Duration
	// SnapshotKeep is how long snapshots stay readable (time travel and
	// change-feed history) absent a slower consumer floor.
	SnapshotKeep time.Duration
	// ConsumerStaleness is how long a consumer may go without reporting
	// progress before it's presumed dead and dropped from the expiry
	// floor. Must exceed a healthy consumer's reporting gap and stay well
	// below SnapshotKeep. Zero disables the floor (pure time-based).
	ConsumerStaleness time.Duration
}

func (c *MaintConfig) applyDefaults() {
	if c.Interval == 0 {
		c.Interval = 15 * time.Minute
	}
	if c.SnapshotKeep == 0 {
		c.SnapshotKeep = 72 * time.Hour
	}
	if c.ConsumerStaleness == 0 {
		c.ConsumerStaleness = time.Hour
	}
}

var (
	maintCycles = promauto.NewCounter(prometheus.CounterOpts{
		Name: "din_lake_maintenance_cycles_total",
		Help: "Completed lake maintenance cycles.",
	})
	maintErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "din_lake_maintenance_errors_total",
		Help: "Failed lake maintenance steps.",
	}, []string{"step"})
	maintStepSeconds = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name: "din_lake_maintenance_step_seconds",
		Help: "Duration of each lake maintenance step.",
	}, []string{"step"})
	// maintOldestSnapshotAge is the health SLI: alert when it approaches
	// LAKE_SNAPSHOT_RETENTION, the budget that must stay ahead of the dq
	// consumer's lag. A gauge (not a counter) so the alert reads current
	// state; it needs a long-lived process to be scraped, which is why
	// maintenance runs as its own Deployment, not a CronJob.
	maintOldestSnapshotAge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_oldest_unexpired_snapshot_age_seconds",
		Help: "Age of the oldest retained snapshot; must stay below LAKE_SNAPSHOT_RETENTION.",
	})
	maintLastSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_last_successful_cycle_timestamp_seconds",
		Help: "Unix time of the last fully successful maintenance cycle.",
	})
	// maintFloorBinding is 1 when a live consumer's cursor is holding the
	// expiry cutoff back below the retention horizon — i.e. a consumer has
	// fallen behind retention and expiry is protecting it. Alert on it:
	// the lake stops reclaiming space until that consumer catches up.
	maintFloorBinding = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_expiry_floor_binding",
		Help: "1 when a live consumer's progress floor is holding expiry back below retention.",
	})
)

// NewMaintainer wires a Maintainer onto an open Lake.
func NewMaintainer(l *Lake, cfg MaintConfig, log zerolog.Logger) *Maintainer {
	cfg.applyDefaults()
	return &Maintainer{lake: l, cfg: cfg, log: log.With().Str("component", "lake-maintainer").Logger()}
}

// Run cycles until ctx is canceled. Step failures are logged and counted,
// never fatal: a wedged merge must not take ingest down with it.
func (m *Maintainer) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	for {
		if err := m.Cycle(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			m.log.Error().Err(err).Msg("maintenance cycle failed")
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
}

// Cycle runs one full maintenance pass.
func (m *Maintainer) Cycle(ctx context.Context) error {
	expireSQL, err := m.expireSQL(ctx)
	if err != nil {
		return err
	}
	steps := []struct{ name, sql string }{
		// Inlined rows first so the merge pass sees their files.
		{"flush_inlined_data", "CALL ducklake_flush_inlined_data('lake')"},
		{"merge_adjacent_files", "CALL ducklake_merge_adjacent_files('lake')"},
		{"expire_snapshots", expireSQL},
		// Files released by expired snapshots, then crash leftovers.
		{"cleanup_old_files", "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"},
		{"delete_orphaned_files", "CALL ducklake_delete_orphaned_files('lake', older_than => now() - INTERVAL '1 day')"},
	}

	var firstErr error
	for _, step := range steps {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := time.Now()
		n, err := m.execCount(ctx, step.sql)
		maintStepSeconds.WithLabelValues(step.name).Observe(time.Since(start).Seconds())
		if err != nil {
			maintErrors.WithLabelValues(step.name).Inc()
			m.log.Error().Err(err).Str("step", step.name).Msg("maintenance step failed")
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", step.name, err)
			}
			continue
		}
		m.log.Debug().Str("step", step.name).Int("rows", n).
			Dur("took", time.Since(start)).Msg("maintenance step done")
	}
	if firstErr == nil {
		maintCycles.Inc()
		maintLastSuccess.Set(float64(time.Now().Unix()))
	}
	// Refresh the health gauge every cycle, even on partial failure — a
	// stalled expire step is exactly when oldest-snapshot age matters.
	m.recordOldestSnapshotAge(ctx)
	return firstErr
}

// recordOldestSnapshotAge sets the retention-headroom gauge from the
// catalog. Best-effort: a failed read logs and leaves the last value.
func (m *Maintainer) recordOldestSnapshotAge(ctx context.Context) {
	var age sql.NullFloat64
	err := m.lake.db.QueryRowContext(ctx,
		"SELECT epoch(now()) - epoch(min(snapshot_time)) FROM lake.snapshots()").Scan(&age)
	if err != nil {
		m.log.Warn().Err(err).Msg("recording oldest-snapshot age failed")
		return
	}
	if age.Valid {
		maintOldestSnapshotAge.Set(age.Float64)
	}
}

// expireSQL builds this cycle's expire_snapshots call. The cutoff is the
// retention horizon (now - SnapshotKeep), pulled back so it never expires
// at or past the slowest live consumer's cursor — we expire only snapshots
// strictly older than the first one that consumer has not yet consumed.
// With no live consumer (none reported within ConsumerStaleness) the floor
// is absent and the cutoff is pure retention. Sets the floor-binding gauge.
func (m *Maintainer) expireSQL(ctx context.Context) (string, error) {
	keepSecs := int64(m.cfg.SnapshotKeep.Seconds())
	retentionOnly := fmt.Sprintf(
		"CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '%d seconds')", keepSecs)

	floor, ok, err := m.lake.ConsumerFloor(ctx, m.cfg.ConsumerStaleness)
	if err != nil {
		return "", err
	}
	if !ok {
		maintFloorBinding.Set(0)
		return retentionOnly, nil
	}

	// retention_epoch = the time-based horizon; floor_bound_epoch = time of
	// the first snapshot the slowest consumer hasn't consumed (NULL when it
	// is caught up to head, i.e. no constraint).
	var retentionEpoch float64
	var floorBoundEpoch sql.NullFloat64
	err = m.lake.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT
			epoch(now() - INTERVAL '%d seconds'),
			epoch((SELECT MIN(snapshot_time) FROM lake.snapshots() WHERE snapshot_id > %d))`,
		keepSecs, floor)).Scan(&retentionEpoch, &floorBoundEpoch)
	if err != nil {
		return "", fmt.Errorf("expire floor bound: %w", err)
	}

	// The floor only binds when it is more restrictive (older) than
	// retention; a consumer keeping up never holds expiry back.
	if !floorBoundEpoch.Valid || floorBoundEpoch.Float64 >= retentionEpoch {
		maintFloorBinding.Set(0)
		return retentionOnly, nil
	}
	maintFloorBinding.Set(1)
	m.log.Warn().Int64("floor_snapshot", floor).
		Msg("consumer floor is holding expiry back below retention; a consumer is lagging")
	return fmt.Sprintf(
		"CALL ducklake_expire_snapshots('lake', older_than => to_timestamp(%f))", floorBoundEpoch.Float64), nil
}

// execCount runs a maintenance CALL and drains its result rows; the row
// count is what the step reports (files merged, snapshots expired, ...).
func (m *Maintainer) execCount(ctx context.Context, q string) (int, error) {
	rows, err := m.lake.db.QueryContext(ctx, q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}
