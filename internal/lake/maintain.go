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
	// OrphanRetention is the ducklake_delete_orphaned_files older_than
	// window. This is the DISASTER-RECOVERY window (B6): after a Postgres
	// PITR restore to T-Δ, every data file written after T-Δ is an orphan
	// whose per-file encryption key was in the lost catalog delta — and the
	// orphan sweep permanently deletes those bytes. The window must exceed
	// worst-case restore-DETECTION time, not just crash-leftover age; the
	// only cost of a generous window is transient orphan disk. Negative
	// disables the step entirely (the post-restore guard: set
	// LAKE_ORPHAN_RETENTION=-1s while re-registering restored-era files).
	// See docs/catalog-backup-restore.md.
	OrphanRetention time.Duration
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
	if c.OrphanRetention == 0 {
		c.OrphanRetention = 7 * 24 * time.Hour
	}
}

// maintStep is one maintenance CALL and its metric label.
type maintStep struct{ name, sql string }

const (
	// maxConsecutiveCycleFailures bounds how many cycles may fail back-to-back
	// before Run returns, failing the errgroup so the pod restarts (and re-runs
	// Open/ensureSchema). At the 15-minute default interval this is ~1h of
	// durably-broken maintenance — long enough to ride out a transient catalog/S3
	// outage, short enough that snapshots don't silently stop expiring against
	// the retention budget while the pod sits Ready-but-idle.
	maxConsecutiveCycleFailures = 4
	// maintStepTimeout bounds a single maintenance CALL. merge/expire run minutes
	// by design, so the budget is generous; it exists only to break a step wedged
	// on a hung S3 multipart or catalog lock, which the between-steps ctx check
	// cannot preempt.
	maintStepTimeout = 30 * time.Minute
)

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
	// staleConsumers is the count of consumers present in the progress table
	// but past the staleness window — known consumers the floor no longer
	// protects. Non-zero means a consumer is unprotected (SR review #2, F2).
	staleConsumers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_stale_consumers",
		Help: "Consumers with a progress row older than the staleness window (no longer protected by the expiry floor).",
	})
	// consumerDropped counts un-consumed snapshots reclaimed past a dropped
	// (stale) consumer's cursor — the actual data-loss event. The floor-binding
	// gauge flips 1→0 at this exact moment, so its alert *resolves* and reads as
	// recovery; this counter makes the loss its own alertable signal (F1).
	consumerDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "din_lake_consumer_dropped_total",
		Help: "Un-consumed snapshots reclaimed past a dropped stale consumer's cursor (permanent change-feed loss).",
	}, []string{"consumer"})
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
	consecutiveFailures := 0
	for {
		if err := m.Cycle(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			consecutiveFailures++
			m.log.Error().Err(err).Int("consecutive", consecutiveFailures).Msg("maintenance cycle failed")
			if consecutiveFailures >= maxConsecutiveCycleFailures {
				// Durably broken (catalog unreachable, metadata wedged): stop
				// pretending to be healthy. Returning fails the errgroup so K8s
				// restarts the pod instead of leaving it Ready while snapshots
				// silently stop expiring (SR review — wedged-maintainer-invisible).
				return fmt.Errorf("maintenance wedged after %d consecutive cycle failures: %w", consecutiveFailures, err)
			}
		} else {
			consecutiveFailures = 0
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
	var firstErr error

	// Re-assert the partition layout first. A crashed backfill can leave
	// raw_events RESET, and unlike a fresh Open the long-lived maintainer never
	// re-runs ensureSchema — so without this it would merge new data into
	// unpartitioned files until the next pod boot (SR review #9). Idempotent and
	// snapshot-free when the layout is already active.
	start := time.Now()
	// Wrap in retryCatalog like the boot path does (lake.go:282, ensureSchema): the ALTERs
	// can lose a cross-pod metadata-commit race (DuckLake "TransactionContext Error") that
	// is transient. Boot retries it; the 15-min maintenance loop must too, or it burns a
	// degraded cycle on a conflict that a retry would clear.
	if err := retryCatalog(ctx, func() error { return m.lake.reassertLayout(ctx) }); err != nil {
		maintErrors.WithLabelValues("reassert_layout").Inc()
		m.log.Error().Err(err).Str("step", "reassert_layout").Msg("maintenance step failed")
		// First step in the cycle, so firstErr is still nil — assign directly.
		firstErr = fmt.Errorf("reassert_layout: %w", err)
	}
	maintStepSeconds.WithLabelValues("reassert_layout").Observe(time.Since(start).Seconds())

	// Surface stale/dropped consumers before expiry runs, while the snapshots
	// they are about to lose still exist to be counted (SR review #2).
	m.reportConsumerHealth(ctx)

	steps := []maintStep{
		// Inlined rows first so the merge pass sees their files.
		{"flush_inlined_data", "CALL ducklake_flush_inlined_data('lake')"},
		{"merge_adjacent_files", "CALL ducklake_merge_adjacent_files('lake')"},
	}
	// A transient catalog blip while building the expire cutoff must not skip the
	// independent merge/cleanup/orphan steps for the whole interval. Count it like
	// a failed step and run the rest; expiry retries next cycle. Insert it between
	// merge and cleanup — cleanup releases the files the expired snapshots pinned.
	expireSQL, err := m.expireSQL(ctx)
	if err != nil {
		maintErrors.WithLabelValues("expire_snapshots").Inc()
		m.log.Error().Err(err).Str("step", "expire_snapshots").Msg("maintenance step failed")
		if firstErr == nil {
			firstErr = fmt.Errorf("expire_snapshots: %w", err)
		}
	} else {
		steps = append(steps, maintStep{"expire_snapshots", expireSQL})
	}
	// Files released by expired snapshots, then crash leftovers.
	steps = append(steps,
		maintStep{"cleanup_old_files", "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"})
	// Orphan deletion is IRREVERSIBLE byte destruction and, with the catalog
	// holding the per-file encryption keys, the step that turns a catalog
	// restore into permanent data loss (B6) — see MaintConfig.OrphanRetention
	// and docs/catalog-backup-restore.md. Skippable for post-restore recovery.
	if m.cfg.OrphanRetention >= 0 {
		steps = append(steps, maintStep{"delete_orphaned_files",
			fmt.Sprintf("CALL ducklake_delete_orphaned_files('lake', older_than => now() - INTERVAL '%d seconds')",
				int64(m.cfg.OrphanRetention.Seconds()))})
	} else {
		m.log.Warn().Msg("orphan-file deletion DISABLED (LAKE_ORPHAN_RETENTION < 0) — re-enable after post-restore recovery completes")
	}

	for _, step := range steps {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		start := time.Now()
		stepCtx, cancel := context.WithTimeout(ctx, maintStepTimeout)
		n, err := m.execCount(stepCtx, step.sql)
		cancel()
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

// reportConsumerHealth surfaces consumers that have gone stale (present but
// past the staleness window) and, distinctly, the moment a dropped consumer's
// un-consumed backlog is reclaimed by time-only expiry. Without it that loss
// shows up only as the floor-binding gauge clearing — an alert *resolving*,
// which reads as recovery (SR review #2). Best-effort; never fails the cycle.
func (m *Maintainer) reportConsumerHealth(ctx context.Context) {
	if m.cfg.ConsumerStaleness <= 0 {
		return
	}
	stale, err := m.lake.StaleConsumers(ctx, m.cfg.ConsumerStaleness)
	if err != nil {
		m.log.Warn().Err(err).Msg("reading stale consumers failed")
		return
	}
	staleConsumers.Set(float64(len(stale)))
	if len(stale) == 0 {
		return
	}
	// Count against the EFFECTIVE expire cutoff (what this cycle will actually expire),
	// not pure retention — a *different* live consumer's floor may protect this stale
	// consumer's backlog, so pure retention would over-count and false-page "data loss".
	cutoff, _, _, err := m.expireCutoff(ctx)
	if err != nil {
		m.log.Warn().Err(err).Msg("resolving expire cutoff for consumer health failed")
		return
	}
	for _, c := range stale {
		lost, err := m.lake.UnconsumedExpiringCount(ctx, c.SnapshotID, cutoff)
		if err != nil {
			m.log.Warn().Err(err).Str("consumer", c.Name).Msg("checking dropped-consumer backlog failed")
			continue
		}
		if lost > 0 {
			consumerDropped.WithLabelValues(c.Name).Add(float64(lost))
			m.log.Error().Str("consumer", c.Name).Int64("cursor_snapshot", c.SnapshotID).
				Float64("stale_age_seconds", c.AgeSeconds).Int64("snapshots_reclaimed", lost).
				Msg("data loss: dropped stale consumer's un-consumed snapshots are being expired; backfill the gap and restore the consumer")
			continue
		}
		m.log.Warn().Str("consumer", c.Name).Float64("stale_age_seconds", c.AgeSeconds).
			Msg("consumer is stale and unprotected by the expiry floor (no backlog past retention yet)")
	}
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

// expireCutoff resolves this cycle's effective expire cutoff: the snapshot_time
// before which snapshots will be expired, as an epoch. It is the retention horizon
// (now - SnapshotKeep), pulled back to the slowest live consumer's first-unconsumed
// snapshot when that floor is more restrictive (older). Returns whether a consumer
// floor (rather than pure retention) is binding, plus the binding floor snapshot id
// (for the lagging-consumer warning). Shared by expireSQL (to build the CALL) and
// reportConsumerHealth (to count only what will ACTUALLY be expired — pure retention
// over-counts when a *different* live consumer's floor protects a stale consumer's
// backlog, which would false-page "data loss").
func (m *Maintainer) expireCutoff(ctx context.Context) (cutoffEpoch float64, floorSnapshot int64, floorBinding bool, err error) {
	keepSecs := int64(m.cfg.SnapshotKeep.Seconds())
	floor, ok, err := m.lake.ConsumerFloor(ctx, m.cfg.ConsumerStaleness)
	if err != nil {
		return 0, 0, false, err
	}
	if !ok {
		// No live consumer reported within ConsumerStaleness: cutoff is pure retention.
		var retentionEpoch float64
		if err = m.lake.db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT epoch(now() - INTERVAL '%d seconds')", keepSecs)).Scan(&retentionEpoch); err != nil {
			return 0, 0, false, fmt.Errorf("expire retention epoch: %w", err)
		}
		return retentionEpoch, 0, false, nil
	}
	// retention_epoch = the time-based horizon; floor_bound_epoch = time of the first
	// snapshot the slowest consumer hasn't consumed (NULL when caught up to head).
	var retentionEpoch float64
	var floorBoundEpoch sql.NullFloat64
	if err = m.lake.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT
			epoch(now() - INTERVAL '%d seconds'),
			epoch((SELECT MIN(snapshot_time) FROM lake.snapshots() WHERE snapshot_id > %d))`,
		keepSecs, floor)).Scan(&retentionEpoch, &floorBoundEpoch); err != nil {
		return 0, 0, false, fmt.Errorf("expire floor bound: %w", err)
	}
	// The floor only binds when it is more restrictive (older) than retention; a
	// consumer keeping up never holds expiry back.
	if !floorBoundEpoch.Valid || floorBoundEpoch.Float64 >= retentionEpoch {
		return retentionEpoch, floor, false, nil
	}
	return floorBoundEpoch.Float64, floor, true, nil
}

// expireSQL builds this cycle's expire_snapshots call from the effective cutoff
// (see expireCutoff) and sets the floor-binding gauge.
func (m *Maintainer) expireSQL(ctx context.Context) (string, error) {
	cutoff, floorSnapshot, floorBinding, err := m.expireCutoff(ctx)
	if err != nil {
		return "", err
	}
	if floorBinding {
		maintFloorBinding.Set(1)
		m.log.Warn().Int64("floor_snapshot", floorSnapshot).
			Msg("consumer floor is holding expiry back below retention; a consumer is lagging")
		return fmt.Sprintf(
			"CALL ducklake_expire_snapshots('lake', older_than => to_timestamp(%f))", cutoff), nil
	}
	maintFloorBinding.Set(0)
	// Use the FROZEN retention cutoff (the value the floor decision at line 341 was made
	// against), not now()-INTERVAL re-evaluated at CALL time. This CALL runs only after the
	// merge steps, each budgeted up to maintStepTimeout (30m), so a re-evaluated now() drifts
	// minutes past the decision cutoff and can expire snapshot floor+1 — un-consumed
	// change-feed data a reader parked at the retention edge still needs (it then hits
	// maybeRecoverExpired and permanently skips that prefix). Freezing can only expire less,
	// and matches the floor-binding branch above.
	return fmt.Sprintf(
		"CALL ducklake_expire_snapshots('lake', older_than => to_timestamp(%f))", cutoff), nil
}

// execCount runs a maintenance CALL and drains its result rows; the row
// count is what the step reports (files merged, snapshots expired, ...).
func (m *Maintainer) execCount(ctx context.Context, q string) (int, error) {
	rows, err := m.lake.db.QueryContext(ctx, q)
	if err != nil {
		return 0, err
	}
	defer rows.Close() //nolint:errcheck
	n := 0
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}
