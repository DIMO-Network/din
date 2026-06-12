package lake

import (
	"context"
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
	// change-feed history). Must exceed downstream consumer lag.
	SnapshotKeep time.Duration
}

func (c *MaintConfig) applyDefaults() {
	if c.Interval == 0 {
		c.Interval = 15 * time.Minute
	}
	if c.SnapshotKeep == 0 {
		c.SnapshotKeep = 72 * time.Hour
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
	keepSecs := int64(m.cfg.SnapshotKeep.Seconds())
	steps := []struct{ name, sql string }{
		// Inlined rows first so the merge pass sees their files.
		{"flush_inlined_data", "CALL ducklake_flush_inlined_data('lake')"},
		{"merge_adjacent_files", "CALL ducklake_merge_adjacent_files('lake')"},
		{"expire_snapshots", fmt.Sprintf(
			"CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '%d seconds')", keepSecs)},
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
	}
	return firstErr
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
