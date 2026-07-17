package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	// mergeRamp is the current per-call merge file cap. It starts at
	// mergeRampStartFiles, doubles after each committed sub-call and halves on a
	// graceful DuckDB OOM, clamped to the effective ceiling. Persisting it across
	// cycles lets a healthy process reach its steady-state cap once and stay there;
	// resetting it only on process boot (a fresh Maintainer) is what makes an
	// uncatchable cgroup OOMKill self-correct. Touched only from the single-threaded
	// Run→Cycle→runBoundedMerge path, so no lock is needed. See issue #11.
	mergeRamp int
	// memBudgetBytes is DUCKDB_MEMORY_LIMIT parsed to bytes (0 if empty/unparseable),
	// the input to the derived merge ceiling.
	memBudgetBytes int64
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
	// MergeMaxFilesPerCall is an explicit ceiling on how many files one merge
	// sub-call rewrites (max_compacted_files). Zero (the default) derives a
	// memory-safe ceiling from MemoryLimit and the observed average file size;
	// runBoundedMerge ramps up to whichever ceiling applies from a small first
	// sub-call so a cold, uncompacted lake never attempts a merge too big for
	// the pod's memory budget (issue #11). Positive pins the ceiling by hand.
	MergeMaxFilesPerCall int
	// MemoryLimit is DUCKDB_MEMORY_LIMIT verbatim (e.g. "1GB"), the same value
	// the DuckDB pool runs under. The maintainer parses it to derive the merge
	// ceiling above; unparseable/empty falls back to mergeMaxFilesFallback.
	MemoryLimit string
	// RewriteDeleteThreshold is ducklake_rewrite_data_files' delete_threshold:
	// a data file whose deleted-row fraction is at or above it is rewritten to a
	// live-rows-only file. Merge-on-read deletes leave the original file in
	// place with a delete file attached, and merge_adjacent_files never touches
	// such files — so delete-churned tables (dq's DELETE+INSERT-flushed
	// signals_latest/events_latest rollups) fragment without bound unless this
	// step reclaims them first; the rewritten outputs are what the merge pass
	// can then compact. Zero takes the default (0.5); negative disables the
	// step. DuckLake's own default (0.95) is too lax for the rollup churn: a
	// multi-subject flush file accretes deletes a few subjects at a time and
	// would sit fragmented for a long tail of flushes before crossing 0.95.
	RewriteDeleteThreshold float64
}

// defaultRewriteDeleteThreshold rewrites a file once at least half its rows are
// deleted: early enough that rollup-flush files (tiny, quickly delete-ridden)
// are reclaimed within a cycle or two, while a big decoded-table file is only
// rewritten when the rewrite at most halves its size.
const defaultRewriteDeleteThreshold = 0.5

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
	if c.RewriteDeleteThreshold == 0 {
		c.RewriteDeleteThreshold = defaultRewriteDeleteThreshold
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
	// mergeCycleBudget caps total merge wall-clock per maintenance cycle. Each
	// bounded sub-call commits independently, so spending the budget just defers
	// the remaining files to the next cycle instead of discarding this cycle's
	// merges — the all-or-nothing failure mode of the old single CALL.
	mergeCycleBudget = maintStepTimeout
)

// The merge sub-call file cap (ducklake_merge_adjacent_files' max_compacted_files)
// is DuckLake's documented memory-batching lever — "compacting data files can be
// very memory intensive... perform this operation in batches by specifying this
// parameter." Bounding the work per CALL also makes each CALL its own committed,
// durable unit of progress against the maintStepTimeout (load review #10). But a
// fixed cap is unbounded in MEMORY: on a cold, uncompacted lake one CALL can rewrite
// enough small files to blow the pod's cgroup before it commits, so maintenance
// OOMKills every cycle and never converges — a death spiral where the first unit of
// progress is too big to fit (issue #11).
//
// runBoundedMerge instead RAMPS the cap: it starts each process at mergeRampStartFiles
// (small enough to fit any sane budget), doubles after every sub-call that commits, and
// clamps to a ceiling that is either MaintConfig.MergeMaxFilesPerCall (an explicit pin)
// or derived from DUCKDB_MEMORY_LIMIT ÷ observed average file size. The ramp resets on
// every process boot, so a cgroup OOMKill — which is uncatchable in-process — is handled
// structurally: the restarted pod's first sub-call is tiny again. A GRACEFUL DuckDB OOM
// (memory_limit hit before the cgroup, surfaced as a query error) is caught and halves
// the ramp in-process, no crash.
const (
	// mergeRampStartFiles is the per-call cap for the first merge sub-call of a
	// process's life, and the value the ramp resets to on each boot. Small enough
	// that a single sub-call fits any realistic memory budget with or without a
	// DuckDB spill volume, so a cold-start first cycle always makes durable progress.
	mergeRampStartFiles = 32
	// mergeMinFilesPerCall is the floor the graceful-OOM backoff descends toward: a
	// single-file cap always fits, so the ramp can always find a size that commits.
	mergeMinFilesPerCall = 1
	// mergeMaxFilesHardCeiling is the absolute upper clamp on any derived or ramped
	// cap — matches din's historical fixed cap and bounds a single transaction's S3
	// churn even on a very large memory budget.
	mergeMaxFilesHardCeiling = 1000
	// mergeMaxFilesFallback is the ceiling used when MergeMaxFilesPerCall is 0 AND
	// DUCKDB_MEMORY_LIMIT is empty/unparseable (e.g. a "80%" percentage form) or the
	// catalog has no file-size stats yet — conservative, since we can't size to the pod.
	mergeMaxFilesFallback = 256
	// mergeMemoryBudgetFraction is the share of DUCKDB_MEMORY_LIMIT a single merge
	// sub-call's rewrite (avg file size × cap) is allowed to touch. The remainder is
	// headroom for DuckDB's baseline, read/decompression buffers, and the metadata
	// commit — a merge reads inputs while it writes outputs, so it needs well under 1.
	mergeMemoryBudgetFraction = 0.5
)

// The din_lake_* maintenance metrics below are maintainer-OWNED: only the
// Maintainer's Cycle/reportConsumerHealth/expireSQL paths ever write them, and
// only the dedicated maintenance Deployment runs a Maintainer.
//
// NOT promauto: package lake is imported by every din binary (ingest sinks,
// backfill), so promauto's package-init registration would make an ingest pod
// export din_lake_last_successful_cycle_timestamp_seconds{}=0 etc. That defeats
// DinMaintenanceDown's absent_over_time (the gauge is never absent while any
// ingest pod is up) and poisons the max()-fallback alerts with a bogus 0 during
// a real maintenance outage (C2). Registration happens in
// registerMaintenanceMetrics, called from NewMaintainer only — mirrors dq's H2
// materializer-metrics fix.
var (
	maintCycles = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "din_lake_maintenance_cycles_total",
		Help: "Completed lake maintenance cycles.",
	})
	maintErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "din_lake_maintenance_errors_total",
		Help: "Failed lake maintenance steps.",
	}, []string{"step"})
	maintStepSeconds = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "din_lake_maintenance_step_seconds",
		Help: "Duration of each lake maintenance step.",
	}, []string{"step"})
	// maintOldestSnapshotAge is the health SLI: alert when it approaches
	// LAKE_SNAPSHOT_RETENTION, the budget that must stay ahead of the dq
	// consumer's lag. A gauge (not a counter) so the alert reads current
	// state; it needs a long-lived process to be scraped, which is why
	// maintenance runs as its own Deployment, not a CronJob.
	maintOldestSnapshotAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_oldest_unexpired_snapshot_age_seconds",
		Help: "Age of the oldest retained snapshot; must stay below LAKE_SNAPSHOT_RETENTION.",
	})
	maintLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_last_successful_cycle_timestamp_seconds",
		Help: "Unix time of the last fully successful maintenance cycle.",
	})
	// maintFloorBinding is 1 when a live consumer's cursor is holding the
	// expiry cutoff back below the retention horizon — i.e. a consumer has
	// fallen behind retention and expiry is protecting it. Alert on it:
	// the lake stops reclaiming space until that consumer catches up.
	maintFloorBinding = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_expiry_floor_binding",
		Help: "1 when a live consumer's progress floor is holding expiry back below retention.",
	})
	// staleConsumers is the count of consumers present in the progress table
	// but past the staleness window — known consumers the floor no longer
	// protects. Non-zero means a consumer is unprotected (SR review #2, F2).
	staleConsumers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_stale_consumers",
		Help: "Consumers with a progress row older than the staleness window (no longer protected by the expiry floor).",
	})
	// consumerDropped counts un-consumed snapshots reclaimed past a dropped
	// (stale) consumer's cursor — the actual data-loss event. The floor-binding
	// gauge flips 1→0 at this exact moment, so its alert *resolves* and reads as
	// recovery; this counter makes the loss its own alertable signal (F1).
	consumerDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "din_lake_consumer_dropped_total",
		Help: "Un-consumed snapshots reclaimed past a dropped stale consumer's cursor (permanent change-feed loss).",
	}, []string{"consumer"})
	// maintDataFiles is the small-file-backlog SLI (load review #10): the DuckLake
	// data-file count per lake table. Sustained growth means compaction isn't
	// keeping up (merge timing out, or falling behind ingest), which inflates the
	// per-query S3 GET cost — invisible before this gauge. maintAvgDataFileBytes
	// pairs with it: a falling average alongside a rising count confirms
	// fragmentation rather than genuine data growth.
	maintDataFiles = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "din_lake_data_files",
		Help: "DuckLake data file count per table; sustained growth signals a small-file backlog compaction isn't clearing.",
	}, []string{"table"})
	maintAvgDataFileBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "din_lake_avg_data_file_bytes",
		Help: "Average DuckLake data file size per table; falling alongside rising din_lake_data_files confirms small-file fragmentation.",
	}, []string{"table"})
	// mergeOOMBackoffs counts merge sub-calls that hit the DuckDB memory limit and
	// triggered a per-call file-cap backoff (issue #11). Non-zero means the merge is
	// running at the edge of the budget: the ramp self-corrects, but sustained growth
	// says the pod is undersized for LAKE_TARGET_FILE_SIZE / the backlog.
	mergeOOMBackoffs = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "din_lake_merge_oom_backoff_total",
		Help: "Merge sub-calls that hit the DuckDB memory limit and backed off the per-call file cap.",
	})
	// mergeFilesPerCall is the current ramped per-call file cap — the SLI that shows
	// the cold-start ramp climbing to its ceiling (rising) or the OOM backoff pulling
	// it down (falling). Set at the end of every merge pass.
	mergeFilesPerCall = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "din_lake_merge_files_per_call",
		Help: "Current ramped max_compacted_files cap for merge sub-calls.",
	})
)

// registerMaintenanceMetricsOnce guards registerMaintenanceMetrics: NewMaintainer
// may be called more than once per process (the maintenance service constructs
// one; tests construct many), and duplicate registration would panic.
var registerMaintenanceMetricsOnce sync.Once

// registerMaintenanceMetrics exports the maintainer-owned din_lake_* set with the
// default registry. Called from NewMaintainer only, so a process that never
// constructs a Maintainer (an ingest pod, backfill) exposes none of these series —
// see the package var block for why that matters (C2).
func registerMaintenanceMetrics() {
	registerMaintenanceMetricsOnce.Do(func() {
		prometheus.MustRegister(
			maintCycles, maintErrors, maintStepSeconds, maintOldestSnapshotAge,
			maintLastSuccess, maintFloorBinding, staleConsumers, consumerDropped,
			maintDataFiles, maintAvgDataFileBytes, mergeOOMBackoffs, mergeFilesPerCall,
		)
	})
}

// NewMaintainer wires a Maintainer onto an open Lake. Constructing a Maintainer
// is what registers the maintainer-owned din_lake_* metrics — see
// registerMaintenanceMetrics (C2).
func NewMaintainer(l *Lake, cfg MaintConfig, log zerolog.Logger) *Maintainer {
	cfg.applyDefaults()
	registerMaintenanceMetrics()
	return &Maintainer{
		lake:           l,
		cfg:            cfg,
		log:            log.With().Str("component", "lake-maintainer").Logger(),
		mergeRamp:      mergeRampStartFiles,
		memBudgetBytes: parseByteSize(cfg.MemoryLimit),
	}
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
	recordErr := func(name string, err error) {
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", name, err)
		}
	}

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
		recordErr("reassert_layout", err)
	}
	maintStepSeconds.WithLabelValues("reassert_layout").Observe(time.Since(start).Seconds())

	// Surface stale/dropped consumers before expiry runs, while the snapshots
	// they are about to lose still exist to be counted (SR review #2).
	m.reportConsumerHealth(ctx)

	// Freeze the expire cutoff BEFORE the (possibly long) merge runs — see
	// expireSQL: re-evaluating now() after merge could drift minutes past the
	// frozen decision cutoff and expire a snapshot floor+1 a reader parked at the
	// retention edge still needs. A transient catalog blip building the cutoff
	// must not skip the independent merge/cleanup/orphan work for the whole
	// interval, so it is counted like a failed step and expiry retries next cycle.
	expireStep, expireErr := m.expireSQL(ctx)
	if expireErr != nil {
		maintErrors.WithLabelValues("expire_snapshots").Inc()
		m.log.Error().Err(expireErr).Str("step", "expire_snapshots").Msg("maintenance step failed")
		recordErr("expire_snapshots", expireErr)
	}

	// Inlined rows first so the merge pass sees their files.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, err := m.runStep(ctx, maintStep{"flush_inlined_data", "CALL ducklake_flush_inlined_data('lake')"})
	recordErr("flush_inlined_data", err)

	// Rewrite delete-heavy files BEFORE the merge pass: merge_adjacent_files
	// never compacts a file that has a delete file attached, so the DELETE+
	// INSERT-churned rollup tables (dq's signals_latest/events_latest) are
	// ineligible for merging until this step rewrites them live-rows-only —
	// without it their KB-sized flush files accumulate without bound and every
	// point-read pays the planning cost (2026-07-17 read-mirror incident).
	// Running first also lets this same cycle's merge compact the outputs.
	if m.cfg.RewriteDeleteThreshold >= 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// execCountTxn, not execCount: the rewrite scan must run inside an
		// explicit transaction (see execCountTxn).
		_, err := m.runStepWith(ctx, maintStep{"rewrite_data_files",
			fmt.Sprintf("CALL ducklake_rewrite_data_files('lake', delete_threshold => %g)",
				m.cfg.RewriteDeleteThreshold)}, m.execCountTxn)
		recordErr("rewrite_data_files", err)
	}

	// Bounded, independently-committed merge (load review #10): each sub-call
	// commits its own snapshot, so hitting a timeout or the per-cycle budget
	// leaves earlier merges durable instead of rolling back the whole compaction.
	if ctx.Err() == nil {
		mergeStart := time.Now()
		if _, mergeErr := m.runBoundedMerge(ctx); mergeErr != nil {
			maintErrors.WithLabelValues("merge_adjacent_files").Inc()
			m.log.Error().Err(mergeErr).Str("step", "merge_adjacent_files").Msg("maintenance step failed")
			recordErr("merge_adjacent_files", mergeErr)
		}
		maintStepSeconds.WithLabelValues("merge_adjacent_files").Observe(time.Since(mergeStart).Seconds())
	}

	// Expire (using the frozen cutoff), then cleanup releases the files those
	// snapshots pinned, then the orphan sweep clears crash leftovers.
	var postSteps []maintStep
	if expireErr == nil {
		postSteps = append(postSteps, maintStep{"expire_snapshots", expireStep})
	}
	postSteps = append(postSteps,
		maintStep{"cleanup_old_files", "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"})
	// Orphan deletion is IRREVERSIBLE byte destruction and, with the catalog
	// holding the per-file encryption keys, the step that turns a catalog
	// restore into permanent data loss (B6) — see MaintConfig.OrphanRetention
	// and docs/catalog-backup-restore.md. Skippable for post-restore recovery.
	if m.cfg.OrphanRetention >= 0 {
		postSteps = append(postSteps, maintStep{"delete_orphaned_files",
			fmt.Sprintf("CALL ducklake_delete_orphaned_files('lake', older_than => now() - INTERVAL '%d seconds')",
				int64(m.cfg.OrphanRetention.Seconds()))})
	} else {
		m.log.Warn().Msg("orphan-file deletion DISABLED (LAKE_ORPHAN_RETENTION < 0) — re-enable after post-restore recovery completes")
	}

	for _, step := range postSteps {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := m.runStep(ctx, step)
		recordErr(step.name, err)
	}
	if firstErr == nil {
		maintCycles.Inc()
		maintLastSuccess.Set(float64(time.Now().Unix()))
	}
	// Refresh the health gauges every cycle, even on partial failure — a stalled
	// expire step is exactly when oldest-snapshot age matters, and a stalled merge
	// is exactly when the data-file backlog matters (load review #10).
	m.recordOldestSnapshotAge(ctx)
	m.recordDataFileCounts(ctx)
	return firstErr
}

// runStep runs one maintenance CALL under its own timeout + catalog-conflict
// retry, records its duration/error metrics, and returns the row count and
// error. Retry each CALL through retryCatalog: flush / merge / expire / cleanup
// / orphan all commit DuckLake metadata, and losing that commit race to a
// concurrent dq write surfaces as a transient "TransactionContext Error";
// without the retry a lost race counts a cycle failure toward the 4-strike
// restart backstop, so under sustained dq load the maintainer restart-loops and
// expiry/orphan-delete stop running (S5). The per-step timeout bounds the whole
// retry loop.
func (m *Maintainer) runStep(ctx context.Context, step maintStep) (int, error) {
	return m.runStepWith(ctx, step, m.execCount)
}

func (m *Maintainer) runStepWith(ctx context.Context, step maintStep, exec func(context.Context, string) (int, error)) (int, error) {
	start := time.Now()
	stepCtx, cancel := context.WithTimeout(ctx, maintStepTimeout)
	defer cancel()
	var n int
	err := retryCatalog(stepCtx, func() error {
		var e error
		n, e = exec(stepCtx, step.sql)
		return e
	})
	maintStepSeconds.WithLabelValues(step.name).Observe(time.Since(start).Seconds())
	if err != nil {
		maintErrors.WithLabelValues(step.name).Inc()
		m.log.Error().Err(err).Str("step", step.name).Msg("maintenance step failed")
		return n, err
	}
	m.log.Debug().Str("step", step.name).Int("rows", n).
		Dur("took", time.Since(start)).Msg("maintenance step done")
	return n, nil
}

// mergeExecFaultHook, when non-nil, replaces a merge sub-call's execution so a
// test can force its (rowCount, error) — used to exercise the graceful-OOM backoff
// path, which real memory pressure can't reproduce deterministically in a unit
// test. Nil in production; the argument is the zero-based sub-call index.
var mergeExecFaultHook func(subCall int) (int, error)

// runBoundedMerge compacts small files in bounded, independently-committed
// sub-calls until nothing is left to merge, the per-cycle wall-clock budget is
// spent, or ctx is canceled. Each CALL passes max_compacted_files so it rewrites
// a bounded set of files in its OWN DuckLake transaction (its own snapshot) —
// so a later sub-call timing out never discards the merges an earlier sub-call
// already committed, the all-or-nothing failure mode of the old single
// unbounded CALL under a 30-min timeout (load review #10). Returns the number of
// sub-calls that merged work and the first error.
func (m *Maintainer) runBoundedMerge(ctx context.Context) (int, error) {
	deadline := time.Now().Add(mergeCycleBudget)
	ceiling := m.mergeCeiling(ctx)
	// A smaller ceiling than last cycle (e.g. the pod restarted with a lower
	// DUCKDB_MEMORY_LIMIT, or the average file grew) must pull the ramp down now,
	// not only via a future OOM.
	m.mergeRamp = min(m.mergeRamp, ceiling)
	defer func() { mergeFilesPerCall.Set(float64(m.mergeRamp)) }()
	subCalls := 0
	for {
		if ctx.Err() != nil {
			return subCalls, ctx.Err()
		}
		if !time.Now().Before(deadline) {
			m.log.Info().Int("sub_calls", subCalls).Int("files_per_call", m.mergeRamp).
				Msg("merge budget for this cycle spent; remaining small files merge next cycle")
			return subCalls, nil
		}
		filesCap := min(max(m.mergeRamp, mergeMinFilesPerCall), ceiling)
		query := fmt.Sprintf(
			"CALL ducklake_merge_adjacent_files('lake', max_compacted_files => %d)", filesCap)
		// Each sub-call is its own committed transaction, bounded by its own
		// timeout; a wedged sub-call rolls back only its chunk, leaving the earlier
		// committed sub-calls intact. Retry the transient commit-conflict class but
		// NOT a graceful OOM — retrying an OOM would just burn the budget hitting the
		// same wall three times before we can shrink the cap.
		var n int
		var err error
		if mergeExecFaultHook != nil {
			// Test-only: force a sub-call result (a graceful DuckDB OOM in
			// particular) that is impractical to reproduce with real memory
			// pressure in a unit test. nil lets the real merge run.
			n, err = mergeExecFaultHook(subCalls)
		} else {
			stepCtx, cancel := context.WithTimeout(ctx, maintStepTimeout)
			err = retryCatalogIf(stepCtx, func() error {
				var e error
				n, e = m.execCount(stepCtx, query)
				return e
			}, func(e error) bool { return !isOutOfMemory(e) })
			cancel()
		}
		if err != nil {
			if isOutOfMemory(err) {
				// The cap was too big for the budget. Halve it (floor-bounded) and defer
				// the remaining files to the next cycle — the earlier sub-calls already
				// committed, so this is durable partial progress, not a lost cycle. It is
				// NOT counted a cycle failure: it is expected backpressure the ramp
				// corrects for, and treating it as a failure would march the pod toward
				// the 4-strike restart backstop while it is in fact making progress.
				prev := filesCap
				m.mergeRamp = max(filesCap/2, mergeMinFilesPerCall)
				mergeOOMBackoffs.Inc()
				m.log.Warn().Err(err).Int("from_files", prev).Int("to_files", m.mergeRamp).
					Int("sub_calls", subCalls).
					Msg("merge sub-call hit the DuckDB memory limit; halving per-call file cap, remaining files merge next cycle")
				return subCalls, nil
			}
			return subCalls, err
		}
		if n == 0 {
			// No (schema, table) rows returned ⇒ nothing left to merge this cycle.
			return subCalls, nil
		}
		subCalls++
		// A full sub-call committed without OOM: ramp up toward the ceiling so a
		// healthy process converges on its steady-state throughput.
		m.mergeRamp = min(filesCap*2, ceiling)
	}
}

// mergeCeiling resolves this cycle's upper clamp on the per-call merge file cap.
// An explicit MergeMaxFilesPerCall wins (an operator pinning the value by hand);
// otherwise it derives a memory-safe ceiling from the memory budget and the largest
// average file size in the lake, so a single sub-call's rewrite (avg × cap) stays
// within mergeMemoryBudgetFraction of DUCKDB_MEMORY_LIMIT regardless of backlog
// depth (issue #11). Falls back to mergeMaxFilesFallback when neither the budget nor
// the file-size stats are available.
func (m *Maintainer) mergeCeiling(ctx context.Context) int {
	if m.cfg.MergeMaxFilesPerCall > 0 {
		return min(m.cfg.MergeMaxFilesPerCall, mergeMaxFilesHardCeiling)
	}
	if m.memBudgetBytes <= 0 {
		return mergeMaxFilesFallback
	}
	avg := m.maxAvgFileBytes(ctx)
	if avg <= 0 {
		return mergeMaxFilesFallback
	}
	budget := int64(float64(m.memBudgetBytes) * mergeMemoryBudgetFraction)
	derived := int(budget / avg)
	return min(max(derived, mergeMinFilesPerCall), mergeMaxFilesHardCeiling)
}

// maxAvgFileBytes returns the largest per-table average data-file size in the lake,
// in bytes, or 0 if the catalog has no files yet or the read fails. The MAX across
// tables is deliberately conservative: max_compacted_files is a per-table cap and one
// CALL runs over every table, so sizing the ceiling to the table with the largest
// files keeps the sub-call within budget for all of them.
func (m *Maintainer) maxAvgFileBytes(ctx context.Context) int64 {
	var avg sql.NullFloat64
	err := m.lake.db.QueryRowContext(ctx,
		"SELECT max(file_size_bytes / file_count) FROM ducklake_table_info('lake') WHERE file_count > 0").Scan(&avg)
	if err != nil {
		m.log.Warn().Err(err).Msg("reading average file size for merge ceiling failed; using fallback")
		return 0
	}
	if !avg.Valid || avg.Float64 <= 0 {
		return 0
	}
	return int64(avg.Float64)
}

// recordDataFileCounts refreshes the small-file-backlog gauges from the catalog:
// the DuckLake data-file count and average file size per table. Best-effort — a
// failed read logs and leaves the last values (load review #10).
func (m *Maintainer) recordDataFileCounts(ctx context.Context) {
	rows, err := m.lake.db.QueryContext(ctx,
		"SELECT table_name, file_count, file_size_bytes FROM ducklake_table_info('lake')")
	if err != nil {
		m.log.Warn().Err(err).Msg("recording data-file counts failed")
		return
	}
	defer rows.Close() //nolint:errcheck
	for rows.Next() {
		var table string
		var fileCount, fileBytes int64
		if err := rows.Scan(&table, &fileCount, &fileBytes); err != nil {
			m.log.Warn().Err(err).Msg("scanning data-file counts failed")
			return
		}
		maintDataFiles.WithLabelValues(table).Set(float64(fileCount))
		avg := 0.0
		if fileCount > 0 {
			avg = float64(fileBytes) / float64(fileCount)
		}
		maintAvgDataFileBytes.WithLabelValues(table).Set(avg)
	}
	if err := rows.Err(); err != nil {
		m.log.Warn().Err(err).Msg("iterating data-file counts failed")
	}
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

// isOutOfMemory reports whether err is a graceful DuckDB out-of-memory error —
// DUCKDB_MEMORY_LIMIT hit before the cgroup, surfaced as a query error like
// "Out of Memory Error: could not allocate block of size ...". This is the ONLY
// OOM class we can react to in-process; a cgroup OOMKill (SIGKILL) never reaches
// here — the process is gone — and is handled instead by the ramp resetting on
// boot (issue #11). Matched on message text because the DuckDB Go driver surfaces
// it as an opaque error, mirroring isCommitConflict's approach.
func isOutOfMemory(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "out of memory") || strings.Contains(msg, "could not allocate")
}

// parseByteSize parses a DuckDB-style byte-size string ("512MB", "1GB", "2GiB",
// "1073741824") to bytes. Suffixes are case-insensitive and treated as powers of
// 1024 (the difference from SI is well within mergeMemoryBudgetFraction's headroom).
// A bare number is bytes. Anything it can't parse — empty, a "%" percentage form,
// junk — returns 0, which callers read as "no budget; use the fallback ceiling".
func parseByteSize(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	lower := strings.ToLower(s)
	type unit struct {
		suffix string
		mult   int64
	}
	// Longest suffixes first so "kib" matches before "kb"/"b".
	units := []unit{
		{"tib", 1 << 40}, {"gib", 1 << 30}, {"mib", 1 << 20}, {"kib", 1 << 10},
		{"tb", 1 << 40}, {"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10}, {"b", 1},
	}
	num, mult := lower, int64(1)
	for _, u := range units {
		if strings.HasSuffix(lower, u.suffix) {
			num = strings.TrimSpace(strings.TrimSuffix(lower, u.suffix))
			mult = u.mult
			break
		}
	}
	v, err := strconv.ParseFloat(num, 64)
	if err != nil || v < 0 {
		return 0
	}
	return int64(v * float64(mult))
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

// execCountTxn is execCount inside an explicit transaction. Required for
// ducklake_rewrite_data_files (DuckLake d318a545): whenever the call selects
// candidate files it scans them lazily, and under autocommit that scan outlives
// the statement's own transaction — "Not implemented Error: Scanning a DuckLake
// table after the transaction has ended". An explicit transaction held open
// across the CALL (and committed after the rows are drained) is the documented-
// workaround shape; a no-op rewrite succeeds either way, which is why the bug
// only bites once there are delete-heavy files to reclaim.
func (m *Maintainer) execCountTxn(ctx context.Context, q string) (int, error) {
	tx, err := m.lake.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	rows, err := tx.QueryContext(ctx, q)
	if err != nil {
		return 0, err
	}
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		rows.Close() //nolint:errcheck
		return n, err
	}
	// database/sql requires the result set closed before Commit.
	if err := rows.Close(); err != nil {
		return n, err
	}
	return n, tx.Commit()
}
