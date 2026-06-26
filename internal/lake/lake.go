// Package lake owns din's DuckLake catalog: an embedded DuckDB instance
// with the ducklake extension, attached to a PostgreSQL catalog in
// production (multi-process writes) or a local DuckDB-file catalog for
// dev and tests. Raw cloudevents land in the lake.raw_events table;
// DuckLake writes partitioned, sorted Parquet under DataPath and tracks
// every file, snapshot, and deletion in the catalog — replacing din's
// previous hand-rolled hive layout, manifests, and watermark protocol.
package lake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Config selects the catalog database and object storage for the lake.
type Config struct {
	// CatalogDSN is either a libpq-style PostgreSQL DSN
	// (postgres://... or "host=... dbname=...") or a local path to a
	// DuckDB catalog file for single-process dev/test use.
	CatalogDSN string
	// DataPath is where DuckLake writes Parquet: s3://bucket/prefix/ or
	// an absolute local path. Immutable after the catalog is created.
	DataPath string

	// S3 credentials/endpoint, reused from the existing S3_* settings.
	// Endpoint (e.g. http://localhost:9000) switches to path-style
	// addressing for MinIO/LocalStack.
	S3Region          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Endpoint        string

	// MemoryLimit caps DuckDB memory (e.g. "1GB"); empty uses the
	// DuckDB default. Size to the pod limit: inserts buffer + sort.
	MemoryLimit string
	// Threads caps DuckDB worker threads; zero uses the default.
	Threads int
	// TargetFileSize is DuckLake's target Parquet file size for writes
	// and compaction (e.g. "512MB"); empty keeps the DuckLake default.
	TargetFileSize string
	// Compression is the Parquet compression codec for writes/compaction:
	// "snappy" (default — fastest write, the throughput choice for this
	// write-heavy ingest node), "zstd" (~1.6x smaller files but ~30% slower
	// write), "lz4", or "uncompressed". Empty keeps the snappy default. The
	// write path is CPU-bound on the codec, so this is the main
	// materialize-throughput lever; bloom filters and dictionary encoding
	// (subject pruning) are codec-independent and preserved either way.
	Compression string
	// ParquetVersion is the Parquet format version DuckLake writes, "1" or
	// "2"; empty (or anything else) keeps the DuckLake default of 1. v2 turns
	// on DELTA_BINARY_PACKED / byte-stream-split encodings that shrink the
	// sorted (subject,"time") time-series layer on top of zstd. Only DuckDB
	// reads this lake, so v2 is compatibility-safe.
	ParquetVersion string
	// ExtensionDir overrides where DuckDB looks for/installs extensions
	// (pre-baked in the container image); empty uses the default.
	ExtensionDir string
	// TempDirectory is where DuckDB spills data that exceeds MemoryLimit
	// (large maintenance merges/compaction especially). Empty uses the
	// DuckDB default (the working directory) — point it at a sized spill
	// volume so a spill can't fill the container root fs and crash-loop the
	// pod. Mirrors dq's DUCKDB_TEMP_DIRECTORY.
	TempDirectory string
	// MaxConns bounds the embedded DuckDB connection pool. Zero means
	// a small default; size to writer count + maintenance.
	MaxConns int
}

// Lake is one embedded DuckDB instance with the catalog attached as "lake".
type Lake struct {
	db *sql.DB
}

// lakeConnMaxLifetime recycles pooled DuckDB connections by age so a poisoned
// catalog attach self-heals rather than persisting until the next deploy.
const lakeConnMaxLifetime = 30 * time.Minute

// Open starts DuckDB, attaches the DuckLake catalog, and bootstraps the
// schema. Bootstrapping is idempotent and safe to race across replicas:
// IF NOT EXISTS guards plus retries absorb conflicting catalog commits.
func Open(ctx context.Context, cfg Config) (*Lake, error) {
	if cfg.CatalogDSN == "" || cfg.DataPath == "" {
		return nil, fmt.Errorf("lake: CatalogDSN and DataPath are required")
	}
	// DuckDB creates a local catalog file but not its parent directory.
	if !isPostgresDSN(cfg.CatalogDSN) {
		if err := os.MkdirAll(filepath.Dir(cfg.CatalogDSN), 0o755); err != nil {
			return nil, fmt.Errorf("lake: creating catalog dir: %w", err)
		}
	}

	// TimeZone is SESSION-local in DuckDB and does NOT persist across pool/writer conns
	// the way the global pragmas (memory_limit, threads) and instance-global ATTACH/LOAD
	// do. Set it on EVERY connection via this init hook: without it a fresh or pinned
	// writer conn inherits the host tz, so day("time") partitioning is evaluated in that
	// tz and mis-partitions rows straddling a UTC day boundary — correct in prod only
	// because distroless defaults to UTC, but wrong on any non-UTC host (incl. local dev).
	// Mirrors dq's connInitFn (duck.go).
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		_, err := execer.ExecContext(context.Background(), "SET TimeZone = 'UTC'", nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("lake: opening duckdb: %w", err)
	}
	db := sql.OpenDB(connector)
	maxConns := cfg.MaxConns
	if maxConns == 0 {
		maxConns = 8
	}
	db.SetMaxOpenConns(maxConns)
	db.SetConnMaxIdleTime(0) // pinned writer conns must not be reaped
	// Recycle pooled connections by age so a maintainer connection whose
	// DuckLake→Postgres catalog attach is poisoned by a PG blip is dropped and
	// re-bootstrapped, instead of intermittently failing cycles forever (which
	// would also keep resetting the consecutive-failure restart backstop). Mirrors
	// dq's CHD-21. Safe for the writer: its conns are pinned via db.Conn() and stay
	// checked out, so the age reaper — which only touches idle/returned pool conns —
	// never reaps one mid-use.
	db.SetConnMaxLifetime(lakeConnMaxLifetime)

	l := &Lake{db: db}
	if err := l.bootstrap(ctx, cfg); err != nil {
		_ = db.Close()
		return nil, err
	}
	return l, nil
}

// DB exposes the embedded DuckDB pool with the lake catalog attached.
func (l *Lake) DB() *sql.DB { return l.db }

func (l *Lake) Close() error { return l.db.Close() }

func (l *Lake) bootstrap(ctx context.Context, cfg Config) error {
	var setup []string
	if cfg.ExtensionDir != "" {
		setup = append(setup, "SET extension_directory = "+sqlString(cfg.ExtensionDir))
	}
	if cfg.MemoryLimit != "" {
		setup = append(setup, "SET memory_limit = "+sqlString(cfg.MemoryLimit))
	}
	if cfg.TempDirectory != "" {
		// Spill to the sized volume, not the container root fs (a full root fs
		// crash-loops the pod). Pairs with memory_limit on the maintenance merge.
		setup = append(setup, "SET temp_directory = "+sqlString(cfg.TempDirectory))
	}
	if cfg.Threads > 0 {
		setup = append(setup, fmt.Sprintf("SET threads = %d", cfg.Threads))
	}
	// TimeZone (UTC) is set per-connection in the NewConnector init hook above — it is
	// session-local, so it must apply to every writer/pool conn, not just this bootstrap
	// one. day("time") then partitions on the UTC date regardless of host tz.
	// preserve_insertion_order=false lets DuckDB reorder rows for lower memory and
	// better parallelism on large appends/exports (DuckDB OOM guidance). Safe here:
	// raw_events is SORTED BY (subject,"time") so DuckLake orders rows on write, and
	// every read uses an explicit ORDER BY — nothing relies on implicit order.
	setup = append(setup, "SET preserve_insertion_order = false")
	setup = append(setup, "INSTALL ducklake", "LOAD ducklake")

	if isPostgresDSN(cfg.CatalogDSN) {
		setup = append(setup, "INSTALL postgres", "LOAD postgres")
	}
	if strings.HasPrefix(cfg.DataPath, "s3://") {
		setup = append(setup, "INSTALL httpfs", "LOAD httpfs", s3SecretSQL(cfg))
	}
	catalogDSN := withCatalogConnectTimeout(cfg.CatalogDSN)
	setup = append(setup, fmt.Sprintf("ATTACH IF NOT EXISTS %s AS lake (DATA_PATH %s)",
		sqlString(catalogURI(catalogDSN)), sqlString(cfg.DataPath)))

	// Consumer progress lives in a plain table in the catalog database —
	// not a DuckLake table (which would snapshot every cursor update). In
	// prod that's the same Postgres holding the catalog, attached directly
	// as `meta`; for a file catalog it's a sibling DuckDB file. See
	// consumer.go for how the expiry floor reads it.
	setup = append(setup, fmt.Sprintf("ATTACH IF NOT EXISTS %s AS meta%s",
		sqlString(metaTarget(catalogDSN)), metaAttachOpts(catalogDSN)))

	for _, q := range setup {
		if _, err := l.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("lake bootstrap %q: %w", redact(q), err)
		}
	}
	if _, err := l.db.ExecContext(ctx, consumerProgressDDL); err != nil {
		return fmt.Errorf("lake bootstrap consumer-progress table: %w", err)
	}
	if err := l.ensureSchema(ctx); err != nil {
		return err
	}
	return l.assertOptions(ctx, cfg)
}

// assertOptions sets the catalog-global write options on every boot, retrying
// to absorb metadata-commit conflicts when several replicas open the same
// catalog at once. Without the retry a transient set_option conflict during a
// cluster-wide cold start is fatal and CrashLoopBackOffs the pod (SR review #7).
func (l *Lake) assertOptions(ctx context.Context, cfg Config) error {
	return retryCatalog(ctx, func() error { return l.tryAssertOptions(ctx, cfg) })
}

// tryAssertOptions sets the catalog-global write options. DuckLake keeps these
// as catalog metadata (not data): the most recent value applies to new writes
// across every table — din's raw_events and dq's decoded tables alike — and
// re-setting an unchanged value mints no snapshot (TestOpen_BootstrapIdempotent
// guards that). Asserting them on every boot, rather than only when raw_events
// is first created, is what lets a changed option (e.g. raising parquet_version)
// reach a catalog that already exists.
func (l *Lake) tryAssertOptions(ctx context.Context, cfg Config) error {
	// name → already-SQL-formatted value (a quoted string or a bare literal).
	opts := []struct{ name, valueSQL string }{
		// snappy is the default: the parquet write is CPU-bound on the codec, and
		// snappy sustains ~30% higher materialize throughput than zstd on this
		// sorted time-series (at ~1.6x the file size). zstd remains available via
		// Config.Compression for storage-constrained deployments. Either way DuckDB
		// writes bloom filters on dictionary-encoded columns (subject), so subject
		// pruning is preserved; backfilled DIS bundles keep their own (zstd) codec
		// since compression is a per-file Parquet property.
		{"parquet_compression", sqlString(compressionLiteral(cfg.Compression))},
	}
	if v := parquetVersionLiteral(cfg.ParquetVersion); v != "" {
		opts = append(opts, struct{ name, valueSQL string }{"parquet_version", v})
	}
	if cfg.TargetFileSize != "" {
		opts = append(opts, struct{ name, valueSQL string }{"target_file_size", sqlString(cfg.TargetFileSize)})
	}
	for _, o := range opts {
		q := fmt.Sprintf("CALL lake.set_option(%s, %s)", sqlString(o.name), o.valueSQL)
		if _, err := l.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("lake set_option %s: %w", o.name, err)
		}
	}
	return nil
}

// parquetVersionLiteral validates the configured Parquet version and returns it
// as a bare SQL integer literal, or "" to keep DuckLake's default. Only "1" and
// "2" are valid; anything else (including empty) yields "" so a typo can neither
// inject SQL nor wedge boot.
func parquetVersionLiteral(v string) string {
	switch v {
	case "1", "2":
		return v
	default:
		return ""
	}
}

// compressionLiteral validates the configured Parquet codec and returns a known
// value, defaulting empty/unknown to "snappy". Like parquetVersionLiteral this
// keeps a typo'd LAKE_COMPRESSION from reaching set_option and wedging boot
// (a deterministic error retryCatalog would otherwise retry then crash on);
// sqlString already blocks injection, this blocks the unknown-codec footgun.
// Unlike parquetVersionLiteral (which returns "" so the option is omitted and
// DuckLake keeps its own default), this always returns a codec: snappy is din's
// deliberate default and must be pinned even when the operator sets nothing.
func compressionLiteral(v string) string {
	c := strings.ToLower(v)
	switch c {
	case "zstd", "lz4", "snappy", "uncompressed":
		return c
	default:
		return "snappy"
	}
}

// metaTarget is the ATTACH target for the side database holding consumer
// progress: the catalog Postgres DSN itself, or a DuckDB file beside a
// local catalog.
func metaTarget(catalogDSN string) string {
	if isPostgresDSN(catalogDSN) {
		return catalogDSN
	}
	return catalogDSN + ".progress.db"
}

func metaAttachOpts(catalogDSN string) string {
	if isPostgresDSN(catalogDSN) {
		return " (TYPE postgres)"
	}
	return ""
}

// retryCatalog runs a catalog bootstrap step up to 3 times, backing off between
// attempts, to absorb metadata-commit conflicts when several replicas open the
// same catalog concurrently (fresh-catalog DDL or a set_option write racing a
// peer). Shared by ensureSchema and assertOptions so neither is fatal on a
// cluster-wide cold start.
func retryCatalog(ctx context.Context, fn func() error) error {
	var attempt int
	for {
		err := fn()
		if err == nil {
			return nil
		}
		attempt++
		if attempt >= 3 {
			return err
		}
		select {
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ensureSchema creates the tables and their layout options on first boot.
// The existence check keeps reboots from minting pointless ALTER snapshots;
// retryCatalog absorbs metadata-commit conflicts when two replicas bootstrap a
// fresh catalog at once.
func (l *Lake) ensureSchema(ctx context.Context) error {
	return retryCatalog(ctx, func() error { return l.tryEnsureSchema(ctx) })
}

func (l *Lake) tryEnsureSchema(ctx context.Context) error {
	var n int
	err := l.db.QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ?`,
		RawTable).Scan(&n)
	if err != nil {
		return fmt.Errorf("lake: checking schema: %w", err)
	}
	if n > 0 {
		// The table exists. A crashed backfill may have left partitioning RESET
		// (its restore defer never ran), which the existence check would
		// otherwise make permanent — re-assert the layout when it is missing
		// (CHD-23). Catalog-global write options are handled by assertOptions.
		return l.reassertLayout(ctx)
	}
	// First boot: create the table and its layout. Catalog-global write options
	// (compression, parquet_version, target_file_size) are asserted separately
	// in assertOptions on every boot.
	for _, q := range rawEventsDDL {
		if _, err := l.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("lake schema %q: %w", q, err)
		}
	}
	return nil
}

// reassertLayout re-applies raw_events' partition + sort layout when it is
// currently missing (e.g. a crashed backfill left it RESET). It is idempotent
// and mints no snapshot when the layout is already active, so it is safe to call
// on every boot (ensureSchema) and at the top of every maintenance cycle. The
// latter repairs a crashed-backfill RESET promptly instead of letting the
// long-lived maintainer merge into unpartitioned files until the next pod boot
// (SR review #9).
func (l *Lake) reassertLayout(ctx context.Context) error {
	// A running backfill deliberately RESETs the partition layout for its
	// registration window (legacy bundles span multiple (type,day) per file). Skip
	// re-asserting while its heartbeat is fresh, so we don't re-partition the table
	// out from under it and abort the registration. A crashed backfill's heartbeat
	// goes stale and we resume — recovering the left-RESET layout, which is the
	// reason this runs every cycle.
	if paused, err := l.backfillPauseActive(ctx); err == nil && paused {
		return nil
	}
	partitioned, err := l.isPartitioned(ctx)
	if err != nil {
		// Can't tell — re-assert defensively (idempotent) rather than risk
		// leaving a reset unfixed.
		partitioned = false
	}
	if partitioned {
		return nil
	}
	for _, q := range rawEventsLayout {
		if _, err := l.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("lake re-assert layout %q: %w", q, err)
		}
	}
	return nil
}

// backfillPauseStaleness bounds how long a backfill's pause heartbeat is honored
// without a refresh. A live backfill refreshes it every batch (well under this);
// a crashed one goes stale within it and the maintainer resumes re-asserting.
const backfillPauseStaleness = 30 * time.Minute

// ensureBackfillPauseTable creates the single-row heartbeat table in the shared
// meta catalog (cross-process: the lake-backfill command and the maintenance pod
// coordinate through it).
func (l *Lake) ensureBackfillPauseTable(ctx context.Context) error {
	_, err := l.db.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS meta.lake_backfill_pause (updated_at TIMESTAMP WITH TIME ZONE)")
	return err
}

// heartbeatBackfillPause records/refreshes the pause heartbeat. Called before the
// backfill RESETs partitioning (closing the race where the maintainer sees a
// RESET table with no pause) and once per batch thereafter.
func (l *Lake) heartbeatBackfillPause(ctx context.Context) error {
	if err := l.ensureBackfillPauseTable(ctx); err != nil {
		return err
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM meta.lake_backfill_pause"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO meta.lake_backfill_pause (updated_at) VALUES (now())"); err != nil {
		return err
	}
	return tx.Commit()
}

// clearBackfillPause removes the heartbeat when a backfill finishes.
func (l *Lake) clearBackfillPause(ctx context.Context) error {
	if err := l.ensureBackfillPauseTable(ctx); err != nil {
		return err
	}
	_, err := l.db.ExecContext(ctx, "DELETE FROM meta.lake_backfill_pause")
	return err
}

// backfillPauseActive reports whether a backfill heartbeat exists and is fresh.
func (l *Lake) backfillPauseActive(ctx context.Context) (bool, error) {
	if err := l.ensureBackfillPauseTable(ctx); err != nil {
		return false, err
	}
	var active bool
	err := l.db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM meta.lake_backfill_pause WHERE now() - updated_at < INTERVAL '%d seconds')",
		int(backfillPauseStaleness.Seconds()))).Scan(&active)
	return active, err
}

// isPartitioned reports whether raw_events currently has an active partition
// spec. The catalog is attached as "lake", so DuckLake keeps its metadata in
// __ducklake_metadata_lake; a table with no active (end_snapshot IS NULL)
// partition row was reset (CHD-23).
func (l *Lake) isPartitioned(ctx context.Context) (bool, error) {
	// RESET leaves an active partition spec with zero columns, so count the
	// active partition COLUMNS, not the spec row.
	var ok bool
	err := l.db.QueryRowContext(ctx, `
		SELECT count(*) > 0
		FROM __ducklake_metadata_lake.ducklake_partition_column pc
		JOIN __ducklake_metadata_lake.ducklake_partition_info pi ON pc.partition_id = pi.partition_id
		WHERE pi.end_snapshot IS NULL
		  AND pi.table_id = (SELECT table_id FROM ducklake_table_info('lake') WHERE table_name = ?)`,
		RawTable).Scan(&ok)
	return ok, err
}

// catalogURI maps a config DSN onto a ducklake ATTACH URI.
func catalogURI(dsn string) string {
	if isPostgresDSN(dsn) {
		return "ducklake:postgres:" + dsn
	}
	return "ducklake:" + dsn
}

func isPostgresDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") ||
		strings.Contains(dsn, "host=") || strings.Contains(dsn, "dbname=")
}

// pgCatalogConnectTimeout bounds the libpq connect on the catalog/meta ATTACH (seconds).
const pgCatalogConnectTimeout = 10

// withCatalogConnectTimeout adds a libpq connect_timeout to a Postgres catalog DSN so a
// boot against an unreachable catalog fails in seconds — letting the pod restart and
// retry — instead of blocking on the OS TCP timeout. A no-op when the operator already
// set one or for a file catalog, and it preserves isPostgresDSN (it only appends).
func withCatalogConnectTimeout(dsn string) string {
	if !isPostgresDSN(dsn) || strings.Contains(dsn, "connect_timeout") {
		return dsn
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		sep := "?"
		if strings.ContainsRune(dsn, '?') {
			sep = "&"
		}
		return fmt.Sprintf("%s%sconnect_timeout=%d", dsn, sep, pgCatalogConnectTimeout)
	}
	return fmt.Sprintf("%s connect_timeout=%d", dsn, pgCatalogConnectTimeout)
}

// s3SecretSQL builds the DuckDB secret for the DataPath bucket. Static
// credentials when configured, otherwise the SDK default chain (IRSA et
// al.) — mirroring s3client.New.
func s3SecretSQL(cfg Config) string {
	parts := []string{"TYPE s3"}
	if cfg.S3AccessKeyID != "" {
		parts = append(parts,
			"PROVIDER config",
			"KEY_ID "+sqlString(cfg.S3AccessKeyID),
			"SECRET "+sqlString(cfg.S3SecretAccessKey))
	} else {
		parts = append(parts, "PROVIDER credential_chain")
	}
	if cfg.S3Region != "" {
		parts = append(parts, "REGION "+sqlString(cfg.S3Region))
	}
	if cfg.S3Endpoint != "" {
		host := cfg.S3Endpoint
		useSSL := true
		if u, err := url.Parse(cfg.S3Endpoint); err == nil && u.Host != "" {
			host = u.Host
			useSSL = u.Scheme != "http"
		}
		parts = append(parts,
			"ENDPOINT "+sqlString(host),
			"URL_STYLE 'path'",
			fmt.Sprintf("USE_SSL %t", useSSL))
	}
	return "CREATE OR REPLACE SECRET lake_s3 (" + strings.Join(parts, ", ") + ")"
}

// sqlString quotes v as a SQL string literal.
func sqlString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// redact hides credential-bearing values — the quoted DSN in ATTACH, the secret body
// in CREATE SECRET — from error messages, while keeping the statement shape (notably
// the `AS lake` / `AS meta` alias) so a failed bootstrap says WHICH attach failed
// instead of collapsing both to an identical "ATTACH IF NOT EXISTS (…)".
func redact(q string) string {
	if !strings.Contains(q, "SECRET") && !strings.Contains(q, "ATTACH") {
		return q
	}
	var b strings.Builder
	inQuote := false
	for _, r := range q {
		switch {
		case r == '\'' && !inQuote:
			b.WriteString("'…'") // open quote: emit the redacted placeholder once
			inQuote = true
		case r == '\'' && inQuote:
			inQuote = false // close quote: the literal between was dropped
		case !inQuote:
			b.WriteRune(r)
		}
	}
	return b.String()
}
