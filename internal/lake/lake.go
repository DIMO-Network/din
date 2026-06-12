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
	// ExtensionDir overrides where DuckDB looks for/installs extensions
	// (pre-baked in the container image); empty uses the default.
	ExtensionDir string
	// MaxConns bounds the embedded DuckDB connection pool. Zero means
	// a small default; size to writer count + maintenance.
	MaxConns int
}

// Lake is one embedded DuckDB instance with the catalog attached as "lake".
type Lake struct {
	db *sql.DB
}

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

	connector, err := duckdb.NewConnector("", nil)
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
	if cfg.Threads > 0 {
		setup = append(setup, fmt.Sprintf("SET threads = %d", cfg.Threads))
	}
	// Naive timestamps throughout: device times are UTC by contract and
	// the table column is zone-less TIMESTAMP.
	setup = append(setup, "SET TimeZone = 'UTC'")
	setup = append(setup, "INSTALL ducklake", "LOAD ducklake")

	if isPostgresDSN(cfg.CatalogDSN) {
		setup = append(setup, "INSTALL postgres")
	}
	if strings.HasPrefix(cfg.DataPath, "s3://") {
		setup = append(setup, "INSTALL httpfs", "LOAD httpfs", s3SecretSQL(cfg))
	}
	setup = append(setup, fmt.Sprintf("ATTACH IF NOT EXISTS %s AS lake (DATA_PATH %s)",
		sqlString(catalogURI(cfg.CatalogDSN)), sqlString(cfg.DataPath)))

	for _, q := range setup {
		if _, err := l.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("lake bootstrap %q: %w", redact(q), err)
		}
	}
	return l.ensureSchema(ctx, cfg)
}

// ensureSchema creates the tables and their layout options on first boot.
// The existence check keeps reboots from minting pointless ALTER snapshots;
// the retry loop absorbs metadata-commit conflicts when two replicas
// bootstrap a fresh catalog at once.
func (l *Lake) ensureSchema(ctx context.Context, cfg Config) error {
	var attempt int
	for {
		err := l.tryEnsureSchema(ctx, cfg)
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

func (l *Lake) tryEnsureSchema(ctx context.Context, cfg Config) error {
	var n int
	err := l.db.QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ?`,
		RawTable).Scan(&n)
	if err != nil {
		return fmt.Errorf("lake: checking schema: %w", err)
	}
	if n > 0 {
		return nil
	}
	ddl := append([]string{}, rawEventsDDL...)
	if cfg.TargetFileSize != "" {
		ddl = append(ddl, fmt.Sprintf("CALL lake.set_option('target_file_size', %s)",
			sqlString(cfg.TargetFileSize)))
	}
	for _, q := range ddl {
		if _, err := l.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("lake schema %q: %w", q, err)
		}
	}
	return nil
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

// redact hides credential-bearing statements from error messages.
func redact(q string) string {
	if strings.Contains(q, "SECRET") || strings.Contains(q, "ATTACH") {
		if i := strings.IndexAny(q, "('"); i > 0 {
			return q[:i] + "(…)"
		}
	}
	return q
}
