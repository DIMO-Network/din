package lake

import (
	"context"
	"database/sql"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// InstallExtensions downloads the DuckDB extensions din needs into dir.
// Run at image build time so pods never fetch extensions over the
// network at startup; point LAKE_EXTENSION_DIR at the same dir.
func InstallExtensions(ctx context.Context, dir string) error {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return fmt.Errorf("opening duckdb: %w", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck

	for _, q := range []string{
		"SET extension_directory = " + sqlString(dir),
		"INSTALL ducklake",
		"INSTALL postgres",
		"INSTALL httpfs",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}
