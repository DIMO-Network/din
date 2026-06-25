package lake

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// pgCatalogDataPath returns the DuckLake DATA_PATH for Postgres-catalog tests. DuckLake
// binds DATA_PATH into the catalog permanently, so every test sharing one catalog DSN
// MUST use the same path or the second ATTACH fails with a DATA_PATH mismatch. It's a
// single package-level dir (so all PG tests in a run agree), overridable with
// LAKE_TEST_PG_DATA_PATH to re-run against a persistent catalog (set it to the path the
// catalog already recorded).
var (
	pgDataPathOnce sync.Once
	pgDataPath     string
	pgDataPathErr  error
)

func pgCatalogDataPath(t *testing.T) string {
	t.Helper()
	pgDataPathOnce.Do(func() {
		if d := os.Getenv("LAKE_TEST_PG_DATA_PATH"); d != "" {
			pgDataPath = d
			return
		}
		pgDataPath, pgDataPathErr = os.MkdirTemp("", "din-pg-catalog-data")
	})
	require.NoError(t, pgDataPathErr)
	return pgDataPath
}

// TestMain removes the shared PG-catalog data dir we created (but not an operator-supplied
// LAKE_TEST_PG_DATA_PATH, which they own).
func TestMain(m *testing.M) {
	code := m.Run()
	if pgDataPath != "" && os.Getenv("LAKE_TEST_PG_DATA_PATH") == "" {
		_ = os.RemoveAll(pgDataPath)
	}
	os.Exit(code)
}
