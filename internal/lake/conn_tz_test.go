package lake

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLake_EveryConnPinnedUTC pins the NewConnector init-hook fix: TimeZone is
// session-local in DuckDB, so it must be set on EVERY pool/writer connection, not
// just the bootstrap one. Otherwise a fresh or pinned writer conn inherits the host
// tz and day("time") partitioning mis-buckets rows that straddle a UTC day boundary.
// A non-UTC TZ is forced so this catches the bug even on a UTC CI runner.
func TestLake_EveryConnPinnedUTC(t *testing.T) {
	t.Setenv("TZ", "America/New_York")
	l, _ := openTestLake(t)

	ctx := context.Background()
	// Hold several DISTINCT connections open at once so each is a separate pool conn,
	// not the same one reused — every one must report UTC.
	const n = 4
	conns := make([]*sql.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := l.DB().Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		conns = append(conns, c)
		var tz string
		require.NoError(t, c.QueryRowContext(ctx, "SELECT current_setting('TimeZone')").Scan(&tz))
		require.Equalf(t, "UTC", tz, "pool conn %d must be pinned to UTC by the NewConnector init hook", i)
	}
}
