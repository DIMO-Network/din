package lake

import (
	"context"
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
	// Grab n DISTINCT connections and hold them open (close only at cleanup) so each is
	// a separate pool conn, not the same one reused — every one must report UTC.
	const n = 4
	for i := range n {
		c, err := l.DB().Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		var tz string
		require.NoError(t, c.QueryRowContext(ctx, "SELECT current_setting('TimeZone')").Scan(&tz))
		require.Equalf(t, "UTC", tz, "pool conn %d must be pinned to UTC by the NewConnector init hook", i)
	}
}
