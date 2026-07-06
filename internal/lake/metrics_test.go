package lake

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestMaintenanceMetricsLazyRegistration pins C2: importing package lake must NOT
// export din_lake_* maintenance series — an ingest pod exporting
// din_lake_last_successful_cycle_timestamp_seconds{}=0 defeats
// DinMaintenanceDown's absent_over_time (never absent while any ingest pod is up)
// and poisons the max()-fallback alerts during a real maintenance outage. Only
// constructing a Maintainer registers the set. Runs in a subprocess because
// sibling tests in this binary construct Maintainers and would register it first.
func TestMaintenanceMetricsLazyRegistration(t *testing.T) {
	if os.Getenv("DIN_MAINT_METRICS_SUBPROCESS") == "1" {
		families, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, f := range families {
			require.False(t, strings.HasPrefix(f.GetName(), "din_lake_"),
				"%s exported before any Maintainer was constructed (C2 regression)", f.GetName())
		}

		// A nil Lake is fine: NewMaintainer only registers metrics and stores the
		// pointer; it does not touch the lake.
		_ = NewMaintainer(nil, MaintConfig{}, zerolog.Nop())

		families, err = prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		var found bool
		for _, f := range families {
			if f.GetName() == "din_lake_last_successful_cycle_timestamp_seconds" {
				found = true
			}
		}
		require.True(t, found, "constructing a Maintainer must register din_lake_* metrics")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestMaintenanceMetricsLazyRegistration$", "-test.v")
	cmd.Env = append(os.Environ(), "DIN_MAINT_METRICS_SUBPROCESS=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "subprocess failed:\n%s", out)
}
