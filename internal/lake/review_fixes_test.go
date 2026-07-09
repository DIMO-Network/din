package lake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SR review #6: native writes must truncate "time" to milliseconds so a row and
// the same event registered from a millisecond-precision backfilled DIS bundle
// carry an identical value and reader dedup collapses them.
func TestRowArgs_TruncatesTimeToMillis(t *testing.T) {
	t.Parallel()
	ev := testEvent("e1", "dimo.status", "did:1", time.Date(2026, 6, 8, 10, 0, 0, 123456789, time.UTC))
	args, err := rowArgs(&ev, time.Now().UTC())
	require.NoError(t, err)
	got, ok := args[1].(time.Time) // column order: subject, "time", ...
	require.True(t, ok, "second column must be the timestamp")
	want := time.Date(2026, 6, 8, 10, 0, 0, 123000000, time.UTC) // 123ms, sub-ms dropped
	assert.True(t, got.Equal(want), "time not truncated to ms: want %v got %v", want, got)
}

// SR review #2: a consumer present in the progress table but past the staleness
// window is reported as stale (distinct from never-reported / caught-up), and
// the snapshots past its cursor that time-only expiry will reclaim are counted
// so the data-loss event is observable rather than silent.
func TestStaleConsumers_AndUnconsumedExpiring(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck
	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	require.NoError(t, w.WriteBundle(ctx, []cloudevent.StoredEvent{testEvent("a", "dimo.status", "did:1", ts)}))
	require.NoError(t, w.WriteBundle(ctx, []cloudevent.StoredEvent{testEvent("b", "dimo.status", "did:1", ts)}))

	// A consumer that last reported two hours ago, at snapshot 0.
	_, err = l.DB().ExecContext(ctx,
		"INSERT INTO meta.din_consumer_progress VALUES ('dq', 0, now() - INTERVAL '2 hours')")
	require.NoError(t, err)

	stale, err := l.StaleConsumers(ctx, time.Hour)
	require.NoError(t, err)
	require.Len(t, stale, 1)
	assert.Equal(t, "dq", stale[0].Name)
	assert.Greater(t, stale[0].AgeSeconds, float64(3000))

	// Cutoff = now: every snapshot past the dropped consumer's cursor (0) is older
	// than now, so all are reclaimable — this is the loss the counter tracks.
	var nowEpoch float64
	require.NoError(t, l.DB().QueryRowContext(ctx, "SELECT epoch(now())").Scan(&nowEpoch))
	lost, err := l.UnconsumedExpiringCount(ctx, stale[0].SnapshotID, nowEpoch)
	require.NoError(t, err)
	assert.Positive(t, lost, "snapshots past a dropped consumer's cursor must be counted")

	// A consumer that just reported is not stale.
	require.NoError(t, l.RecordConsumerProgress(ctx, "dq2", 1))
	stale, err = l.StaleConsumers(ctx, time.Hour)
	require.NoError(t, err)
	for _, c := range stale {
		assert.NotEqual(t, "dq2", c.Name, "a fresh consumer must not be reported stale")
	}
}

// redact must hide the credential-bearing literal (DSN / secret body) from a boot
// error while keeping the statement shape — notably the AS lake/meta alias — so an
// operator can tell which attach failed.
func TestRedact_HidesSecretsKeepsAlias(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		mustHave    []string
		mustNotHave []string
	}{
		{
			"attach lake redacts dsn, keeps alias",
			"ATTACH IF NOT EXISTS 'ducklake:postgres:host=p password=topsecret' AS lake (DATA_PATH 's3://b/lake')",
			[]string{"AS lake", "…"},
			[]string{"password", "topsecret", "host=p"},
		},
		{
			"attach meta keeps alias and type",
			"ATTACH IF NOT EXISTS 'x' AS meta (TYPE postgres)",
			[]string{"AS meta", "TYPE postgres"},
			[]string{"'x'"},
		},
		{
			"create secret redacts keys",
			"CREATE SECRET (TYPE s3, KEY_ID 'AKIAEXAMPLE', SECRET 'shh')",
			[]string{"CREATE SECRET", "TYPE s3"},
			[]string{"AKIAEXAMPLE", "shh"},
		},
		{
			// A SQL-escaped quote ('') inside a value must not desync the scrubber
			// and unmask the rest of the secret (or the following literal).
			"escaped quote inside value stays hidden",
			"CREATE SECRET (KEY_ID 'AK''IA', SECRET 'sh''hh')",
			[]string{"CREATE SECRET"},
			[]string{"AK", "IA", "shh", "hh"},
		},
		{
			"non-secret statement untouched",
			"SELECT 1",
			[]string{"SELECT 1"},
			nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redact(tc.in)
			for _, s := range tc.mustHave {
				assert.Contains(t, got, s, "redacted=%q", got)
			}
			for _, s := range tc.mustNotHave {
				assert.NotContains(t, got, s, "redacted=%q LEAKED %q", got, s)
			}
		})
	}
}

// redactErr scrubs quoted literals out of a DuckDB driver error for
// credential-bearing statements (the driver often echoes the offending
// statement), while leaving ordinary statement errors intact.
func TestRedactErr_ScrubsCredentialErrors(t *testing.T) {
	secretStmt := "CREATE SECRET (KEY_ID 'AKIAEXAMPLE', SECRET 'shh')"
	driverErr := errors.New("Parser Error near 'AKIAEXAMPLE': bad secret")
	got := redactErr(secretStmt, driverErr)
	require.Error(t, got)
	assert.NotContains(t, got.Error(), "AKIAEXAMPLE", "credential leaked via wrapped driver error: %q", got)

	// ATTACH carries the catalog DSN password; when the driver echoes the quoted
	// statement fragment (the common leak vector) it's scrubbed too. Unquoted
	// echoes are out of scope — this is defense-in-depth, not a guarantee.
	attachErr := redactErr("ATTACH 'postgres:password=topsecret' AS lake", errors.New("Binder Error: near 'postgres:password=topsecret'"))
	assert.NotContains(t, attachErr.Error(), "topsecret")

	// A non-credential statement keeps its original error untouched.
	plain := errors.New("boom 'literal'")
	assert.Equal(t, plain, redactErr("SELECT 1", plain))

	// nil stays nil.
	assert.NoError(t, redactErr(secretStmt, nil))
}
