package lake

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionSafeTime(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		in      time.Time
		wantNow bool
	}{
		{"epoch-0 (unset RTC)", time.Unix(0, 0).UTC(), true},
		{"1980 gps-week rollover", time.Date(1980, 1, 6, 0, 0, 0, 0, time.UTC), true},
		{"2000 factory default", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), true},
		{"2099 runaway", time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), true},
		{"20d offline buffer", now.AddDate(0, 0, -20), false},
		{"2m clock skew", now.Add(2 * time.Minute), false},
		{"12h dis-parity near-future", now.Add(12 * time.Hour), false},
		{"exactly now", now, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := partitionSafeTime(tc.in, now)
			if tc.wantNow {
				assert.True(t, got.Equal(now), "garbage time must clamp to now, got %s", got)
			} else {
				assert.True(t, got.Equal(tc.in), "in-window time must be untouched, got %s", got)
			}
		})
	}
}

// TestRowArgs_ClampsStoredTimeNotHeader proves scale-review #1: the broken-clock clamp
// bounds the raw_events partition key but leaves the CloudEvent HEADER time untouched, so
// the NATS MsgID and decodestream dedup id (which hash the header time) are unaffected and a
// retry still dedups instead of double-firing vehicle-triggers.
func TestRowArgs_ClampsStoredTimeNotHeader(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	garbage := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)

	ev := testEvent("g1", "dimo.status", "did:1", garbage)
	args, err := rowArgs(&ev, now)
	require.NoError(t, err)
	assert.True(t, args[1].(time.Time).Equal(now), "stored partition time must clamp to now, got %v", args[1])
	assert.True(t, ev.Time.Equal(garbage), "HEADER time must be untouched (MsgID/dedup identity), got %v", ev.Time)

	legit := now.AddDate(0, 0, -20)
	ev2 := testEvent("g2", "dimo.status", "did:1", legit)
	args2, err := rowArgs(&ev2, now)
	require.NoError(t, err)
	assert.True(t, args2[1].(time.Time).Equal(legit.Truncate(time.Millisecond)), "legit time stored as-is (ms-truncated), got %v", args2[1])
	assert.True(t, ev2.Time.Equal(legit))
}
