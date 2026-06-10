package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteLimiter_BoundedKeys(t *testing.T) {
	t.Parallel()
	l := newRemoteLimiter(1000, 5)
	for i := range limiterMaxKeys + 500 {
		l.allow(fmt.Sprintf("key-%d", i))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	require.LessOrEqual(t, len(l.limiters), limiterMaxKeys, "limiter map stays bounded")
}

func TestRemoteLimiter_EvictionKeepsHotBuckets(t *testing.T) {
	t.Parallel()
	l := newRemoteLimiter(0.001, 2) // hot buckets refill ~never
	// Drain one bucket so it is visibly hot.
	require.True(t, l.allow("hot"))
	require.True(t, l.allow("hot"))
	require.False(t, l.allow("hot"))

	l.mu.Lock()
	l.evictLocked()
	_, hotKept := l.limiters["hot"]
	l.mu.Unlock()
	require.True(t, hotKept, "non-refilled bucket survives idle eviction")
	require.False(t, l.allow("hot"), "hot remote stays limited across eviction")
}
