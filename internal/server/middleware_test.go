package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteLimiter_BoundedKeys(t *testing.T) {
	t.Parallel()
	l := newRemoteLimiter(1000, 5)
	for i := range limiterMaxKeys + 5000 {
		l.allow(fmt.Sprintf("key-%d", i))
	}
	total := 0
	for i := range l.shards {
		l.shards[i].mu.Lock()
		total += len(l.shards[i].limiters)
		l.shards[i].mu.Unlock()
	}
	require.LessOrEqual(t, total, limiterMaxKeys, "limiter map stays bounded")
}

func TestRemoteLimiter_EvictionKeepsHotBuckets(t *testing.T) {
	t.Parallel()
	l := newRemoteLimiter(0.001, 2) // hot buckets refill ~never
	// Drain one bucket so it is visibly hot.
	require.True(t, l.allow("hot"))
	require.True(t, l.allow("hot"))
	require.False(t, l.allow("hot"))

	s := l.shard("hot")
	s.mu.Lock()
	s.evictLocked(l.burst)
	_, hotKept := s.limiters["hot"]
	s.mu.Unlock()
	require.True(t, hotKept, "non-refilled bucket survives idle eviction")
	require.False(t, l.allow("hot"), "hot remote stays limited across eviction")
}
