package lake

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isCommitConflict must retry ONLY the transient DuckLake commit-conflict class
// (load review #6). A deterministic ErrPoisonRow — even one whose message
// happens to contain "conflict" — must never be classified as retryable, or the
// write path would spin on an unpersistable row instead of handing it to the
// sink's isolate/terminate path.
func TestIsCommitConflict(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"transaction context error retries", errors.New("TransactionContext Error: Failed to commit: conflict on raw_events"), true},
		{"wrapped conflict retries", fmt.Errorf("lake write commit: %w", errors.New("write-write Conflict detected")), true},
		{"poison row never retries", fmt.Errorf("lake row 3: %w: invalid utf-8", ErrPoisonRow), false},
		{"poison row mentioning conflict still never retries", fmt.Errorf("%w: conflict-shaped payload", ErrPoisonRow), false},
		{"generic transient error fails fast", errors.New("s3: connection reset by peer"), false},
		{"nil is not a conflict", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isCommitConflict(tc.err))
		})
	}
}

// retryCatalogIf is the retry WriteBundle wraps its BEGIN/append/COMMIT in. It
// must retry a transient conflict (which a lost commit race clears) but attempt
// a deterministic error exactly once.
func TestRetryCatalogIf_ConflictVsDeterministic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("transient conflict retried then succeeds", func(t *testing.T) {
		calls := 0
		err := retryCatalogIf(ctx, func() error {
			calls++
			if calls < 3 {
				return errors.New("TransactionContext Error: conflict")
			}
			return nil
		}, isCommitConflict)
		require.NoError(t, err)
		assert.Equal(t, 3, calls, "a clearing conflict must retry until it succeeds")
	})

	t.Run("deterministic error fails fast", func(t *testing.T) {
		calls := 0
		err := retryCatalogIf(ctx, func() error {
			calls++
			return fmt.Errorf("lake row 0: %w: bad", ErrPoisonRow)
		}, isCommitConflict)
		require.ErrorIs(t, err, ErrPoisonRow)
		assert.Equal(t, 1, calls, "a deterministic error must be attempted exactly once — no retry")
	})

	t.Run("persistent conflict exhausts the 3-attempt cap", func(t *testing.T) {
		calls := 0
		err := retryCatalogIf(ctx, func() error {
			calls++
			return errors.New("TransactionContext Error: conflict")
		}, isCommitConflict)
		require.Error(t, err)
		assert.Equal(t, 3, calls)
	})
}
