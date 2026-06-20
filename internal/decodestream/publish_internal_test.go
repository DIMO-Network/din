package decodestream

import "testing"

// publishJSONAsync must return a marshal error rather than panicking when the
// payload can't be encoded. The old mustJSON panicked, and publishJSONAsync runs
// (via handle) in the per-partition fetch goroutine with no recover — a single
// unmarshalable payload would take down the whole bridge (SR-19).
func TestPublishJSONAsync_MarshalErrorReturns(t *testing.T) {
	b := &Bridge{}
	if _, err := b.publishJSONAsync("signal.x", make(chan int), "dedup"); err == nil {
		t.Fatal("expected a marshal error, got nil")
	}
}
