package main

import (
	"testing"
	"time"

	"github.com/DIMO-Network/din/internal/config"
)

// maintConfig must copy every maintenance-tuning field from settings — in
// particular ConsumerStaleness (LAKE_CONSUMER_STALENESS), which was previously
// dropped, silently pinning the consumer-protection floor to its 1h default
// and making it impossible to widen the window for a lagging dq (SR-2).
func TestMaintConfigFromSettings(t *testing.T) {
	s := config.Settings{
		LakeMaintInterval:     5 * time.Minute,
		LakeSnapshotKeep:      48 * time.Hour,
		LakeConsumerStaleness: 2 * time.Hour,
	}
	got := maintConfig(s)
	if got.Interval != s.LakeMaintInterval {
		t.Errorf("Interval = %v, want %v", got.Interval, s.LakeMaintInterval)
	}
	if got.SnapshotKeep != s.LakeSnapshotKeep {
		t.Errorf("SnapshotKeep = %v, want %v", got.SnapshotKeep, s.LakeSnapshotKeep)
	}
	if got.ConsumerStaleness != s.LakeConsumerStaleness {
		t.Errorf("ConsumerStaleness = %v, want %v", got.ConsumerStaleness, s.LakeConsumerStaleness)
	}
}
