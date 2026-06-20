package config

import (
	"testing"
	"time"
)

// ConsumerStaleness must stay below SnapshotKeep. Otherwise a consumer can be
// un-reported for longer than snapshots are retained, so the expiry floor can
// never protect its cursor and ranges it hasn't read are reclaimed (SR-15).
func TestValidateMaintenance_StalenessBelowSnapshotKeep(t *testing.T) {
	cases := []struct {
		name      string
		staleness time.Duration
		keep      time.Duration
		wantErr   bool
	}{
		{"below is valid", time.Hour, 72 * time.Hour, false},
		{"equal is invalid", 72 * time.Hour, 72 * time.Hour, true},
		{"above is invalid", 96 * time.Hour, 72 * time.Hour, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Settings{LakeConsumerStaleness: tc.staleness, LakeSnapshotKeep: tc.keep}
			err := s.validateMaintenance()
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateMaintenance() err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
