package config

import (
	"testing"
	"time"
)

// S3_KMS_KEY_ID must be a KMS ARN: DuckLake's S3 secret KMS_KEY_ID wants the full
// ARN form, so a bare key-id or typo is rejected at config load rather than 400ing
// at runtime (or CrashLooping the write tier).
func TestIsKMSKeyARN(t *testing.T) {
	for in, want := range map[string]bool{
		"arn:aws:kms:us-east-2:123456789012:key/abc-123":  true,
		"arn:aws:kms:us-east-2:123456789012:alias/my-key": true,
		"arn:aws-us-gov:kms:us-gov-west-1:1:key/x":        true,
		"arn:aws-cn:kms:cn-north-1:1:key/x":               true,
		"abc-123-bare-key-id":                             false,
		"alias/my-key":                                    false,
		"arn:aws:s3:::bucket":                             false, // ARN but not KMS
		"":                                                false,
	} {
		if got := isKMSKeyARN(in); got != want {
			t.Errorf("isKMSKeyARN(%q) = %v, want %v", in, got, want)
		}
	}
}

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
