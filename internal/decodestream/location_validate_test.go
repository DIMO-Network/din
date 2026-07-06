package decodestream

import (
	"math"
	"testing"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// TestValidLatLon pins the WGS-84 / finite / non-origin coordinate guard added
// so din never lets a pathological position reach storage or DIMO_SIGNALS.
func TestValidLatLon(t *testing.T) {
	cases := []struct {
		name     string
		lat, lon float64
		want     bool
	}{
		{"valid", 37.7749, -122.4194, true},
		{"origin null island", 0, 0, false},
		{"lat over", 90.001, 10, false},
		{"lat under", -90.001, 10, false},
		{"lon over", 10, 180.001, false},
		{"lon under", 10, -180.001, false},
		{"lat NaN", math.NaN(), 10, false},
		{"lon NaN", 10, math.NaN(), false},
		{"lat +Inf", math.Inf(1), 10, false},
		{"lon -Inf", 10, math.Inf(-1), false},
		{"boundary 90/180", 90, 180, true},
		{"boundary -90/-180", -90, -180, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := validLatLon(tc.lat, tc.lon); got != tc.want {
				t.Fatalf("validLatLon(%v,%v)=%v want %v", tc.lat, tc.lon, got, tc.want)
			}
		})
	}
}

// TestHandleCoordinates_PrunesInvalid proves an out-of-range coordinate pair is
// pruned (no location emitted), exactly like the origin (0,0) pair.
func TestHandleCoordinates_PrunesInvalid(t *testing.T) {
	ts := time.Now().UTC()
	mk := func(name string, v float64) vss.Signal {
		return vss.Signal{Data: vss.SignalData{Name: name, Timestamp: ts, ValueNumber: v}}
	}
	out, err := handleCoordinates([]vss.Signal{
		mk(fieldCurrentLocationLatitude, 5000), // out of WGS-84 range
		mk(fieldCurrentLocationLongitude, 10),
	})
	if err == nil {
		t.Fatal("expected an error for the invalid coordinate")
	}
	for _, s := range out {
		if s.Data.Name == vss.FieldCurrentLocationCoordinates {
			t.Fatalf("invalid coordinate must not produce a location signal: %+v", s)
		}
	}
}

// TestHandleCoordinates_HDOPOnlyEmitsNoLocation pins D6: an HDOP-only triple
// (latitude/longitude absent) must NOT emit a currentLocationCoordinates — it
// previously produced a location at the (0,0) null island.
func TestHandleCoordinates_HDOPOnlyEmitsNoLocation(t *testing.T) {
	ts := time.Now().UTC()
	out, err := handleCoordinates([]vss.Signal{
		{Data: vss.SignalData{Name: fieldDIMOAftermarketHDOP, Timestamp: ts, ValueNumber: 1.5}},
	})
	if err != nil {
		t.Fatalf("HDOP alone is not an error condition: %v", err)
	}
	for _, s := range out {
		if s.Data.Name == vss.FieldCurrentLocationCoordinates {
			t.Fatalf("HDOP-only input must not emit a location signal: %+v", s)
		}
	}
}

// TestHandleCoordinates_ValidTripleEmitsLocationWithHDOP guards the merge path:
// a valid lat+lon pair with HDOP still yields one location carrying all three.
func TestHandleCoordinates_ValidTripleEmitsLocationWithHDOP(t *testing.T) {
	ts := time.Now().UTC()
	out, err := handleCoordinates([]vss.Signal{
		{Data: vss.SignalData{Name: fieldCurrentLocationLatitude, Timestamp: ts, ValueNumber: 37.7749}},
		{Data: vss.SignalData{Name: fieldCurrentLocationLongitude, Timestamp: ts, ValueNumber: -122.4194}},
		{Data: vss.SignalData{Name: fieldDIMOAftermarketHDOP, Timestamp: ts, ValueNumber: 1.5}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var locs []vss.Signal
	for _, s := range out {
		if s.Data.Name == vss.FieldCurrentLocationCoordinates {
			locs = append(locs, s)
		}
	}
	if len(locs) != 1 {
		t.Fatalf("expected exactly one location signal, got %d", len(locs))
	}
	loc := locs[0].Data.ValueLocation
	if loc.Latitude != 37.7749 || loc.Longitude != -122.4194 || loc.HDOP != 1.5 {
		t.Fatalf("location did not carry lat/lon/hdop: %+v", loc)
	}
}
