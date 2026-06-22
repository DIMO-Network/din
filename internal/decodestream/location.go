package decodestream

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/vss"
)

// maxLatLongDur is the amount of time we'll wait before starting
// a new coordinate triple.
//
// TODO(elffjs): Should this be a setting?
const maxLatLongDur = 500 * time.Millisecond

// zeroTime is used to reset the timestamp on the coordinate store.
var zeroTime time.Time

// We removed these from the standard set but they're still used
// by AutoPi and Macaron, so we need to keep them around.
const (
	fieldCurrentLocationLatitude  = "currentLocationLatitude"
	fieldCurrentLocationLongitude = "currentLocationLongitude"
	fieldDIMOAftermarketHDOP      = "dimoAftermarketHDOP"
)

// handleCoordinates transforms a slice of input signals in ways that
// simplify downstream processing. Currently this means:
//
//   - Remove location values with latitude and longitude both equal
//     to zero.
//   - Roughly, for each triple of the input signals named
//     currentLocationLatitude, currentLocationLongitude, and
//     dimoAftermarketHDOP with sufficiently
//     close timestamps, we will also emit a location-values signal
//     named currentLocationCoordinates which combines all three.
//   - Remove unpaired latitudes and longitudes.
//
// The returned slice of signals is always meaningful, even if an error
// is also returned.
//
// Note that this function may reorder the input slice.
func handleCoordinates(signals []vss.Signal) ([]vss.Signal, error) {
	return newCoordinateStore(signals).processSignals()
}

func newCoordinateStore(signals []vss.Signal) *coordinateStore {
	return &coordinateStore{
		signals:  signals,
		lastLat:  -1,
		lastLon:  -1,
		lastHDOP: -1,
	}
}

type coordinateStore struct {
	// lastLat is the index of the signals slice holding latitude for
	// the location triple under construction. If there is no latitude
	// yet found for the triple then the value of lastLat is -1.
	lastLat int
	// lastLon is like lastLat for longitude.
	lastLon int
	// lastHDOP is like lastLat for HDOP.
	lastHDOP int
	// lastTime is the timestamp of the earliest signal in the active
	// triple. If we have no parts for the active triple then this
	// will be the zero value of time.Time.
	lastTime time.Time

	// signals is the input slice of signals.
	signals []vss.Signal

	// created holds location signals that we've constructed while
	// iterating over signals.
	created []vss.Signal
	// errs contains errors arising from location construction.
	// Typically these have to do with unpaired coordinates, or
	// latitude = longitude = 0.
	errs []error
}

func (c *coordinateStore) processSignals() ([]vss.Signal, error) {
	if len(c.signals) == 0 {
		return c.signals, nil
	}

	// Sorting this way makes it easier to handle time gaps. Sorting
	// thereafter by name is not strictly necessary but improves
	// reproducibility. Typically, this sorting will already have been
	// performed upstream.
	slices.SortFunc(c.signals, func(a, b vss.Signal) int {
		return cmp.Or(a.Data.Timestamp.Compare(b.Data.Timestamp), cmp.Compare(a.Data.Name, b.Data.Name))
	})

	for i := range c.signals {
		c.processSignal(i)
	}

	// One last attempt, in case we're in the process of constructing
	// a location.
	c.tryCreateLocation()

	out := dropPruned(c.signals)

	// TODO(elffjs): Should we sort these?
	out = append(out, c.created...)

	return out, errors.Join(c.errs...)
}

func (c *coordinateStore) processSignal(index int) {
	sig := c.signals[index]

	if !c.lastTime.IsZero() && sig.Data.Timestamp.Sub(c.lastTime) >= maxLatLongDur {
		c.tryCreateLocation()
	}

	// This logic could be made shorter and less repetitive by
	// playing around with *int.
	switch sig.Data.Name {
	case fieldCurrentLocationLatitude:
		if c.lastLat != -1 {
			// Start a new triple, but see if what's already being
			// tracked is enough to yield a row.
			c.tryCreateLocation()
		}
		c.lastLat = index
	case fieldCurrentLocationLongitude:
		if c.lastLon != -1 {
			c.tryCreateLocation()
		}
		c.lastLon = index
	case fieldDIMOAftermarketHDOP:
		if c.lastHDOP != -1 {
			c.tryCreateLocation()
		}
		c.lastHDOP = index
	default:
		return
	}

	if c.lastTime.IsZero() {
		c.lastTime = sig.Data.Timestamp
	}
}

// tryCreateLocation tries to add a VSS location row using the active
// location triple.
//
// Only call this function when forced: if there is any chance that
// the triple can be completed by the next element of the slice then
// calling this function may discard the elements of the active triple
// on the grounds of it being incomplete.
func (c *coordinateStore) tryCreateLocation() {
	var loc vss.Location
	var create bool

	// Inherit the header from the active triple itself: a batch can mix
	// sources, and the synthesized coordinate signal must carry the
	// Source/Producer of the location signals, not whatever came first.
	template := c.signals[0]
	switch {
	case c.lastLat != -1:
		template = c.signals[c.lastLat]
	case c.lastLon != -1:
		template = c.signals[c.lastLon]
	case c.lastHDOP != -1:
		template = c.signals[c.lastHDOP]
	}

	if c.lastLat != -1 && c.lastLon != -1 {
		lat := c.signals[c.lastLat].Data.ValueNumber
		lon := c.signals[c.lastLon].Data.ValueNumber

		if !validLatLon(lat, lon) {
			c.signals[c.lastLat].Data.Name = pruneSignalName
			c.signals[c.lastLon].Data.Name = pruneSignalName
			c.errs = append(c.errs, fmt.Errorf("%w: invalid coordinate lat=%v lon=%v at time %s", errLatLongMismatch, lat, lon, c.lastTime))
		} else {
			loc.Latitude = lat
			loc.Longitude = lon
			c.signals[c.lastLat].Data.Name = pruneSignalName
			c.signals[c.lastLon].Data.Name = pruneSignalName
			create = true
		}
	} else if c.lastLat != -1 {
		c.signals[c.lastLat].Data.Name = pruneSignalName
		c.errs = append(c.errs, fmt.Errorf("%w: unpaired latitude at time %s", errLatLongMismatch, c.lastTime))
	} else if c.lastLon != -1 {
		c.signals[c.lastLon].Data.Name = pruneSignalName
		c.errs = append(c.errs, fmt.Errorf("%w: unpaired longitude at time %s", errLatLongMismatch, c.lastTime))
	}

	if c.lastHDOP != -1 {
		loc.HDOP = c.signals[c.lastHDOP].Data.ValueNumber
		c.signals[c.lastHDOP].Data.Name = pruneSignalName
		create = true
	}

	if create {
		c.created = append(c.created, vss.Signal{
			CloudEventHeader: template.CloudEventHeader,
			Data: vss.SignalData{
				Timestamp:     c.lastTime,
				Name:          vss.FieldCurrentLocationCoordinates,
				ValueLocation: loc,
				CloudEventID:  template.Data.CloudEventID,
			},
		})
	}

	c.lastLat = -1
	c.lastLon = -1
	c.lastHDOP = -1
	c.lastTime = zeroTime
}

// validLatLon reports whether a decoded coordinate is a usable WGS-84 position:
// finite, within latitude/longitude bounds, and not the (0,0) null island that
// devices emit for "no fix". model-garage's per-source decoders pass numbers
// through without enforcing their documented min/max, so din rejects bad
// coordinates here — otherwise a device reporting lat=5000 or NaN would flow
// into storage and the DIMO_SIGNALS trigger stream (geofence/threshold rules).
func validLatLon(lat, lon float64) bool {
	if math.IsNaN(lat) || math.IsNaN(lon) || math.IsInf(lat, 0) || math.IsInf(lon, 0) {
		return false
	}
	if lat == 0 && lon == 0 {
		return false
	}
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}
