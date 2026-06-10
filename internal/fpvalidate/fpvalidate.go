// Package fpvalidate gates fingerprint CloudEvents on having a valid VIN.
// It is a pure-Go port of the DIS fingerprintvalidate processor with the
// Benthos message plumbing removed.
package fpvalidate

import (
	"context"
	"errors"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/shared/pkg/vin"
)

// ErrInvalidVIN marks fingerprints whose VIN is neither a valid VIN nor a
// valid Japan chassis number. DIS silently dropped these messages; callers
// should use errors.Is to drop the event without surfacing an error to the
// client.
var ErrInvalidVIN = errors.New("invalid VIN")

// Validate checks that a fingerprint event carries a valid VIN. Events that
// are not fingerprints pass through with no error. A conversion failure is
// returned as a plain error; an invalid VIN is returned wrapped in
// ErrInvalidVIN so the caller can distinguish "drop silently" from "fail".
func Validate(ctx context.Context, event cloudevent.RawEvent) error {
	if event.Type != cloudevent.TypeFingerprint {
		return nil
	}
	fingerprint, err := modules.ConvertToFingerprint(ctx, event.Source, event)
	if err != nil {
		return fmt.Errorf("failed to convert to fingerprint: %w", err)
	}
	vinObj := vin.VIN(fingerprint.VIN)
	if !vinObj.IsValidVIN() && !vinObj.IsValidJapanChassis() {
		return fmt.Errorf("%w: invalid VIN format in fingerprint: subject=%s source=%s vin=%s", ErrInvalidVIN, event.Subject, event.Source, fingerprint.VIN)
	}
	return nil
}
