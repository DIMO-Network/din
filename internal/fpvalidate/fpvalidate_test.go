package fpvalidate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	modelce "github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockConvertToFingerprint struct{}

// Mock for modules.FingerprintConvert
func (mockConvertToFingerprint) FingerprintConvert(ctx context.Context, event cloudevent.RawEvent) (modelce.Fingerprint, error) {
	var fp modelce.Fingerprint
	err := json.Unmarshal(event.Data, &fp)
	if err != nil {
		return modelce.Fingerprint{}, err
	}
	return fp, nil
}

func TestValidate(t *testing.T) {
	// Override the ConvertToFingerprint function for testing
	oldDefault, _ := modules.FingerprintRegistry.Get("")
	defer func() {
		modules.FingerprintRegistry.Override("", oldDefault)
	}()
	modules.FingerprintRegistry.Override("", mockConvertToFingerprint{})

	tests := []struct {
		name             string
		event            cloudevent.RawEvent
		expectInvalidVIN bool
		expectError      bool
	}{
		{
			name:  "Valid VIN",
			event: createFingerprintEvent(t, "1HGCM82633A123456"),
		},
		{
			name:             "Invalid VIN - too short",
			event:            createFingerprintEvent(t, "1HGCM123"),
			expectInvalidVIN: true,
		},
		{
			name:             "Invalid VIN - contains invalid character I",
			event:            createFingerprintEvent(t, "1HGCM82633A12345I"), // 'I' is not a valid character
			expectInvalidVIN: true,
		},
		{
			name:             "Invalid VIN - contains invalid character Q",
			event:            createFingerprintEvent(t, "1HGCM82633A12345Q"), // 'Q' is not a valid character
			expectInvalidVIN: true,
		},
		{
			name:             "Invalid VIN - all As with invalid character Q",
			event:            createFingerprintEvent(t, "AAAAAAAAAAAAAAAAQ"), // 'Q' is not a valid character
			expectInvalidVIN: true,
		},
		{
			name:  "Valid Japan Chassis VIN",
			event: createFingerprintEvent(t, "SNT33-042261"),
		},
		{
			name:  "Non-fingerprint event",
			event: createNonFingerprintEvent(),
		},
		{
			name:        "Fingerprint with malformed data",
			event:       createRawEvent(fingerprintHeader(), []byte(`{"this is not valid JSON`)),
			expectError: true,
		},
		{
			name:             "Invalid VIN - garbage characters",
			event:            createFingerprintEvent(t, "INVALID-VIN-HERE!"),
			expectInvalidVIN: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(context.Background(), tt.event)
			switch {
			case tt.expectInvalidVIN:
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidVIN)
			case tt.expectError:
				require.Error(t, err)
				assert.NotErrorIs(t, err, ErrInvalidVIN)
			default:
				assert.NoError(t, err)
			}
		})
	}
}

func fingerprintHeader() cloudevent.CloudEventHeader {
	var hdr cloudevent.CloudEventHeader
	hdr.Type = cloudevent.TypeFingerprint
	return hdr
}

func createFingerprintEvent(t *testing.T, vin string) cloudevent.RawEvent {
	t.Helper()
	var fingerprintEvent modelce.FingerprintEvent
	fingerprintEvent.Type = cloudevent.TypeFingerprint
	fingerprintEvent.Data.VIN = vin
	data, err := json.Marshal(fingerprintEvent.Data)
	require.NoError(t, err)
	return createRawEvent(fingerprintEvent.CloudEventHeader, data)
}

func createNonFingerprintEvent() cloudevent.RawEvent {
	var hdr cloudevent.CloudEventHeader
	hdr.Type = cloudevent.TypeStatus
	return createRawEvent(hdr, []byte(`{"key": "value"}`))
}

func createRawEvent(hdr cloudevent.CloudEventHeader, data []byte) cloudevent.RawEvent {
	return cloudevent.RawEvent{
		CloudEventHeader: hdr,
		Data:             data,
	}
}
