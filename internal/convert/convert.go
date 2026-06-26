// Package convert validates raw connection payloads and canonicalizes them
// into DIMO CloudEvents. It is a pure-Go port of the DIS
// cloudeventconvert processor with the Benthos message plumbing removed.
package convert

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
)

const (
	// MaxHeaderBytes caps the JSON-serialized size of a CloudEvent header
	// (every field except data and data_base64). No individual header field
	// has its own length limit, so without this any string field, Tags entry,
	// or Extras value could balloon stored rows and Parquet columns
	// downstream.
	MaxHeaderBytes = 8 * 1024

	defaultSkew = time.Minute * 5
)

// ErrValidation marks 400-class errors caused by invalid client payloads or
// headers, as opposed to 500-class internal failures. Use errors.Is to
// distinguish them.
var ErrValidation = errors.New("validation error")

// allowedContentTypes is the whitelist of MIME types accepted for CloudEvent data.
var allowedContentTypes = map[string]struct{}{
	"application/json": {},
	"image/png":        {},
	"image/jpeg":       {},
	"application/pdf":  {},
}

// allowableTimeSkew bounds how far past now() a CloudEvent timestamp may be. It holds
// the default until SetAllowableTimeSkew wires in the validated config value at boot,
// so the converter uses the value config.Load already validated rather than
// independently re-reading (and leniently mis-parsing) the env.
var allowableTimeSkew = defaultSkew

// SetAllowableTimeSkew sets the future-timestamp tolerance from the validated config
// (Settings.AllowableTimeSkew). Call once at boot before ingest starts.
func SetAllowableTimeSkew(d time.Duration) {
	if d > 0 {
		allowableTimeSkew = d
	}
}

// ValidIdentifier reports whether str is non-empty and contains only characters
// allowed in CloudEvent identifier fields: [a-zA-Z0-9-_/,. :]. This is a
// hand-rolled ASCII scan, not the equivalent regexp `^[a-zA-Z0-9\-_/,. :]+$`: it
// is called ~7x per event by ValidateHeadersAndSetDefaults and benchmarks ~9x
// faster (≈35ns vs ≈315ns/op). Every allowed character is single-byte ASCII, so
// any byte ≥ 0x80 (a multi-byte UTF-8 rune) falls through to the default and is
// rejected — identical to the regexp's behavior on non-ASCII runes.
func ValidIdentifier(str string) bool {
	if str == "" {
		return false
	}
	for i := 0; i < len(str); i++ {
		switch c := str[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '/', c == ',', c == '.', c == ' ', c == ':':
		default:
			return false
		}
	}
	return true
}

// IsFutureTimestamp checks if a timestamp is in the future past the allowable time skew.
func IsFutureTimestamp(ts time.Time) bool {
	return ts.After(time.Now().Add(allowableTimeSkew))
}

// ValidateHeadersAndSetDefaults validates the cloud event header and fills in defaults.
// isBase64 indicates whether the event payload arrived as data_base64 rather than data;
// it controls how the data content type is defaulted and validated.
func ValidateHeadersAndSetDefaults(event *cloudevent.CloudEventHeader, source, defaultID string, isBase64 bool) error {
	event.Source = source

	if event.Subject == "" {
		event.Subject = event.Producer
	}

	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}

	if event.ID == "" {
		event.ID = defaultID
	}
	if event.SpecVersion == "" {
		event.SpecVersion = "1.0"
	}
	if err := validateAndSetContentType(event, isBase64); err != nil {
		return err
	}

	if !ValidIdentifier(event.ID) {
		return fmt.Errorf("%w: invalid id: %s", ErrValidation, event.ID)
	}
	if !ValidIdentifier(event.SpecVersion) {
		return fmt.Errorf("%w: invalid specversion: %s", ErrValidation, event.SpecVersion)
	}
	if !ValidIdentifier(event.DataContentType) {
		return fmt.Errorf("%w: invalid data content type: %s", ErrValidation, event.DataContentType)
	}
	if event.DataSchema != "" && !ValidIdentifier(event.DataSchema) {
		return fmt.Errorf("%w: invalid data schema: %s", ErrValidation, event.DataSchema)
	}
	if event.DataVersion != "" && !ValidIdentifier(event.DataVersion) {
		return fmt.Errorf("%w: invalid data version: %s", ErrValidation, event.DataVersion)
	}
	if event.Type != "" && !ValidIdentifier(event.Type) {
		return fmt.Errorf("%w: invalid data type: %s", ErrValidation, event.Type)
	}
	if event.Subject != "" && !ValidIdentifier(event.Subject) {
		return fmt.Errorf("%w: invalid subject: %s", ErrValidation, event.Subject)
	}
	if event.Producer != "" && !ValidIdentifier(event.Producer) {
		return fmt.Errorf("%w: invalid producer: %s", ErrValidation, event.Producer)
	}

	b, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal header for size check: %w", err)
	}
	if len(b) > MaxHeaderBytes {
		return fmt.Errorf("%w: header size %d exceeds max %d", ErrValidation, len(b), MaxHeaderBytes)
	}

	return nil
}

// validateAndSetContentType applies the content type rules to the event header.
//   - If the event uses data_base64, datacontenttype must be set explicitly.
//   - Otherwise the data field is treated as JSON: an empty value is defaulted to
//     application/json and any other value is rejected.
//   - In all cases, datacontenttype must be one of the whitelisted MIME types.
func validateAndSetContentType(event *cloudevent.CloudEventHeader, isBase64 bool) error {
	if isBase64 {
		if event.DataContentType == "" {
			return fmt.Errorf("%w: datacontenttype is required for data_base64 events", ErrValidation)
		}
	} else {
		if event.DataContentType == "" {
			event.DataContentType = "application/json"
		}
		if event.DataContentType != "application/json" {
			return fmt.Errorf("%w: datacontenttype %q is not allowed for data events: must be application/json", ErrValidation, event.DataContentType)
		}
	}
	if _, ok := allowedContentTypes[event.DataContentType]; !ok {
		return fmt.Errorf("%w: datacontenttype %q is not in the allowed list", ErrValidation, event.DataContentType)
	}
	return nil
}
