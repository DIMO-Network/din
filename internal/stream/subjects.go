// Package stream provisions the INGEST_RAW JetStream stream and publishes
// validated raw cloudevents to it. The stream is the durability point for
// ingest: an HTTP request is acknowledged only after JetStream has accepted
// the message.
package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/DIMO-Network/cloudevent"
)

const (
	// StreamName is the JetStream stream holding all raw ingested cloudevents.
	StreamName = "INGEST_RAW"

	// SubjectRoot is the prefix for all raw ingest subjects.
	SubjectRoot = "in.raw"

	// SubjectWildcard matches every raw ingest subject.
	SubjectWildcard = SubjectRoot + ".>"
)

// Subject returns the NATS subject for a cloudevent:
// in.raw.<typeToken>.<subjectToken>. Type dots become underscores so the
// event type occupies a single subject token; the cloudevent subject (a DID)
// is sanitized defensively. Consumers filtering on the embedded subject must
// re-check the real header value because sanitization can collide.
func Subject(hdr *cloudevent.CloudEventHeader) string {
	return SubjectRoot + "." + typeToken(hdr.Type) + "." + sanitizeToken(hdr.Subject)
}

// SubjectFilterForType returns a filter subject matching all events of one
// cloudevent type, e.g. dimo.status across all vehicles.
func SubjectFilterForType(ceType string) string {
	return SubjectRoot + "." + typeToken(ceType) + ".>"
}

// SubjectFilterForSubject returns a filter subject matching all event types
// for one cloudevent subject (vehicle DID).
func SubjectFilterForSubject(ceSubject string) string {
	return SubjectRoot + ".*." + sanitizeToken(ceSubject)
}

// MsgID returns the JetStream deduplication ID for an event: the SHA-256 of
// the header uniqueness key (subject+time+type+source+id). Devices that
// retry after a timed-out 200 collapse into one stored message inside the
// stream's duplicate window.
func MsgID(hdr *cloudevent.CloudEventHeader) string {
	sum := sha256.Sum256([]byte(hdr.Key()))
	return hex.EncodeToString(sum[:])
}

func typeToken(ceType string) string {
	return sanitizeToken(strings.ReplaceAll(ceType, ".", "_"))
}

// sanitizeToken replaces anything that is not NATS-token-safe with '-'.
// '.' (token separator), '*' and '>' (wildcards), and whitespace must never
// appear inside a single token.
func sanitizeToken(s string) string {
	if s == "" {
		return "-"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == ':', r == '_', r == '=', r == '-':
			return r
		default:
			return '-'
		}
	}, s)
}
