// Package stream provisions the INGEST_RAW JetStream stream and publishes
// validated raw cloudevents to it. The stream is the durability point for
// ingest: an HTTP request is acknowledged only after JetStream has accepted
// the message.
package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
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
// in.raw.<typeToken>.<subjectToken>[.pNNN]. Type dots become underscores so
// the event type occupies a single subject token; the cloudevent subject (a
// DID) is sanitized defensively. Consumers filtering on the embedded subject
// must re-check the real header value because sanitization can collide.
// With partitions > 1 a trailing partition token routes the event to its
// partition stream; it goes LAST so type/subject filters are partition-count
// agnostic.
func Subject(hdr *cloudevent.CloudEventHeader, partitions int) string {
	base := SubjectRoot + "." + typeToken(hdr.Type) + "." + sanitizeToken(hdr.Subject)
	if partitions <= 1 {
		return base
	}
	return base + fmt.Sprintf(".p%03d", Partition(hdr.Subject, partitions))
}

// Partition maps a cloudevent subject to its WAL partition. All events of
// one vehicle land in one partition, preserving per-vehicle ordering.
func Partition(ceSubject string, partitions int) int {
	if partitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(ceSubject))
	return int(h.Sum32() % uint32(partitions))
}

// StreamNameFor names partition i of n. One partition keeps the historical
// name so single-stream deployments are unchanged.
func StreamNameFor(i, n int) string {
	if n <= 1 {
		return StreamName
	}
	return fmt.Sprintf("%s_P%03d", StreamName, i)
}

// partitionSubjects returns the subject filters owned by partition i of n.
func partitionSubjects(i, n int) []string {
	if n <= 1 {
		return []string{SubjectWildcard}
	}
	return []string{fmt.Sprintf("%s.*.*.p%03d", SubjectRoot, i)}
}

// SubjectFilterForType returns a filter subject matching all events of one
// cloudevent type, e.g. dimo.status across all vehicles.
func SubjectFilterForType(ceType string) string {
	return SubjectRoot + "." + typeToken(ceType) + ".>"
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
