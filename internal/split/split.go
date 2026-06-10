// Package split externalizes large CloudEvent `data` payloads: when a
// payload exceeds a configured threshold the raw bytes are written to an
// object store and the event is returned with `data` and `data_base64`
// cleared and the blob's object key recorded as DataIndexKey.
package split

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/google/uuid"
)

// Defaults mirror the dis dimo_cloudevent_split processor configuration.
const (
	// DefaultThreshold is the data payload size in bytes above which events
	// are externalized; events at or below it stay inline.
	DefaultThreshold = 1 << 20 // 1 MiB
	// DefaultPrefix is the object key prefix for externalized blobs.
	DefaultPrefix = "cloudevent/blobs/"
)

// ObjectStore persists externalized payload blobs.
type ObjectStore interface {
	PutObject(ctx context.Context, key string, body []byte) error
}

// Splitter externalizes oversized CloudEvent payloads to an ObjectStore.
type Splitter struct {
	store     ObjectStore
	threshold int
	prefix    string
	now       func() time.Time
}

// New returns a Splitter writing blobs to store. A prefix of "" defaults to
// DefaultPrefix; a threshold <= 0 defaults to DefaultThreshold.
func New(store ObjectStore, prefix string, threshold int) *Splitter {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if threshold <= 0 {
		threshold = DefaultThreshold
	}
	return &Splitter{
		store:     store,
		threshold: threshold,
		prefix:    prefix,
		now:       time.Now,
	}
}

// MaybeSplit returns the event unchanged when its data payload is at or
// below the threshold. Otherwise it uploads the raw payload bytes to the
// object store under <prefix><subject>/<year>/<month>/<day>/<uuid> and
// returns the event with data stripped and DataIndexKey set to the blob
// key. The threshold applies to the decoded `data` payload only, never the
// header.
func (s *Splitter) MaybeSplit(ctx context.Context, event cloudevent.RawEvent) (cloudevent.StoredEvent, error) {
	if dataPayloadLen(&event) <= s.threshold {
		return cloudevent.StoredEvent{RawEvent: event}, nil
	}

	raw, err := decodePayload(&event)
	if err != nil {
		return cloudevent.StoredEvent{}, fmt.Errorf("decode data_base64 for event %s: %w", event.ID, err)
	}

	key := buildBlobKey(s.prefix, event.Subject, s.now().UTC())
	if err := s.store.PutObject(ctx, key, raw); err != nil {
		return cloudevent.StoredEvent{}, fmt.Errorf("put blob %s for event %s: %w", key, event.ID, err)
	}

	return cloudevent.StoredEvent{
		RawEvent:     cloudevent.RawEvent{CloudEventHeader: event.CloudEventHeader},
		DataIndexKey: key,
	}, nil
}

func dataPayloadLen(ev *cloudevent.RawEvent) int {
	if ev.DataBase64 != "" {
		return base64.StdEncoding.DecodedLen(len(ev.DataBase64))
	}
	return len(ev.Data)
}

func decodePayload(ev *cloudevent.RawEvent) ([]byte, error) {
	if ev.DataBase64 != "" {
		return base64.StdEncoding.DecodeString(ev.DataBase64)
	}
	return []byte(ev.Data), nil
}

func buildBlobKey(prefix, subject string, t time.Time) string {
	return fmt.Sprintf("%s%s/%d/%02d/%02d/%s",
		prefix, subject, t.Year(), int(t.Month()), t.Day(), uuid.New().String())
}
