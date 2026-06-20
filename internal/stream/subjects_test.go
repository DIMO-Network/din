package stream

import (
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
)

func header(ceType, subject string) *cloudevent.CloudEventHeader {
	return &cloudevent.CloudEventHeader{
		Type:    ceType,
		Subject: subject,
		Source:  "0xConn",
		ID:      "evt-1",
		Time:    time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
	}
}

func TestSubject(t *testing.T) {
	t.Parallel()
	hdr := header("dimo.status", "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42")
	assert.Equal(t,
		"in.raw.dimo_status.did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
		Subject(hdr, 1))
}

func TestSubject_SanitizesUnsafeRunes(t *testing.T) {
	t.Parallel()
	hdr := header("dimo.raw.v1/extra", "sub ject*with>stars")
	got := Subject(hdr, 1)
	assert.Equal(t, "in.raw.dimo_raw_v1-extra.sub-ject-with-stars", got)
	assert.NotContains(t, got[len("in.raw."):], "*")
	assert.NotContains(t, got[len("in.raw."):], ">")
}

func TestSubject_EmptySubject(t *testing.T) {
	t.Parallel()
	hdr := header("dimo.status", "")
	assert.Equal(t, "in.raw.dimo_status.-", Subject(hdr, 1))
}

func TestSubjectFilters(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "in.raw.dimo_status.>", SubjectFilterForType("dimo.status"))
}

func TestMsgID_StableAndDistinct(t *testing.T) {
	t.Parallel()
	a := header("dimo.status", "did:erc721:137:0xA:1")
	b := header("dimo.fingerprint", "did:erc721:137:0xA:1") // same ID, different type

	assert.Equal(t, MsgID(a), MsgID(a), "msg id must be deterministic")
	assert.NotEqual(t, MsgID(a), MsgID(b), "key includes type; status+fingerprint sharing an ID must not dedup")
	assert.Len(t, MsgID(a), 64)
}
