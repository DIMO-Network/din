package pqwrite_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/din/internal/pqwrite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hdr(ceType string, ts time.Time) *cloudevent.CloudEventHeader {
	return &cloudevent.CloudEventHeader{
		Type:    ceType,
		Subject: "did:erc721:137:0xA:1",
		Source:  "0xConn",
		ID:      "e1",
		Time:    ts,
	}
}

func TestPartitionFor_UsesEventTimeUTC(t *testing.T) {
	t.Parallel()
	est := time.FixedZone("EST", -5*3600)
	// 23:30 EST on June 8 is June 9 UTC — partition must follow UTC.
	p := pqwrite.PartitionFor(hdr("dimo.status", time.Date(2026, 6, 8, 23, 30, 0, 0, est)))
	assert.Equal(t, "dimo.status", p.Type)
	assert.Equal(t, "2026-06-09", p.Date)
	assert.Equal(t, "raw/type=dimo.status/date=2026-06-09/", p.Dir(pqwrite.RawPrefix))
}

func TestPartitionFor_SanitizesType(t *testing.T) {
	t.Parallel()
	p := pqwrite.PartitionFor(hdr("weird/type with:stuff", time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, "weird-type-with-stuff", p.Type)
}

func TestNewIngestObjectKey_LexicographicTimeOrder(t *testing.T) {
	t.Parallel()
	p := pqwrite.PartitionKey{Type: "dimo.status", Date: "2026-06-09"}
	t1 := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	k1 := pqwrite.NewIngestObjectKey(pqwrite.RawPrefix, p, t1)
	k2 := pqwrite.NewIngestObjectKey(pqwrite.RawPrefix, p, t2)

	assert.Less(t, k1, k2, "later writes must sort later: materializer cursor depends on it")
	assert.Contains(t, k1, "raw/type=dimo.status/date=2026-06-09/ingest-")
	assert.NotEqual(t, k1, pqwrite.NewIngestObjectKey(pqwrite.RawPrefix, p, t1), "ULID must make same-ms keys unique")
}

func TestEncode_PreservesDataIndexKeyAndSorts(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	events := []cloudevent.StoredEvent{
		{
			RawEvent: cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion, Type: "dimo.status",
				Subject: "did:erc721:137:0xB:2", Source: "0xC", ID: "e2", Time: ts,
			}},
			DataIndexKey: "blobs/big-payload",
		},
		{
			RawEvent: cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion, Type: "dimo.status",
				Subject: "did:erc721:137:0xA:1", Source: "0xC", ID: "e1", Time: ts,
			}, Data: json.RawMessage(`{"v":1}`)},
		},
	}

	body, err := pqwrite.Encode(events, "raw/type=dimo.status/date=2026-06-09/x.parquet")
	require.NoError(t, err)

	got, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "e1", got[0].ID, "rows sorted by subject")
	assert.Equal(t, "e2", got[1].ID)
	assert.Equal(t, "blobs/big-payload", got[1].DataIndexKey)
}
