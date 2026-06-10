package split

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStore records PutObject calls and optionally fails them.
type fakeStore struct {
	puts map[string][]byte
	err  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{puts: map[string][]byte{}}
}

func (f *fakeStore) PutObject(_ context.Context, key string, body []byte) error {
	if f.err != nil {
		return f.err
	}
	f.puts[key] = bytes.Clone(body)
	return nil
}

func newTestSplitter(store ObjectStore, threshold int) *Splitter {
	s := New(store, "cloudevent/blobs/", threshold)
	s.now = func() time.Time { return time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC) }
	return s
}

func makeJSONEvent(id, subject string, payloadSize int) cloudevent.RawEvent {
	return cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion:     "1.0",
			ID:              id,
			Source:          "0xSource",
			Subject:         subject,
			Producer:        "did:nft:137:0xProd:1",
			Time:            time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:            "dimo.attestation",
			DataContentType: "application/json",
		},
		Data: json.RawMessage(`{"padding":"` + strings.Repeat("x", payloadSize) + `"}`),
	}
}

func makeBase64Event(id, subject, contentType string, raw []byte) cloudevent.RawEvent {
	return cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion:     "1.0",
			ID:              id,
			Source:          "0xSource",
			Subject:         subject,
			Producer:        "did:nft:137:0xProd:1",
			Time:            time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:            "dimo.attestation",
			DataContentType: contentType,
		},
		DataBase64: base64.StdEncoding.EncodeToString(raw),
	}
}

func TestMaybeSplit_SmallEventPassesThrough(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	splitter := newTestSplitter(store, 1024)

	ev := makeJSONEvent("small-1", "did:erc721:1:0xV:1", 64)
	got, err := splitter.MaybeSplit(context.Background(), ev)
	require.NoError(t, err)

	assert.Empty(t, got.DataIndexKey, "small event should not get a data index key")
	assert.Equal(t, ev, got.RawEvent, "small event must pass through unchanged")
	assert.Empty(t, store.puts, "no blob should be written")
}

func TestMaybeSplit_BigJSONEventSplits(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	splitter := newTestSplitter(store, 1024)

	subject := "did:erc721:1:0xV:99"
	ev := makeJSONEvent("big-1", subject, 4096)
	got, err := splitter.MaybeSplit(context.Background(), ev)
	require.NoError(t, err)

	require.NotEmpty(t, got.DataIndexKey)
	assert.True(t, strings.HasPrefix(got.DataIndexKey, "cloudevent/blobs/"+subject+"/"),
		"key %q must start with prefix and subject", got.DataIndexKey)

	// Key pattern: <prefix><subject>/<year>/<month>/<day>/<uuid>
	keyPattern := regexp.MustCompile(
		`^cloudevent/blobs/` + regexp.QuoteMeta(subject) + `/2024/06/15/[0-9a-f-]{36}$`)
	assert.Regexp(t, keyPattern, got.DataIndexKey)

	assert.Empty(t, got.Data, "stripped event should have empty data")
	assert.Empty(t, got.DataBase64)
	assert.Equal(t, ev.CloudEventHeader, got.CloudEventHeader, "header should be preserved")

	blob, ok := store.puts[got.DataIndexKey]
	require.True(t, ok, "blob must be stored under the data index key")
	var dataObj map[string]any
	require.NoError(t, json.Unmarshal(blob, &dataObj))
	assert.Contains(t, dataObj, "padding")
}

func TestMaybeSplit_BigBase64EventSplitsAndDecodes(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	splitter := newTestSplitter(store, 1024)

	raw := bytes.Repeat([]byte{0x00, 0x01, 0x7f, 0x80, 0xff}, 1024)
	subject := "did:erc721:1:0xV:99"
	ev := makeBase64Event("img-1", subject, "image/jpeg", raw)

	got, err := splitter.MaybeSplit(context.Background(), ev)
	require.NoError(t, err)
	require.NotEmpty(t, got.DataIndexKey)

	assert.Equal(t, raw, store.puts[got.DataIndexKey], "blob payload should be decoded raw bytes")
	assert.Empty(t, got.Data)
	assert.Empty(t, got.DataBase64, "stripped event must drop data_base64")
}

func TestMaybeSplit_ThresholdIsDataOnly(t *testing.T) {
	t.Parallel()
	// Threshold 1024 bytes against the data field. A header that pushes the
	// whole message over 1024 should NOT trigger a split if the data itself
	// is small.
	store := newFakeStore()
	splitter := newTestSplitter(store, 1024)

	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: "1.0",
			ID:          "small-data",
			Source:      "0xSource",
			Subject:     "did:erc721:1:0xV:1",
			Producer:    "did:nft:137:0xProd:1",
			Time:        time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			Type:        "dimo.attestation",
			Tags:        []string{strings.Repeat("a", 800), strings.Repeat("b", 800)},
		},
		Data: json.RawMessage(`{"k":"v"}`),
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	require.Greater(t, len(b), 1024, "test setup: full message must exceed threshold")

	got, err := splitter.MaybeSplit(context.Background(), ev)
	require.NoError(t, err)
	assert.Empty(t, got.DataIndexKey, "small data should not split even when header is big")
	assert.Empty(t, store.puts)
}

func TestMaybeSplit_ExactThresholdStaysInline(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	splitter := newTestSplitter(store, 64)

	ev := makeJSONEvent("edge-1", "did:erc721:1:0xV:1", 0)
	ev.Data = json.RawMessage(strings.Repeat("x", 64)) // exactly the threshold
	got, err := splitter.MaybeSplit(context.Background(), ev)
	require.NoError(t, err)
	assert.Empty(t, got.DataIndexKey, "events at the threshold stay inline")
}

func TestMaybeSplit_StoreErrorPropagates(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.err = errors.New("bucket on fire")
	splitter := newTestSplitter(store, 1024)

	ev := makeJSONEvent("big-err", "did:erc721:1:0xV:1", 4096)
	_, err := splitter.MaybeSplit(context.Background(), ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, store.err)
	assert.Contains(t, err.Error(), "big-err", "error should reference the event id")
}

func TestMaybeSplit_InvalidBase64Errors(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	splitter := newTestSplitter(store, 16)

	ev := makeBase64Event("bad-b64", "did:erc721:1:0xV:1", "image/jpeg", bytes.Repeat([]byte{0xab}, 64))
	ev.DataBase64 = "!!!not-base64!!!" + strings.Repeat("A", 64)

	_, err := splitter.MaybeSplit(context.Background(), ev)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode data_base64")
	assert.Empty(t, store.puts)
}

func TestMaybeSplit_UniqueKeysPerCall(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	splitter := newTestSplitter(store, 1024)

	subject := "did:erc721:1:0xV:7"
	keys := map[string]struct{}{}
	for i := range 5 {
		ev := makeJSONEvent(fmt.Sprintf("big-%d", i), subject, 4096)
		got, err := splitter.MaybeSplit(context.Background(), ev)
		require.NoError(t, err)
		keys[got.DataIndexKey] = struct{}{}
	}
	assert.Len(t, keys, 5, "every split must get a unique blob key")
}

func TestNew_Defaults(t *testing.T) {
	t.Parallel()
	s := New(newFakeStore(), "", 0)
	assert.Equal(t, DefaultPrefix, s.prefix)
	assert.Equal(t, DefaultThreshold, s.threshold)
}
