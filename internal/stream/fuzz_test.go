package stream

import (
	"testing"

	"github.com/nats-io/nats.go"
)

// FuzzParseMsg drives ParseMsg with arbitrary message bodies. ParseMsg is on the
// ingest hot path decoding bytes that originate from (semi-trusted) producers over
// NATS, so it must always fail gracefully — never panic — on malformed input. This
// also fuzzes cloudevent.RawEvent.UnmarshalJSON, the shared decoder underneath.
func FuzzParseMsg(f *testing.F) {
	f.Add([]byte(`{"id":"x","source":"0xc0dec0dec0dec0dec0dec0dec0dec0dec0dec0de","subject":"did:erc721:1:0xc0de:1","type":"dimo.status","time":"2024-01-01T00:00:00Z","data":{"v":1}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"data":`))
	f.Add([]byte(`{"time":"not-a-time"}`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`{"data":{"deeply":{"nested":[[[[]]]]}}}`))

	f.Fuzz(func(t *testing.T, body []byte) {
		// A malformed body must produce an error, not a panic. Headers are a
		// fixed empty set here; the body (JSON) is the parser surface that matters.
		_, _ = ParseMsg(nats.Header{}, body)
	})
}
