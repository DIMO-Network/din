package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/server"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/rs/zerolog"
)

// fakePublisher records how it was called so a test can prove the connection
// handler pipelines via PublishBatch (load review #2) rather than the old
// per-event Publish loop, and can inject the publish outcome.
type fakePublisher struct {
	batchCalls  int
	singleCalls int
	lastBatch   []*cloudevent.StoredEvent
	batchErr    error
}

func (f *fakePublisher) Publish(context.Context, *cloudevent.StoredEvent) error {
	f.singleCalls++
	return nil
}

func (f *fakePublisher) PublishBatch(_ context.Context, events []*cloudevent.StoredEvent) error {
	f.batchCalls++
	f.lastBatch = events
	return f.batchErr
}

// fakeConverter returns a fixed set of events (or a conversion error).
type fakeConverter struct {
	events []cloudevent.RawEvent
	err    error
}

func (f *fakeConverter) Convert(context.Context, string, []byte) ([]cloudevent.RawEvent, error) {
	return f.events, f.err
}

// passSplitter is an identity splitter — no externalization.
type passSplitter struct{}

func (passSplitter) MaybeSplit(_ context.Context, e cloudevent.RawEvent) (cloudevent.StoredEvent, error) {
	return cloudevent.StoredEvent{RawEvent: e}, nil
}

// failOnSplitter fails MaybeSplit for one event ID and passes the rest — used to prove a
// single event's split failure doesn't drop the siblings that split cleanly.
type failOnSplitter struct{ failID string }

func (s failOnSplitter) MaybeSplit(_ context.Context, e cloudevent.RawEvent) (cloudevent.StoredEvent, error) {
	if e.ID == s.failID {
		return cloudevent.StoredEvent{}, errors.New("split failed for " + e.ID)
	}
	return cloudevent.StoredEvent{RawEvent: e}, nil
}

func statusEvent(id string) cloudevent.RawEvent {
	return cloudevent.RawEvent{CloudEventHeader: cloudevent.CloudEventHeader{ID: id, Type: cloudevent.TypeStatus}}
}

// A split failure on ONE event of a multi-event payload must NOT drop the siblings that
// split cleanly — the old serial loop published every event before the failing one, so
// aborting the whole request here would silently lose already-good events (load review R3).
func TestConnection_SplitFailurePublishesGoodSiblings(t *testing.T) {
	pub := &fakePublisher{}
	h := &Handlers{
		Converter: &fakeConverter{events: []cloudevent.RawEvent{statusEvent("a"), statusEvent("b"), statusEvent("c")}},
		Splitter:  failOnSplitter{failID: "b"}, // the middle event fails to split
		Publisher: pub,
		Log:       zerolog.Nop(),
	}
	rec := doConnection(t, h)

	if len(pub.lastBatch) != 2 {
		t.Fatalf("good siblings still published: want 2 (a, c), got %d", len(pub.lastBatch))
	}
	if rec.Code == http.StatusOK {
		t.Fatalf("a split failure must still surface an error, not 200")
	}
}

func doConnection(t *testing.T, h *Handlers) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body"))
	req = req.WithContext(server.WithSource(req.Context(), "0xc0dec0dec0dec0dec0dec0dec0dec0dec0dec0de"))
	rec := httptest.NewRecorder()
	h.Connection().ServeHTTP(rec, req)
	return rec
}

// The connection handler must persist a multi-event payload through a single
// PublishBatch (all events in one pipelined round-trip), never the per-event
// Publish path — the load review #2 fix.
func TestConnection_MultiEventUsesPublishBatch(t *testing.T) {
	pub := &fakePublisher{}
	h := &Handlers{
		Converter: &fakeConverter{events: []cloudevent.RawEvent{statusEvent("a"), statusEvent("b"), statusEvent("c")}},
		Splitter:  passSplitter{},
		Publisher: pub,
		Log:       zerolog.Nop(),
	}
	rec := doConnection(t, h)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d", rec.Code)
	}
	if pub.batchCalls != 1 {
		t.Fatalf("PublishBatch calls: want 1, got %d", pub.batchCalls)
	}
	if pub.singleCalls != 0 {
		t.Fatalf("serial Publish calls: want 0 (must not fall back to per-event), got %d", pub.singleCalls)
	}
	if len(pub.lastBatch) != 3 {
		t.Fatalf("batched events: want 3, got %d", len(pub.lastBatch))
	}
}

// An unacked publish (ErrUnavailable) is retryable → 503; a max-payload
// rejection is deterministic → 413. Both must round-trip through the batch path.
func TestConnection_PublishErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"unavailable is 503", stream.ErrUnavailable, http.StatusServiceUnavailable},
		{"too-large is 413", stream.ErrPayloadTooLarge, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakePublisher{batchErr: tc.err}
			h := &Handlers{
				Converter: &fakeConverter{events: []cloudevent.RawEvent{statusEvent("a"), statusEvent("b")}},
				Splitter:  passSplitter{},
				Publisher: pub,
				Log:       zerolog.Nop(),
			}
			rec := doConnection(t, h)
			if rec.Code != tc.want {
				t.Fatalf("status: want %d, got %d", tc.want, rec.Code)
			}
		})
	}
}

// A conversion failure is the caller's fault (400) and must never reach the
// publisher.
func TestConnection_ConversionErrorIs400(t *testing.T) {
	pub := &fakePublisher{}
	h := &Handlers{
		// Joined so errors.Is(convert.ErrValidation) holds → writeError maps to 400.
		Converter: &fakeConverter{err: errors.Join(convert.ErrValidation, errors.New("bad payload"))},
		Splitter:  passSplitter{},
		Publisher: pub,
		Log:       zerolog.Nop(),
	}
	rec := doConnection(t, h)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d", rec.Code)
	}
	if pub.batchCalls != 0 {
		t.Fatalf("publisher must not be called on a conversion error, got %d batch calls", pub.batchCalls)
	}
}
