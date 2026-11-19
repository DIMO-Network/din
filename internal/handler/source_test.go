package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/admit"
	"github.com/DIMO-Network/din/internal/server"
	"github.com/rs/zerolog"
)

// fakeAdmitter returns fixed events (or an error) and records its source.
type fakeAdmitter struct {
	events []cloudevent.RawEvent
	err    error
	source string
}

func (f *fakeAdmitter) Admit(_ context.Context, source string, _ []byte) ([]cloudevent.RawEvent, error) {
	f.source = source
	return f.events, f.err
}

func doSource(h *Handlers, source string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body"))
	req = req.WithContext(server.WithSource(req.Context(), source))
	rec := httptest.NewRecorder()
	h.Connection().ServeHTTP(rec, req)
	return rec
}

// A DID source goes to the admitter, never the converter.
func TestConnection_DIDSourceIsAdmitted(t *testing.T) {
	pub := &fakePublisher{}
	adm := &fakeAdmitter{events: []cloudevent.RawEvent{statusEvent("a")}}
	h := &Handlers{
		Converter: &fakeConverter{err: fmt.Errorf("the converter must not run")},
		Admitter:  adm, Splitter: passSplitter{}, Publisher: pub, Log: zerolog.Nop(),
	}
	rec := doSource(h, "did:dimo:flespi")
	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d %s", rec.Code, rec.Body)
	}
	if adm.source != "did:dimo:flespi" || len(pub.lastBatch) != 1 {
		t.Fatalf("admitter source %q, published %d", adm.source, len(pub.lastBatch))
	}
}

func TestConnection_DIDSourceErrors(t *testing.T) {
	for _, tc := range []struct {
		admitter Admitter
		want     int
	}{
		{nil, http.StatusForbidden},
		{&fakeAdmitter{err: fmt.Errorf("event 0: %w: no grant", admit.ErrForbidden)}, http.StatusForbidden},
		{&fakeAdmitter{err: fmt.Errorf("%w: org host down", admit.ErrUnavailable)}, http.StatusServiceUnavailable},
	} {
		pub := &fakePublisher{}
		h := &Handlers{Admitter: tc.admitter, Splitter: passSplitter{}, Publisher: pub, Log: zerolog.Nop()}
		rec := doSource(h, "did:dimo:flespi")
		if rec.Code != tc.want {
			t.Errorf("want %d, got %d %s", tc.want, rec.Code, rec.Body)
		}
		if pub.batchCalls != 0 {
			t.Errorf("nothing may be published on a refusal")
		}
	}
}
