package server_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/DIMO-Network/din/internal/server"
)

// The ops /ready probe must gate on the supplied Ready func (503 until wiring
// completes) while /ping (liveness) stays an unconditional 200.
func TestOpsServer_ReadinessGate(t *testing.T) {
	var ready atomic.Bool
	srv := server.NewOpsServer(server.OpsConfig{Ready: ready.Load})

	do := func(path string) int {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}

	if got := do("/ready"); got != http.StatusServiceUnavailable {
		t.Fatalf("/ready before ready = %d, want 503", got)
	}
	if got := do("/ping"); got != http.StatusOK {
		t.Fatalf("/ping (liveness) = %d, want 200", got)
	}
	ready.Store(true)
	if got := do("/ready"); got != http.StatusOK {
		t.Fatalf("/ready after ready = %d, want 200", got)
	}
}

// A nil Ready keeps /ready unconditionally 200 (back-compat).
func TestOpsServer_NilReadyAlwaysOK(t *testing.T) {
	srv := server.NewOpsServer(server.OpsConfig{})
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/ready with nil Ready = %d, want 200", rec.Code)
	}
}
