package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOpsServer(t *testing.T) {
	t.Parallel()

	srv := NewOpsServer(OpsConfig{})
	assert.Equal(t, DefaultOpsAddr, srv.Addr)

	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()

	for _, path := range []string{"/ping", "/ready"} {
		resp, err := ts.Client().Get(ts.URL + path)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		_ = resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, path)
		assert.Equal(t, "ok", string(body), path)
	}

	resp, err := ts.Client().Get(ts.URL + "/metrics")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "go_goroutines", "metrics output should include Go runtime collectors")
}

func TestNewOpsServer_CustomAddr(t *testing.T) {
	t.Parallel()
	srv := NewOpsServer(OpsConfig{Addr: ":18080"})
	assert.Equal(t, ":18080", srv.Addr)
}
