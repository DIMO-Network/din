package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConnectionLicense = "0x9c94C395cBcBDe662235E0A9d3bB87Ad708561BA"

// startConnectionServer builds a connection server from cfg and serves it
// on a loopback listener, returning the base URL.
func startConnectionServer(t *testing.T, cfg ConnectionConfig, handler http.Handler) string {
	t.Helper()

	srv, err := NewConnectionServer(cfg, handler)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		// Certificates live in srv.TLSConfig.
		_ = srv.ServeTLS(ln, "", "")
	}()
	t.Cleanup(func() { _ = srv.Close() })

	return "https://" + ln.Addr().String()
}

// connectionTestSetup wires a CA, server certs on disk, and a default
// config for the mTLS server tests.
func connectionTestSetup(t *testing.T) (ca *testCA, cfg ConnectionConfig) {
	t.Helper()

	dir := t.TempDir()
	ca = newTestCA(t, "din test root CA")
	serverCert := ca.issue(t, "localhost", true)
	certFile, keyFile := writeCertFiles(t, dir, serverCert)
	caFile := writeCAFile(t, dir, "root_ca.crt", ca)

	return ca, ConnectionConfig{
		TLSCertFile:   certFile,
		TLSKeyFile:    keyFile,
		ClientCAFiles: []string{caFile},
	}
}

func newMTLSClient(ca *testCA, clientCert *tls.Certificate) *http.Client {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	tlsCfg := &tls.Config{RootCAs: pool}
	if clientCert != nil {
		tlsCfg.Certificates = []tls.Certificate{*clientCert}
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   5 * time.Second,
	}
}

func TestConnectionServer_SourceFromClientCert(t *testing.T) {
	t.Parallel()
	ca, cfg := connectionTestSetup(t)

	var gotSource string
	var gotOK bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSource, gotOK = SourceFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	baseURL := startConnectionServer(t, cfg, handler)

	clientCert := ca.issue(t, testConnectionLicense, false)
	client := newMTLSClient(ca, &clientCert)

	resp, err := client.Post(baseURL+"/", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, gotOK, "source should be set in the request context")
	assert.Equal(t, testConnectionLicense, gotSource, "source must be the client cert CN")
}

func TestConnectionServer_RejectsMissingClientCert(t *testing.T) {
	t.Parallel()
	ca, cfg := connectionTestSetup(t)

	baseURL := startConnectionServer(t, cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not be reached without a client cert")
	}))

	client := newMTLSClient(ca, nil)
	//nolint:bodyclose // the request must fail before a body exists.
	_, err := client.Post(baseURL+"/", "application/json", strings.NewReader(`{}`))
	require.Error(t, err, "handshake must fail when no client cert is presented")
}

func TestConnectionServer_RejectsUntrustedClientCert(t *testing.T) {
	t.Parallel()
	ca, cfg := connectionTestSetup(t)

	baseURL := startConnectionServer(t, cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must not be reached with an untrusted client cert")
	}))

	otherCA := newTestCA(t, "rogue CA")
	rogueCert := otherCA.issue(t, testConnectionLicense, false)
	client := newMTLSClient(ca, &rogueCert)

	//nolint:bodyclose // the request must fail before a body exists.
	_, err := client.Post(baseURL+"/", "application/json", strings.NewReader(`{}`))
	require.Error(t, err, "handshake must fail for a cert from an unknown CA")
}

func TestConnectionServer_RateLimit(t *testing.T) {
	t.Parallel()
	ca, cfg := connectionTestSetup(t)
	cfg.RateLimitRPS = 1
	cfg.RateLimitBurst = 1

	baseURL := startConnectionServer(t, cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	clientCert := ca.issue(t, testConnectionLicense, false)
	client := newMTLSClient(ca, &clientCert)

	statuses := make([]int, 0, 3)
	for range 3 {
		resp, err := client.Post(baseURL+"/", "application/json", strings.NewReader(`{}`))
		require.NoError(t, err)
		_ = resp.Body.Close()
		statuses = append(statuses, resp.StatusCode)
	}

	assert.Equal(t, http.StatusOK, statuses[0], "first request fits the burst")
	assert.Contains(t, statuses[1:], http.StatusTooManyRequests, "subsequent burst requests must be limited")
}

func TestConnectionServer_BodyLimit(t *testing.T) {
	t.Parallel()
	ca, cfg := connectionTestSetup(t)
	cfg.MaxBodyBytes = 16

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
	})
	baseURL := startConnectionServer(t, cfg, handler)

	clientCert := ca.issue(t, testConnectionLicense, false)
	client := newMTLSClient(ca, &clientCert)

	// Small body passes.
	resp, err := client.Post(baseURL+"/", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Oversized body is cut off by MaxBytesReader.
	resp, err = client.Post(baseURL+"/", "application/json", strings.NewReader(strings.Repeat("x", 64)))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

func TestNewConnectionServer_Defaults(t *testing.T) {
	t.Parallel()
	_, cfg := connectionTestSetup(t)

	srv, err := NewConnectionServer(cfg, http.NotFoundHandler())
	require.NoError(t, err)

	assert.Equal(t, DefaultConnectionAddr, srv.Addr)
	assert.Equal(t, DefaultTimeout, srv.ReadTimeout)
	require.NotNil(t, srv.TLSConfig)
	assert.Equal(t, tls.RequireAndVerifyClientCert, srv.TLSConfig.ClientAuth)
	require.NotNil(t, srv.TLSConfig.ClientCAs)
}

func TestNewConnectionServer_ConfigErrors(t *testing.T) {
	t.Parallel()
	_, cfg := connectionTestSetup(t)

	t.Run("missing client CA files", func(t *testing.T) {
		bad := cfg
		bad.ClientCAFiles = nil
		_, err := NewConnectionServer(bad, http.NotFoundHandler())
		require.Error(t, err)
	})

	t.Run("unreadable CA file", func(t *testing.T) {
		bad := cfg
		bad.ClientCAFiles = []string{"/nonexistent/root_ca.crt"}
		_, err := NewConnectionServer(bad, http.NotFoundHandler())
		require.Error(t, err)
	})

	t.Run("missing key pair", func(t *testing.T) {
		bad := cfg
		bad.TLSCertFile = "/nonexistent/tls.crt"
		_, err := NewConnectionServer(bad, http.NotFoundHandler())
		require.Error(t, err)
	})
}

// TestCertSourceHandler_Direct exercises the middleware against a crafted
// request, mirroring the dis CertRoutingMiddlewarefunc unit test.
func TestCertSourceHandler_Direct(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t, "din test root CA")
	leaf := ca.issue(t, testConnectionLicense, false)

	var gotSource string
	handler := certSourceHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSource, _ = SourceFromContext(r.Context())
	}))

	t.Run("verified chain present", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf.Leaf, ca.cert}}}
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, testConnectionLicense, gotSource)
	})

	t.Run("no TLS state", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("empty verified chains", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.TLS = &tls.ConnectionState{}
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

func TestRateLimitMiddleware_PerRemote(t *testing.T) {
	t.Parallel()

	mw := rateLimitMiddleware(1, 1, remoteIPKey)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	do := func(remote string) int {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.RemoteAddr = remote
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	assert.Equal(t, http.StatusOK, do("10.0.0.1:1111"), "first request from remote A")
	assert.Equal(t, http.StatusTooManyRequests, do("10.0.0.1:2222"), "second request from remote A is limited")
	assert.Equal(t, http.StatusOK, do("10.0.0.2:1111"), "remote B has its own bucket")
}

func TestRateLimitMiddleware_Disabled(t *testing.T) {
	t.Parallel()

	mw := rateLimitMiddleware(0, 0, remoteIPKey)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := range 50 {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.RemoteAddr = "10.0.0.1:1111"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, fmt.Sprintf("request %d must pass with limiting disabled", i))
	}
}

// publishAckBudgetForTest mirrors cmd/din's publishAckTimeout (10s) — the max a handler
// can block on a JetStream ack before writing its 503. Kept here so the server package can
// assert its write budget exceeds it without importing the cmd package.
const publishAckBudgetForTest = 10 * time.Second

// TestServers_WriteTimeoutExceedsAckBudget pins the fix for the WriteTimeout(5s) <
// publishAckTimeout(10s) mismatch: the ingest handler can block up to the publish-ack
// budget before writing a 503+Retry-After, so the socket WriteTimeout must exceed that
// (and the read budget) or the orderly backpressure response is never writable — the
// device sees a raw connection reset and retry-storms (scale review #5).
func TestServers_WriteTimeoutExceedsAckBudget(t *testing.T) {
	t.Parallel()
	_, cfg := connectionTestSetup(t)
	srv, err := NewConnectionServer(cfg, http.NotFoundHandler())
	require.NoError(t, err)
	assert.Equal(t, DefaultWriteTimeout, srv.WriteTimeout)
	assert.Greater(t, srv.WriteTimeout, srv.ReadTimeout, "write budget must exceed the read budget so a slow-publish 503 is writable")
	assert.Greater(t, srv.WriteTimeout, publishAckBudgetForTest, "write budget must exceed the publish-ack budget")
}
