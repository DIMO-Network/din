package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Defaults mirror the dis external-ingest listener settings.
const (
	DefaultConnectionAddr  = ":9443"
	DefaultAttestationAddr = ":9442"
	DefaultOpsAddr         = ":8080"
	DefaultTimeout         = 5 * time.Second
	DefaultMaxBodyBytes    = 32 << 20 // 32 MiB
	// DefaultIdleTimeout bounds how long a keep-alive connection may sit idle
	// between requests; without it an idle (or slow-trickle) connection is held
	// open indefinitely, tying up a goroutine/fd (slowloris-style exhaustion).
	DefaultIdleTimeout = 60 * time.Second
	// DefaultMaxHeaderBytes caps request header size (the net/http default is
	// 1 MiB); a tight bound blunts header-flood attacks on the ingest listeners.
	DefaultMaxHeaderBytes = 64 << 10 // 64 KiB
)

// ConnectionConfig configures the mTLS connection ingest server.
type ConnectionConfig struct {
	// Addr is the listen address; defaults to ":9443".
	Addr string
	// TLSCertFile is the PEM-encoded server certificate.
	TLSCertFile string
	// TLSKeyFile is the PEM-encoded server private key.
	TLSKeyFile string
	// ClientCAFiles are PEM files holding the root CAs that client
	// certificates must chain to (mutual TLS is required).
	ClientCAFiles []string
	// Timeout bounds request read/write; defaults to 5s.
	Timeout time.Duration
	// MaxBodyBytes caps request body size; defaults to 32 MiB.
	MaxBodyBytes int64
	// RateLimitRPS is the per-remote sustained request rate; <= 0 disables
	// rate limiting.
	RateLimitRPS float64
	// RateLimitBurst is the per-remote burst size; defaults to ceil(RPS).
	RateLimitBurst int
	// Logger is used for middleware diagnostics; the zero value is silent.
	Logger zerolog.Logger
}

// AttestationConfig configures the JWT-authenticated attestation server.
type AttestationConfig struct {
	// Addr is the listen address; defaults to ":9442".
	Addr string
	// TokenExchangeIssuer is the issuer URL for the token exchange service.
	TokenExchangeIssuer string
	// TokenExchangeKeySetURL provides the public keys for JWT signature
	// validation (JWKS).
	TokenExchangeKeySetURL string
	// Timeout bounds request read/write; defaults to 5s.
	Timeout time.Duration
	// MaxBodyBytes caps request body size; defaults to 32 MiB.
	MaxBodyBytes int64
	// RateLimitRPS is the per-remote sustained request rate; <= 0 disables
	// rate limiting.
	RateLimitRPS float64
	// RateLimitBurst is the per-remote burst size; defaults to ceil(RPS).
	RateLimitBurst int
	// Logger is used for middleware diagnostics; the zero value is silent.
	Logger zerolog.Logger
}

// OpsConfig configures the operational HTTP server.
type OpsConfig struct {
	// Addr is the listen address; defaults to ":8080".
	Addr string
	// Ready, when non-nil, backs the /ready probe: /ready returns 503 until
	// Ready() returns true, so Kubernetes withholds traffic until the process
	// has finished wiring (NATS connected, streams ensured, lake attached).
	// /ping (liveness) stays an unconditional 200 so a busy-but-healthy pod is
	// never restarted. A nil Ready keeps /ready unconditionally 200 (back-compat).
	Ready func() bool
}

// NewConnectionServer builds the mTLS ingest server. Clients must present a
// certificate chaining to one of cfg.ClientCAFiles; the leaf certificate's
// CommonName (the connection license address) is injected into the request
// context and is available to handler via SourceFromContext. Start it with
// srv.ListenAndServeTLS("", "") — the certificates are already loaded into
// srv.TLSConfig.
func NewConnectionServer(cfg ConnectionConfig, handler http.Handler) (*http.Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultConnectionAddr
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server key pair: %w", err)
	}

	if len(cfg.ClientCAFiles) == 0 {
		return nil, fmt.Errorf("at least one client CA file is required for mutual TLS")
	}
	caPool := x509.NewCertPool()
	for _, caFile := range cfg.ClientCAFiles {
		pemBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading client CA file %s: %w", caFile, err)
		}
		if !caPool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("no client CA certificates found in %s", caFile)
		}
	}

	h := certSourceHandler(handler)
	h = maxBytesMiddleware(cfg.MaxBodyBytes)(h)
	h = rateLimitMiddleware(cfg.RateLimitRPS, cfg.RateLimitBurst, certCNKey)(h)
	h = recoverMiddleware(cfg.Logger)(h)

	return &http.Server{
		Addr:    cfg.Addr,
		Handler: h,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caPool,
		},
		ReadTimeout:       cfg.Timeout,
		ReadHeaderTimeout: cfg.Timeout,
		WriteTimeout:      cfg.Timeout,
		IdleTimeout:       DefaultIdleTimeout,
		MaxHeaderBytes:    DefaultMaxHeaderBytes,
	}, nil
}

// NewAttestationServer builds the JWT-authenticated attestation server.
// Requests must carry a Bearer token issued by cfg.TokenExchangeIssuer and
// signed by a key in cfg.TokenExchangeKeySetURL; the token's
// ethereum_address claim is injected into the request context and is
// available to handler via SourceFromContext. The JWKS is fetched at
// construction time and refreshed in the background.
func NewAttestationServer(cfg AttestationConfig, handler http.Handler) (*http.Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAttestationAddr
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}

	auth, err := newAttestationAuth(cfg.TokenExchangeIssuer, cfg.TokenExchangeKeySetURL)
	if err != nil {
		return nil, fmt.Errorf("building attestation auth: %w", err)
	}

	logger := cfg.Logger
	authHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		source, err := auth(r)
		if err != nil {
			logger.Warn().Err(err).Str("remote", r.RemoteAddr).Msg("attestation auth failed")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r.WithContext(WithSource(r.Context(), source)))
	})

	h := maxBytesMiddleware(cfg.MaxBodyBytes)(authHandler)
	h = rateLimitMiddleware(cfg.RateLimitRPS, cfg.RateLimitBurst, remoteIPKey)(h)
	h = recoverMiddleware(cfg.Logger)(h)

	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           h,
		ReadTimeout:       cfg.Timeout,
		ReadHeaderTimeout: cfg.Timeout,
		WriteTimeout:      cfg.Timeout,
		IdleTimeout:       DefaultIdleTimeout,
		MaxHeaderBytes:    DefaultMaxHeaderBytes,
	}, nil
}

// NewOpsServer builds the operational server exposing /ping, /ready, and
// Prometheus /metrics.
func NewOpsServer(cfg OpsConfig) *http.Server {
	if cfg.Addr == "" {
		cfg.Addr = DefaultOpsAddr
	}

	ok := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
	ready := ok
	if cfg.Ready != nil {
		ready = func(w http.ResponseWriter, _ *http.Request) {
			if !cfg.Ready() {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", ok)
	mux.HandleFunc("/ready", ready)
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: DefaultTimeout,
		IdleTimeout:       DefaultIdleTimeout,
		MaxHeaderBytes:    DefaultMaxHeaderBytes,
	}
}
