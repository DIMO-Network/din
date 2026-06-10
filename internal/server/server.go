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

	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           h,
		ReadTimeout:       cfg.Timeout,
		ReadHeaderTimeout: cfg.Timeout,
		WriteTimeout:      cfg.Timeout,
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

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", ok)
	mux.HandleFunc("/ready", ok)
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: DefaultTimeout,
	}
}
