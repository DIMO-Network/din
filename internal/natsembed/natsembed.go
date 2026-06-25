// Package natsembed boots an in-process nats-server with JetStream enabled.
// It backs single-node deployments, local development, and integration tests;
// production points the same client code at an external cluster instead.
package natsembed

import (
	"fmt"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Config controls the embedded server.
type Config struct {
	// StoreDir is the JetStream file-store directory. Required.
	StoreDir string
	// Port, when > 0, opens a TCP client listener (advanced/debug only). The
	// default (<= 0) runs in-process only with NO network listener: din's
	// sink/publisher reach the server via Connect (net.Pipe), so no port is
	// needed, and listening would default to 0.0.0.0 and expose an
	// unauthenticated JetStream server on the pod network.
	Port int
	// StartTimeout bounds how long to wait for the server to come up.
	StartTimeout time.Duration
	// MaxStore overrides JetStream's storage capacity. Zero keeps the
	// server's disk-based default. Tests set this high so stream
	// MaxBytes reservations don't fail on nearly-full dev disks.
	MaxStore int64
}

// Run starts an embedded nats-server and blocks until it is ready.
// Call Shutdown on the returned server when done.
func Run(cfg Config) (*natsserver.Server, error) {
	if cfg.StoreDir == "" {
		return nil, fmt.Errorf("natsembed: StoreDir is required")
	}
	timeout := cfg.StartTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	opts := &natsserver.Options{
		JetStream: true,
		StoreDir:  cfg.StoreDir,
		Port:      cfg.Port,
		NoSigs:    true, // din owns signal handling; the server must not install its own
	}
	if cfg.MaxStore > 0 {
		opts.JetStreamMaxStore = cfg.MaxStore
	}
	// In-process only unless a positive port is explicitly requested: din's clients use
	// the net.Pipe transport (Connect), so the server must not open a network listener
	// (which defaults to 0.0.0.0 — an unauthenticated JetStream server on the pod net).
	if cfg.Port <= 0 {
		opts.DontListen = true
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("creating embedded nats server: %w", err)
	}

	go srv.Start()
	if !srv.ReadyForConnections(timeout) {
		srv.Shutdown()
		srv.WaitForShutdown() // release the store-dir lock before a caller retries
		return nil, fmt.Errorf("embedded nats server not ready after %s", timeout)
	}
	return srv, nil
}

// Connect returns a client connection over the in-process transport,
// avoiding the network stack entirely.
func Connect(srv *natsserver.Server) (*nats.Conn, error) {
	conn, err := nats.Connect("", nats.InProcessServer(srv))
	if err != nil {
		return nil, fmt.Errorf("connecting to embedded nats server: %w", err)
	}
	return conn, nil
}
