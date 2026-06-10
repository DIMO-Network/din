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
	// Port is the client listen port. Use -1 for a random port (tests) or
	// 0 to disable the network listener entirely (in-process clients only).
	Port int
	// StartTimeout bounds how long to wait for the server to come up.
	StartTimeout time.Duration
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
		// The embedded server is reached in-process; do not advertise.
		NoSigs: true,
	}
	if cfg.Port == 0 {
		opts.DontListen = true
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("creating embedded nats server: %w", err)
	}

	go srv.Start()
	if !srv.ReadyForConnections(timeout) {
		srv.Shutdown()
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
