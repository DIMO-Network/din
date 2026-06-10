// Package server provides the HTTP listeners for the din ingest path: an
// mTLS connection server, a JWT-authenticated attestation server, and an
// operational server (ping/ready/metrics). Authentication is implemented as
// middleware that injects the verified source address into the request
// context; the business handler is supplied by the caller.
package server

import "context"

type contextKey struct{ name string }

var sourceKey = &contextKey{name: "dimo_cloudevent_source"}

// WithSource returns a copy of ctx carrying the authenticated source
// address (connection license address from the client cert CN, or the
// ethereum address claim from an attestation JWT).
func WithSource(ctx context.Context, source string) context.Context {
	return context.WithValue(ctx, sourceKey, source)
}

// SourceFromContext returns the authenticated source address injected by
// the connection or attestation middleware.
func SourceFromContext(ctx context.Context) (string, bool) {
	source, ok := ctx.Value(sourceKey).(string)
	return source, ok
}
