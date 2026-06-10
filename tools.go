//go:build tools

// Package tools pins dependencies for packages under construction so that
// parallel development never needs go.mod edits. Remove once all packages
// import these directly.
package tools

import (
	_ "github.com/DIMO-Network/cloudevent"
	_ "github.com/DIMO-Network/model-garage/pkg/modules"
	_ "github.com/DIMO-Network/shared/pkg/vin"
	_ "github.com/MicahParks/keyfunc/v3"
	_ "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/golang-jwt/jwt/v5"
	_ "github.com/nats-io/nats-server/v2/server"
	_ "github.com/nats-io/nats.go/jetstream"
	_ "github.com/oklog/ulid/v2"
	_ "github.com/parquet-go/parquet-go"
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/rs/zerolog"
	_ "github.com/stretchr/testify/require"
	_ "golang.org/x/sync/errgroup"
	_ "golang.org/x/time/rate"
)
