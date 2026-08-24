package client

import "time"

type Config struct {
	Address           string
	ConnectionTimeout time.Duration

	// RequestTimeout/MaxAttempts/InitialBackoff/MaxBackoff controlan el
	// timeout por request y el reintento corto de IngestRaw (ver
	// internal/adapters/grpc/client/tracking.go). Pensados para tolerar que
	// ingestion-service tarde en resolver por DNS Docker o en estar listo
	// durante el arranque, no como mecanismo general de reintento.
	RequestTimeout time.Duration
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func DefaultConfig() Config {
	return Config{
		ConnectionTimeout: 5 * time.Second,
		RequestTimeout:    10 * time.Second,
		MaxAttempts:       3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        4 * time.Second,
	}
}
