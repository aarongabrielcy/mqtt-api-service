package client

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

func NewConnection(cfg Config) (*grpc.ClientConn, error) {

	conn, err := grpc.NewClient(
		cfg.Address,

		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),

		grpc.WithConnectParams(
			grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  1 * time.Second,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   30 * time.Second,
				},
				MinConnectTimeout: 5 * time.Second,
			},
		),
	)

	if err != nil {
		return nil, err
	}

	return conn, nil
}
