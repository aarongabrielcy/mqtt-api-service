package client

import (
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	trackingpb "mqtt-api-service/internal/adapters/grpc/proto/tracking"
)

type Client struct {
	conn *grpc.ClientConn

	tracking trackingpb.TrackingServiceClient

	log *zap.Logger

	requestTimeout time.Duration
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

func New(
	cfg Config,
	log *zap.Logger,
) (*Client, error) {

	conn, err := NewConnection(cfg)

	if err != nil {
		return nil, err
	}

	requestTimeout := cfg.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = 10 * time.Second
	}

	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	initialBackoff := cfg.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = 1 * time.Second
	}

	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 4 * time.Second
	}

	return &Client{
		conn:           conn,
		tracking:       trackingpb.NewTrackingServiceClient(conn),
		log:            log,
		requestTimeout: requestTimeout,
		maxAttempts:    maxAttempts,
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
	}, nil

}

func (c *Client) Close() error {
	return c.conn.Close()
}
