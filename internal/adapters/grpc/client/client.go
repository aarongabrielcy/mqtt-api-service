package client

import (
	"go.uber.org/zap"
	"google.golang.org/grpc"

	trackingpb "mqtt-api-service/internal/adapters/grpc/proto/tracking"
)

type Client struct {
	conn *grpc.ClientConn

	tracking trackingpb.TrackingServiceClient

	log *zap.Logger
}

func New(
	cfg Config,
	log *zap.Logger,
) (*Client, error) {

	conn, err := NewConnection(cfg)

	if err != nil {
		return nil, err
	}

	return &Client{
		conn:     conn,
		tracking: trackingpb.NewTrackingServiceClient(conn),
		log:      log,
	}, nil

}

func (c *Client) Close() error {
	return c.conn.Close()
}
