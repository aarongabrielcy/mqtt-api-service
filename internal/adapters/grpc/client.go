package grpc

import (
"context"
"go.uber.org/zap"
)

type Client struct {
log *zap.Logger
}

func NewClient(config interface{}, log *zap.Logger) (*Client, error) {
return &Client{log: log}, nil
}

func (c *Client) IngestRaw(ctx context.Context, topic string, payload []byte, qos int) error {
return nil
}
