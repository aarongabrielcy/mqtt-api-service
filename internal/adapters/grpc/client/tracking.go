package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	trackingpb "mqtt-api-service/internal/adapters/grpc/proto/tracking"
	"mqtt-api-service/internal/application/normalizers"

	"go.uber.org/zap"
)

func (c *Client) PublishEvent(
	ctx context.Context,
	payload []byte,
) error {

	var msg normalizers.NormalizedMessage

	if err := json.Unmarshal(payload, &msg); err != nil {
		return fmt.Errorf(
			"failed to unmarshal normalized message: %w",
			err,
		)
	}

	req := &trackingpb.RawMessage{
		Topic:      msg.Topic,
		Payload:    payload,
		Qos:        -1,
		Retain:     false,
		ReceivedAt: msg.ReceivedAt,
	}

	rpcCtx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()

	resp, err := c.tracking.IngestRaw(
		rpcCtx,
		req,
	)

	if err != nil {
		c.log.Error(
			"failed to publish tracking event",
			zap.Error(err),
			zap.String(
				"grpc_state",
				c.conn.GetState().String(),
			),
		)

		return fmt.Errorf(
			"failed to publish tracking event: %w",
			err,
		)
	}

	c.log.Info(
		"tracking event published",
		zap.String("devvice_id", msg.DeviceID),
		zap.String("topic", msg.Topic),
		zap.Bool("ok", resp.GetOk()),
	)

	return nil
}
