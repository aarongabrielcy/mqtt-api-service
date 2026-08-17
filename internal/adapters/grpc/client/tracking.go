package client

import (
	"context"
	"fmt"
	"time"

	trackingpb "mqtt-api-service/internal/adapters/grpc/proto/tracking"
	"mqtt-api-service/internal/domain/interfaces"

	"go.uber.org/zap"
)

// IngestRaw envía el mensaje ya normalizado a tracking-platform vía
// TrackingService.IngestRaw. input.Payload debe ser JSON directo (sin
// wrapper); el topic viaja en el campo dedicado del contrato, no dentro
// del payload.
func (c *Client) IngestRaw(
	ctx context.Context,
	input interfaces.IngestRawInput,
) error {

	req := &trackingpb.RawMessage{
		Topic:      input.Topic,
		Payload:    input.Payload,
		Qos:        -1,
		Retain:     false,
		ReceivedAt: input.ReceivedAt.Format(time.RFC3339),
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
			"failed to ingest tracking event",
			zap.Error(err),
			zap.String("topic", input.Topic),
			zap.String(
				"grpc_state",
				c.conn.GetState().String(),
			),
		)

		return fmt.Errorf(
			"failed to ingest tracking event: %w",
			err,
		)
	}

	c.log.Info(
		"tracking event ingested",
		zap.String("topic", input.Topic),
		zap.Bool("ok", resp.GetOk()),
	)

	return nil
}
