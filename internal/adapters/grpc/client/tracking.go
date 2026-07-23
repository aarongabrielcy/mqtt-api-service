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

	req := &trackingpb.NormalizedMessage{
		Imei:       msg.IMEI,
		ReceivedAt: msg.ReceivedAt,
		Topic:      msg.Topic,
		Payload: &trackingpb.LGTelemetryPayload{
			EventCode:  trackingpb.EventCode(msg.Payload.EventCode),
			DeviceType: msg.Payload.DeviceType,
			Power:      msg.Payload.Power,
			Temperature: &trackingpb.TemperaturePayload{
				Current: msg.Payload.Temperature.Current,
				Target:  msg.Payload.Temperature.Target,
				Unit:    msg.Payload.Temperature.Unit,
			},
			Humidity: msg.Payload.Humidity,
		},
	}

	rpcCtx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()

	resp, err := c.tracking.PublishEvent(
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
		zap.String("imei", msg.IMEI),
		zap.String("topic", msg.Topic),
		zap.String("response", resp.GetMessage()),
	)

	return nil
}
