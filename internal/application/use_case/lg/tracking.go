package lg_service

import (
	"context"
	"fmt"
	"mqtt-api-service/internal/adapters/parser"
	"mqtt-api-service/internal/application/normalizers"

	"go.uber.org/zap"
)

func (s *LGService) publishTracking(
	ctx context.Context,
	deviceID string,
	deviceType string,
	eventCode normalizers.EventCode,
	state *parser.AirConditionerState,
) error {

	normalized, err := s.stateNormalizer.NormalizeTelemetry(
		deviceID,
		deviceType,
		eventCode,
		state,
	)

	if err != nil {
		return fmt.Errorf(
			"normalize telemetry: %w",
			err,
		)
	}

	if s.trackingClient == nil {
		return nil
	}

	if err := s.trackingClient.PublishEvent(
		ctx,
		normalized,
	); err != nil {
		return fmt.Errorf(
			"publish tracking event: %w",
			err,
		)
	}

	s.log.Info(
		"tracking event published",
		zap.String("deviceID", deviceID),
		zap.Int("eventCode", int(eventCode)),
	)

	return nil
}
