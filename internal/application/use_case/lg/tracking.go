package lg_service

import (
	"context"
	"fmt"
	"mqtt-api-service/internal/adapters/parser"
	"mqtt-api-service/internal/application/normalizers"
	"mqtt-api-service/internal/domain/interfaces"
	"time"

	"go.uber.org/zap"
)

func (s *LGService) publishTracking(
	ctx context.Context,
	deviceID string,
	deviceType string,
	eventCode normalizers.EventCode,
	state *parser.AirConditionerState,
) error {

	topic, payload, receivedAt, err := s.stateNormalizer.NormalizeTelemetry(
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

	if err := s.trackingClient.IngestRaw(
		ctx,
		interfaces.IngestRawInput{
			Topic:      topic,
			Payload:    payload,
			ReceivedAt: receivedAt,
		},
	); err != nil {
		return fmt.Errorf(
			"publish tracking event: %w",
			err,
		)
	}

	s.log.Info(
		"tracking event published",
		zap.String("deviceID", deviceID),
		zap.String("topic", topic),
		zap.Int("eventCode", int(eventCode)),
	)

	return nil
}

func (s *LGService) publishEnergyTracking(
	ctx context.Context,
	deviceID string,
	deviceType string,
	dailyUsageKwh float64,
) error {
	raw, err := s.deviceStateStore.GetState(ctx, deviceID)
	if err != nil || len(raw) == 0 {
		s.log.Warn("skipping energy telemetry: no known device state in redis yet",
			zap.String("deviceID", deviceID),
		)
		return nil
	}

	state, err := s.stateParser.ParseAirConditionerState(deviceID, raw)
	if err != nil {
		return fmt.Errorf("parse last known state for energy telemetry: %w", err)
	}

	energy := normalizers.LGEnergyInfo{
		DailyUsageKwh: dailyUsageKwh,
		LastUpdated:   time.Now().UTC().Unix(),
	}

	topic, payload, receivedAt, err := s.stateNormalizer.NormalizeEnergyTelemetry(
		deviceID,
		deviceType,
		normalizers.EventCodeEnergyUsage,
		state,
		energy,
	)
	if err != nil {
		return fmt.Errorf("normalize energy telemetry: %w", err)
	}

	if s.trackingClient == nil {
		return nil
	}

	if err := s.trackingClient.IngestRaw(
		ctx,
		interfaces.IngestRawInput{
			Topic:      topic,
			Payload:    payload,
			ReceivedAt: receivedAt,
		},
	); err != nil {
		return fmt.Errorf("publish energy tracking event: %w", err)
	}

	s.log.Info(
		"energy tracking event published",
		zap.String("deviceID", deviceID),
		zap.String("topic", topic),
		zap.Float64("dailyUsageKwh", dailyUsageKwh),
	)

	return nil
}
