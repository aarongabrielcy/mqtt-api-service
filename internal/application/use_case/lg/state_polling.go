package lg_service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mqtt-api-service/internal/adapters/api/lg"
	"mqtt-api-service/internal/application/normalizers"
	"time"

	"go.uber.org/zap"
)

func (s *LGService) StartDeviceStateMonitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		s.log.Info("device state monitor started", zap.Duration("interval", interval))

		for {
			select {
			case <-ctx.Done():
				s.log.Info("device state monitor stopped")
				return
			case <-ticker.C:
				s.refreshDeviceStates(ctx)
			}
		}
	}()
}

func (s *LGService) refreshDeviceStates(ctx context.Context) {
	failed := 0

	for deviceID, device := range s.devices {
		if device.Device.DeviceInfo.DeviceType != "DEVICE_AIR_CONDITIONER" {
			s.log.Warn("skipping state refresh: unsupported device type",
				zap.String("deviceID", deviceID),
				zap.String("deviceType", device.Device.DeviceInfo.DeviceType),
			)
			continue
		}

		raw, err := s.deviceService.GetState(ctx, deviceID)
		if err != nil {
			var apiErr *lg.APIError
			if errors.As(err, &apiErr) {
				fmt.Printf("device %s desconectado: %v\n", deviceID, apiErr)
			}

			s.log.Error("failed to get device state", zap.String("deviceID", deviceID), zap.Error(err))
			failed++
			continue
		}

		state, err := s.stateParser.ParseAirConditionerState(deviceID, raw)
		if err != nil {
			s.log.Error("failed to parse device state", zap.String("deviceID", deviceID), zap.Error(err))
			failed++
			continue
		}

		if err := s.deviceStateStore.SetSnapshot(ctx, deviceID, raw); err != nil {
			s.log.Error("failed to sync device state to redis", zap.String("deviceID", deviceID), zap.Error(err))
		}

		device.LastState = state

		var p map[string]any
		json.Unmarshal(raw, &p)

		if err := s.repository.SaveFromAPI(ctx, deviceID, "LG", "telemetry", "/devices/"+deviceID+"/state", string(raw), p); err != nil {
			s.log.Error("failed to save raw message", zap.String("deviceID", deviceID), zap.Error(err))
		}

		normalized, err := s.stateNormalizer.NormalizeTelemetry(deviceID, device.Device.DeviceInfo.DeviceType, normalizers.EventCodeTracking, state)
		if err != nil {
			s.log.Error("failed to normalize telemetry", zap.String("deviceID", deviceID), zap.Error(err))
			failed++
			continue
		}

		s.log.Info("normalized telemetry ready to send", zap.ByteString("payload", normalized))

	}

	s.log.Info("Device states refreshed", zap.Int("devices", len(s.devices)), zap.Int("failed", failed))
}
