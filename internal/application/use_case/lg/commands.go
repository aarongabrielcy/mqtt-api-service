package lg_service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mqtt-api-service/internal/adapters/api/lg"

	"go.uber.org/zap"
)

func (s *LGService) logControlError(err error, action string, fields ...zap.Field) {
	var apiErr *lg.APIError
	if errors.As(err, &apiErr) && apiErr.IsDeviceNotConnected() {
		s.log.Warn("device disconnected",
			append(fields,
				zap.String("lgErrorCode", apiErr.Code),
				zap.Int("httpStatus", apiErr.StatusCode),
			)...,
		)
		return
	}

	s.log.Error(action, append(fields, zap.Error(err))...)
}

func (s *LGService) SetDevicePower(ctx context.Context, deviceID string, on bool) error {
	s.log.Info("SetDevicePower called",
		zap.String("deviceID", deviceID),
		zap.Bool("on", on),
	)
	if _, ok := s.devices.Get(deviceID); !ok {
		return fmt.Errorf("device %s not managed", deviceID)
	}

	mode := "POWER_OFF"
	if on {
		mode = "POWER_ON"
	}

	payload := map[string]interface{}{
		"operation": map[string]interface{}{
			"airConOperationMode": mode,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to build control payload: %w", err)
	}

	if err := s.deviceService.ControlState(ctx, deviceID, body); err != nil {
		s.logControlError(err, "failed to set device power", zap.String("deviceID", deviceID))
		return err
	}

	s.log.Info("device power changed", zap.String("deviceID", deviceID), zap.String("mode", mode))
	return nil
}

func (s *LGService) SetDeviceTemperature(ctx context.Context, deviceID string, temperature float64) error {
	s.log.Info("SetDeviceTemperature called",
		zap.String("deviceID", deviceID),
		zap.Float64("temperature", temperature),
	)
	if _, ok := s.devices.Get(deviceID); !ok {
		return fmt.Errorf("device %s not managed", deviceID)
	}

	payload := map[string]interface{}{
		"temperature": map[string]interface{}{
			"targetTemperature": temperature,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to build control payload: %w", err)
	}

	if err := s.deviceService.ControlState(ctx, deviceID, body); err != nil {
		s.logControlError(err, "failed to set device temperature", zap.String("deviceID", deviceID))
		return err
	}

	s.log.Info("device temperature changed", zap.String("deviceID", deviceID), zap.Float64("temperature", temperature))
	return nil
}

func (s *LGService) SetAirFlow(ctx context.Context, deviceID string, strength string) error {
	s.log.Info("SetAirFlow called",
		zap.String("deviceID", deviceID),
		zap.String("strength", strength),
	)

	if _, ok := s.devices.Get(deviceID); !ok {
		return fmt.Errorf("device %s not managed", deviceID)
	}

	validStrengths := map[string]bool{
		"LOW":  true,
		"MID":  true,
		"HIGH": true,
		"AUTO": true,
	}

	if !validStrengths[strength] {
		return fmt.Errorf("invalid air flow strength: %s", strength)
	}

	payload := map[string]interface{}{
		"airFlow": map[string]interface{}{
			"windStrength": strength,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to build air flow payload: %w", err)
	}

	if err := s.deviceService.ControlState(ctx, deviceID, body); err != nil {
		s.logControlError(err, "failed to set air flow",
			zap.String("deviceID", deviceID),
			zap.String("strength", strength),
		)
		return err
	}

	s.log.Info("air flow changed",
		zap.String("deviceID", deviceID),
		zap.String("strength", strength),
	)

	return nil
}

func (s *LGService) SetOperationMode(ctx context.Context, deviceID string, mode string) error {
	s.log.Info("SetOperationMode called",
		zap.String("deviceID", deviceID),
		zap.String("mode", mode),
	)

	if _, ok := s.devices.Get(deviceID); !ok {
		return fmt.Errorf("device %s not managed", deviceID)
	}

	validModes := map[string]bool{
		"COOL":    true,
		"AUTO":    true,
		"FAN":     true,
		"AIR_DRY": true,
	}

	if !validModes[mode] {
		return fmt.Errorf("invalid air conditioner mode: %s", mode)
	}

	payload := map[string]interface{}{
		"airConJobMode": map[string]interface{}{
			"currentJobMode": mode,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to build operation mode payload: %w", err)
	}

	if err := s.deviceService.ControlState(ctx, deviceID, body); err != nil {
		s.logControlError(err, "failed to set operation mode",
			zap.String("deviceID", deviceID),
			zap.String("mode", mode),
		)
		return err
	}

	s.log.Info("operation mode changed",
		zap.String("deviceID", deviceID),
		zap.String("mode", mode),
	)

	return nil
}

func (s *LGService) SetOscillation(ctx context.Context, deviceID string, enabled bool) error {
	s.log.Info("SetOscillation called",
		zap.String("deviceID", deviceID),
		zap.Bool("enabled", enabled),
	)

	if _, ok := s.devices.Get(deviceID); !ok {
		return fmt.Errorf("device %s not managed", deviceID)
	}

	payload := map[string]interface{}{
		"windDirection": map[string]interface{}{
			"rotateUpDown": enabled,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to build oscillation payload: %w", err)
	}

	if err := s.deviceService.ControlState(ctx, deviceID, body); err != nil {
		s.logControlError(err, "failed to set oscillation",
			zap.String("deviceID", deviceID),
			zap.Bool("enabled", enabled),
		)
		return err
	}

	s.log.Info("oscillation changed",
		zap.String("deviceID", deviceID),
		zap.Bool("enabled", enabled),
	)

	return nil
}

func (s *LGService) SetPowerSave(ctx context.Context, deviceID string, enabled bool) error {
	s.log.Info("SetPowerSave called",
		zap.String("deviceID", deviceID),
		zap.Bool("enabled", enabled),
	)

	if _, ok := s.devices.Get(deviceID); !ok {
		return fmt.Errorf("device %s not managed", deviceID)
	}

	payload := map[string]interface{}{
		"powerSave": map[string]interface{}{
			"powerSaveEnabled": enabled,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to build power save payload: %w", err)
	}

	if err := s.deviceService.ControlState(ctx, deviceID, body); err != nil {
		s.logControlError(err, "failed to set power save",
			zap.String("deviceID", deviceID),
			zap.Bool("enabled", enabled),
		)
		return err
	}

	s.log.Info("power save changed",
		zap.String("deviceID", deviceID),
		zap.Bool("enabled", enabled),
	)

	return nil
}
