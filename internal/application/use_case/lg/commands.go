package lg_service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mqtt-api-service/internal/adapters/api/lg"

	"go.uber.org/zap"
)

func (s *LGService) SetDevicePower(ctx context.Context, deviceID string, on bool) error {
	s.log.Info("SetDevicePower called",
		zap.String("deviceID", deviceID),
		zap.Bool("on", on),
	)
	if _, ok := s.devices[deviceID]; !ok {
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
		var apiErr *lg.APIError
		if errors.As(err, &apiErr) {
			fmt.Printf("device %s desconectado: %v\n", deviceID, apiErr)
		}

		s.log.Error("failed to set device power", zap.String("deviceID", deviceID), zap.Error(err))
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
	if _, ok := s.devices[deviceID]; !ok {
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
		var apiErr *lg.APIError
		if errors.As(err, &apiErr) {
			fmt.Printf("device %s desconectado: %v\n", deviceID, apiErr)
		}

		s.log.Error("failed to set device temperature", zap.String("deviceID", deviceID), zap.Error(err))
		return err
	}

	s.log.Info("device temperature changed", zap.String("deviceID", deviceID), zap.Float64("temperature", temperature))
	return nil
}
