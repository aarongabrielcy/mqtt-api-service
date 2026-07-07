package lg_service

import (
	"context"
	"mqtt-api-service/internal/adapters/api/lg"

	"go.uber.org/zap"
)

func (s *LGService) syncDevices(ctx context.Context) error {
	devices, err := s.deviceService.List(ctx)
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(devices))

	for _, device := range devices {
		if !s.isSupportedDevice(device) {
			continue
		}

		seen[device.DeviceID] = struct{}{}

		if existing, ok := s.devices[device.DeviceID]; ok {
			existing.Device = device
			continue
		}

		s.devices[device.DeviceID] = &ManagedDevice{
			Device: device,
		}
	}

	removed := 0
	for deviceID := range s.devices {
		if _, ok := seen[deviceID]; !ok {
			delete(s.devices, deviceID)
			removed++
		}
	}

	s.log.Info(
		"LG devices synchronized",
		zap.Int("count", len(s.devices)),
		zap.Int("removed", removed),
	)

	return nil
}

func (s *LGService) isSupportedDevice(device lg.Device) bool {
	switch device.DeviceInfo.DeviceType {
	case "DEVICE_AIR_CONDITIONER":
		return true
	default:
		return false
	}
}
