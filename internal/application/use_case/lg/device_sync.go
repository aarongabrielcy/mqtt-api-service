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

		if existing, ok := s.devices.Get(device.DeviceID); ok {
			existing.SetDevice(device)
			continue
		}

		md := &ManagedDevice{}
		md.SetDevice(device)
		s.devices.Set(device.DeviceID, md)
	}

	removed := 0
	for _, entry := range s.devices.Snapshot() {
		if _, ok := seen[entry.DeviceID]; !ok {
			s.devices.Delete(entry.DeviceID)
			removed++
		}
	}

	s.log.Info(
		"LG devices synchronized",
		zap.Int("count", s.devices.Len()),
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
