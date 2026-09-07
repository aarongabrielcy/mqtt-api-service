package lg_service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lg "mqtt-api-service/internal/adapters/api/lg"
	repository "mqtt-api-service/internal/adapters/mongo"

	"go.uber.org/zap"
)

// energyRefreshConcurrencyLimit acota cuántas llamadas simultáneas a
// GetEnergyUsage se disparan contra la API de LG.
const energyRefreshConcurrencyLimit = 8

type energyRefreshCounters struct {
	energyRetrieved int64
	failed          int64
	disconnected    int64
	skipped         int64
}

func (s *LGService) StartEnergyMonitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		s.log.Info("energy usage monitor started", zap.Duration("interval", interval))

		for {
			select {
			case <-ctx.Done():
				s.log.Info("energy usage monitor stopped")
				return

			case <-ticker.C:
				s.refreshEnergyUsages(ctx)
			}
		}
	}()
}

func (s *LGService) refreshEnergyUsages(ctx context.Context) {
	entries := s.devices.Snapshot()

	var counters energyRefreshCounters
	sem := make(chan struct{}, energyRefreshConcurrencyLimit)
	var wg sync.WaitGroup

	for _, entry := range entries {
		if ctx.Err() != nil {
			s.log.Warn("context cancelled, stopping energy usage refresh",
				zap.Error(ctx.Err()),
			)
			break
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			s.log.Warn("context cancelled while waiting for slot",
				zap.Error(ctx.Err()),
			)
			wg.Wait()
			return
		}

		wg.Add(1)
		go func(deviceID string, device *ManagedDevice) {
			defer wg.Done()
			defer func() { <-sem }()

			deviceType := device.GetDevice().DeviceInfo.DeviceType
			if deviceType != "DEVICE_AIR_CONDITIONER" {
				s.log.Warn(
					"skipping energy usage: unsupported device type",
					zap.String("deviceID", deviceID),
					zap.String("deviceType", deviceType),
				)
				atomic.AddInt64(&counters.skipped, 1)
				return
			}

			usage, rawBody, err := s.refreshEnergyUsage(ctx, deviceID)
			if err != nil {
				var apiErr *lg.APIError
				if errors.As(err, &apiErr) && apiErr.IsDeviceNotConnected() {
					s.log.Warn("device disconnected, skipping energy usage",
						zap.String("deviceID", deviceID),
						zap.String("lgErrorCode", apiErr.Code),
						zap.Int("httpStatus", apiErr.StatusCode),
					)
					atomic.AddInt64(&counters.disconnected, 1)
					return
				}

				s.log.Error(
					"failed to get energy usage",
					zap.String("deviceID", deviceID),
					zap.Error(err),
				)
				atomic.AddInt64(&counters.failed, 1)
				return
			}

			var dailyUsageKwh float64
			today := time.Now().Format("20060102")
			for _, data := range usage.DataList {
				s.log.Info(
					"energy usage retrieved",
					zap.String("deviceID", deviceID),
					zap.String("date", data.UsedDate),
					zap.Float64("energyUsage", data.EnergyUsage),
				)
				if data.UsedDate == today {
					dailyUsageKwh = data.EnergyUsage
				}
			}

			var p map[string]any
			json.Unmarshal(rawBody, &p)

			if err := s.repository.Save(
				ctx,
				repository.RawMessage{
					IMEI:        deviceID,
					Brand:       "LG",
					MessageType: "energy_usage",
					Endpoint:    "/devices/energy/" + deviceID + "/usage",
					Payload:     p,
					PayloadRaw:  string(rawBody),
				},
			); err != nil {
				s.log.Error(
					"failed to save raw energy message",
					zap.String("deviceID", deviceID),
					zap.Error(err),
				)
			}

			if err := s.publishEnergyTracking(ctx, deviceID, deviceType, dailyUsageKwh); err != nil {
				s.log.Error(
					"failed publishing energy telemetry",
					zap.String("deviceID", deviceID),
					zap.Error(err),
				)
				atomic.AddInt64(&counters.failed, 1)
				return
			}

			atomic.AddInt64(&counters.energyRetrieved, 1)
		}(entry.DeviceID, entry.Device)
	}

	wg.Wait()

	s.log.Info("Energy usages refreshed",
		zap.Int("devices", len(entries)),
		zap.Int64("energyRetrieved", counters.energyRetrieved),
		zap.Int64("failed", counters.failed),
		zap.Int64("disconnected", counters.disconnected),
		zap.Int64("skipped", counters.skipped),
	)
}

func (s *LGService) refreshEnergyUsage(
	ctx context.Context,
	deviceID string,
) (*lg.EnergyUsageResponse, []byte, error) {
	if deviceID == "" {
		return nil, nil, fmt.Errorf("device ID is required")
	}

	now := time.Now()
	date := now.Format("20060102")

	return s.energyService.GetEnergyUsage(
		ctx,
		lg.EnergyUsageRequest{
			DeviceID:  deviceID,
			Period:    "DAILY",
			StartDate: date,
			EndDate:   date,
		},
	)
}
