package lg_service

import (
	"context"
	"encoding/json"
	"errors"
	repository "mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/application/commands"
	"mqtt-api-service/internal/application/normalizers"
	"time"

	"mqtt-api-service/internal/adapters/api/lg"
	"mqtt-api-service/internal/adapters/cache"

	"go.uber.org/zap"
)

// deviceRefreshCounters separa los distintos resultados posibles de un
// ciclo de polling. Antes de FASE LG-1B existía un único contador "failed"
// que no distinguía un fallo real de publicación de un dispositivo
// simplemente desconectado (LG 416/1222) — lo que producía resúmenes
// engañosos como "failed publishing telemetry" seguido de "failed:0".
type deviceRefreshCounters struct {
	stateReadFailed        int
	telemetryPublished     int
	telemetryPublishFailed int
	disconnected           int
	skipped                int
}

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
	var counters deviceRefreshCounters

	for deviceID, device := range s.devices {
		if device.Device.DeviceInfo.DeviceType != "DEVICE_AIR_CONDITIONER" {
			s.log.Warn("skipping state refresh: unsupported device type",
				zap.String("deviceID", deviceID),
				zap.String("deviceType", device.Device.DeviceInfo.DeviceType),
			)
			counters.skipped++
			continue
		}

		raw, err := s.deviceService.GetState(ctx, deviceID)
		if err != nil {
			var apiErr *lg.APIError
			if errors.As(err, &apiErr) && apiErr.IsDeviceNotConnected() {
				// Condición operativa esperada (dispositivo apagado/offline
				// en la app LG), no un error crítico del servicio: se
				// loguea como warn sin repetir el error completo, y no
				// rompe el polling del resto de dispositivos.
				s.log.Warn("device disconnected",
					zap.String("deviceID", deviceID),
					zap.String("lgErrorCode", apiErr.Code),
					zap.Int("httpStatus", apiErr.StatusCode),
				)
				counters.disconnected++

				s.recordDeviceStatus(ctx, deviceID, cache.DeviceStatus{
					Status:        "offline",
					LastErrorCode: apiErr.Code,
				})
				continue
			}

			s.log.Error("failed to get device state", zap.String("deviceID", deviceID), zap.Error(err))
			counters.stateReadFailed++
			continue
		}

		state, err := s.stateParser.ParseAirConditionerState(deviceID, raw)
		if err != nil {
			s.log.Error("failed to parse device state", zap.String("deviceID", deviceID), zap.Error(err))
			counters.stateReadFailed++
			continue
		}

		if err := s.deviceStateStore.SetSnapshot(ctx, deviceID, raw); err != nil {
			s.log.Error("failed to sync device state to redis", zap.String("deviceID", deviceID), zap.Error(err))
		}

		device.LastState = state

		if s.confirmationManager != nil {
			s.confirmationManager.TryConfirm(ctx, deviceID, commands.CurrentState{
				Power:             state.Operation.AirConOperationMode == "POWER_ON",
				Mode:              state.AirConJobMode.CurrentJobMode,
				TemperatureTarget: state.Temperature.TargetTemperature,
				Airflow:           state.AirFlow.WindStrength,
				Oscillation:       state.WindDirection.RotateUpDown,
				PowerSave:         state.PowerSave.PowerSaveEnabled,
			})
		}

		var p map[string]any
		json.Unmarshal(raw, &p)

		if err := s.repository.Save(
			ctx,
			repository.RawMessage{
				IMEI:        deviceID,
				Brand:       "LG",
				MessageType: "telemetry",
				Endpoint:    "/devices/" + deviceID + "/state",
				Payload:     p,
				PayloadRaw:  string(raw),
			},
		); err != nil {
			s.log.Error(
				"failed to save raw message",
				zap.String("deviceID", deviceID),
				zap.Error(err),
			)
		}

		if err := s.publishTracking(
			ctx,
			deviceID,
			device.Device.DeviceInfo.DeviceType,
			normalizers.EventCodeTracking,
			state,
		); err != nil {

			s.log.Error(
				"failed publishing telemetry",
				zap.String("deviceID", deviceID),
				zap.Error(err),
			)
			counters.telemetryPublishFailed++
			continue
		}

		counters.telemetryPublished++
		s.recordDeviceStatus(ctx, deviceID, cache.DeviceStatus{
			Status:     "online",
			LastSeenAt: time.Now().UTC(),
		})
	}

	s.log.Info("Device states refreshed",
		zap.Int("devices", len(s.devices)),
		zap.Int("telemetryPublished", counters.telemetryPublished),
		zap.Int("stateReadFailed", counters.stateReadFailed),
		zap.Int("telemetryPublishFailed", counters.telemetryPublishFailed),
		zap.Int("disconnected", counters.disconnected),
		zap.Int("skipped", counters.skipped),
	)
}

// recordDeviceStatus guarda un resumen operativo mínimo en Redis
// (lg:device:<deviceID>:status). Es puramente diagnóstico/best-effort: un
// fallo aquí nunca debe interrumpir el flujo de polling/telemetry, solo se
// loguea como warn.
//
// TODO (FASE LG-1B): se evaluó publicar este mismo cambio de estado
// (online/offline) como un RawMessage adicional por gRPC en
// devices/<deviceID>/status, pero no se implementó: requeriría asumir cómo
// ingestion-service/Payload Profiles interpretan un topic "status" para un
// dispositivo LG, algo que esta fase no puede verificar sin tocar
// tracking-platform (regla explícita de esta fase). Inventar ese flujo sin
// validarlo del lado de tracking-platform arriesga un payload que se
// ingiere pero no se interpreta, lo cual sería peor que no enviarlo. El
// estado queda disponible localmente vía Redis (device disconnected/online
// en logs + lg:device:<deviceID>:status) hasta que se decida y valide ese
// flujo del lado de tracking-platform.
func (s *LGService) recordDeviceStatus(ctx context.Context, deviceID string, status cache.DeviceStatus) {
	if err := s.deviceStateStore.SetDeviceStatus(ctx, deviceID, status); err != nil {
		s.log.Warn("failed to record device status in redis",
			zap.String("deviceID", deviceID),
			zap.String("status", status.Status),
			zap.Error(err),
		)
	}
}
