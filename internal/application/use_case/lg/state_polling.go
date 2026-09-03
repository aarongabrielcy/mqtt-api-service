package lg_service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	repository "mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/application/commands"
	"mqtt-api-service/internal/application/normalizers"
	"sync"
	"sync/atomic"
	"time"

	"mqtt-api-service/internal/adapters/api/lg"
	"mqtt-api-service/internal/adapters/cache"
	"mqtt-api-service/internal/adapters/parser"

	"go.uber.org/zap"
)

const stateRefreshConcurrencyLimit = 8

// deviceRefreshCounters separa los distintos resultados posibles de un
// ciclo de polling. Antes de FASE LG-1B existía un único contador "failed"
// que no distinguía un fallo real de publicación de un dispositivo
// simplemente desconectado (LG 416/1222) — lo que producía resúmenes
// engañosos como "failed publishing telemetry" seguido de "failed:0".
type deviceRefreshCounters struct {
	stateReadFailed        int64
	telemetryPublished     int64
	telemetryPublishFailed int64
	disconnected           int64
	skipped                int64
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
	entries := s.devices.Snapshot()

	var counters deviceRefreshCounters
	sem := make(chan struct{}, stateRefreshConcurrencyLimit)
	var wg sync.WaitGroup

	for _, entry := range entries {
		if ctx.Err() != nil {
			s.log.Warn("context cancelled, stopping device state refresh",
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
				s.log.Warn("skipping state refresh: unsupported device type",
					zap.String("deviceID", deviceID),
					zap.String("deviceType", deviceType),
				)
				atomic.AddInt64(&counters.skipped, 1)
				return
			}

			state, err := s.refreshDeviceState(ctx, deviceID, device, normalizers.EventCodeTracking)
			if err != nil {
				if state != nil {
					// El estado se leyó/parseó bien; falló solo la publicación
					// de telemetry — ya logueado dentro de refreshDeviceState.
					atomic.AddInt64(&counters.telemetryPublishFailed, 1)
					return
				}

				var apiErr *lg.APIError
				if errors.As(err, &apiErr) && apiErr.IsDeviceNotConnected() {
					atomic.AddInt64(&counters.disconnected, 1)
					return
				}

				atomic.AddInt64(&counters.stateReadFailed, 1)
				return
			}

			atomic.AddInt64(&counters.telemetryPublished, 1)
		}(entry.DeviceID, entry.Device)
	}

	wg.Wait()

	s.log.Info("Device states refreshed",
		zap.Int("devices", len(entries)),
		zap.Int64("telemetryPublished", counters.telemetryPublished),
		zap.Int64("stateReadFailed", counters.stateReadFailed),
		zap.Int64("telemetryPublishFailed", counters.telemetryPublishFailed),
		zap.Int64("disconnected", counters.disconnected),
		zap.Int64("skipped", counters.skipped),
	)
}

// refreshDeviceState hace un ciclo completo de refresh de estado para un
// solo dispositivo administrado: GET /devices/:id/state → parse → debug
// log (si LG_DEBUG_STATE_LOGS) → snapshot en Redis → TryConfirm contra
// cualquier pending → guardar raw en Mongo → publicar telemetry
// normalizada por gRPC IngestRaw con el eventCode indicado. Usado tanto por
// el polling periódico (eventCode=EventCodeTracking, "event=0") como por
// el refresh inmediato post-comando (FASE LG-CMD-2H,
// LGService.RefreshDeviceState) — un solo lugar donde vive esta lógica,
// para que ambos caminos se comporten idénticamente.
//
// El error de retorno distingue "no se pudo ni leer/parsear el estado"
// (state=nil) de "se leyó bien pero falló la publicación de telemetry"
// (state!=nil) — refreshDeviceStates usa esa distinción para clasificar sus
// contadores sin duplicar la lógica de clasificación de errores LG.
func (s *LGService) refreshDeviceState(
	ctx context.Context,
	deviceID string,
	device *ManagedDevice,
	eventCode normalizers.EventCode,
) (*parser.AirConditionerState, error) {
	raw, err := s.deviceService.GetState(ctx, deviceID)
	if err != nil {
		var apiErr *lg.APIError
		if errors.As(err, &apiErr) && apiErr.IsDeviceNotConnected() {
			// Condición operativa esperada (dispositivo apagado/offline en
			// la app LG), no un error crítico del servicio: se loguea como
			// warn sin repetir el error completo, y no rompe el resto del
			// ciclo de polling.
			s.log.Warn("device disconnected",
				zap.String("deviceID", deviceID),
				zap.String("lgErrorCode", apiErr.Code),
				zap.Int("httpStatus", apiErr.StatusCode),
			)
			s.recordDeviceStatus(ctx, deviceID, cache.DeviceStatus{
				Status:        "offline",
				LastErrorCode: apiErr.Code,
			})
			return nil, err
		}

		s.log.Error("failed to get device state", zap.String("deviceID", deviceID), zap.Error(err))
		return nil, err
	}

	state, err := s.stateParser.ParseAirConditionerState(deviceID, raw)
	if err != nil {
		s.log.Error("failed to parse device state", zap.String("deviceID", deviceID), zap.Error(err))
		return nil, err
	}

	s.logParsedStateIfEnabled(deviceID, raw, state)

	if err := s.deviceStateStore.SetSnapshot(ctx, deviceID, raw); err != nil {
		s.log.Error("failed to sync device state to redis", zap.String("deviceID", deviceID), zap.Error(err))
	}

	device.SetLastState(state)

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
		device.GetDevice().DeviceInfo.DeviceType,
		eventCode,
		state,
	); err != nil {
		s.log.Error(
			"failed publishing telemetry",
			zap.String("deviceID", deviceID),
			zap.Error(err),
		)
		return state, err
	}

	s.recordDeviceStatus(ctx, deviceID, cache.DeviceStatus{
		Status:     "online",
		LastSeenAt: time.Now().UTC(),
	})

	return state, nil
}

// RefreshDeviceState (FASE LG-CMD-2H) hace una lectura puntual de estado LG
// para deviceID, reutilizando el mismo pipeline que el polling periódico
// (ver refreshDeviceState). Se usa para el refresh inmediato disparado por
// CommandDispatcher tras un comando exitoso o ambiguo (2211) — nunca marca
// ACKNOWLEDGED por sí mismo, solo le da a TryConfirm una oportunidad de
// confirmar antes del siguiente ciclo de polling o push.
func (s *LGService) RefreshDeviceState(ctx context.Context, deviceID string) error {
	device, ok := s.devices.Get(deviceID)
	if !ok {
		return fmt.Errorf("device %s not managed", deviceID)
	}

	_, err := s.refreshDeviceState(ctx, deviceID, device, normalizers.EventCodeTracking)
	return err
}

// GetLastKnownPower (FASE LG-CMD-2I) implementa
// commands.LGCommander.GetLastKnownPower: lee el snapshot de estado más
// reciente de deviceID desde Redis (el mismo snapshot que actualizan tanto
// el polling periódico como los push de LG, a diferencia de
// ManagedDevice.LastState que solo se actualiza en polling/refresh) y
// determina si el A/C estaba encendido. known=false si todavía no hay
// snapshot guardado o si no se pudo interpretar — el llamador (dispatcher)
// trata eso como "sin evidencia" y no bloquea por precondición.
func (s *LGService) GetLastKnownPower(ctx context.Context, deviceID string) (known bool, powerOn bool) {
	raw, err := s.deviceStateStore.GetState(ctx, deviceID)
	if err != nil || len(raw) == 0 {
		return false, false
	}

	var state parser.AirConditionerState
	if err := json.Unmarshal(raw, &state); err != nil {
		s.log.Warn("failed to parse last known state for command precondition check",
			zap.String("deviceID", deviceID),
			zap.Error(err),
		)
		return false, false
	}

	return true, state.Operation.AirConOperationMode == "POWER_ON"
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
