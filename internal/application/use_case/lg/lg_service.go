package lg_service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mqtt-api-service/internal/adapters/api/lg"
	"mqtt-api-service/internal/adapters/cache"
	mongo "mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/adapters/parser"
	"mqtt-api-service/internal/application/normalizers"
	"mqtt-api-service/internal/infrastructure/config"
	"time"

	"go.uber.org/zap"
)

type LGService struct {
	deviceService   *lg.DeviceService
	pushService     *lg.PushService
	registryService *lg.DeviceRegistryService
	eventService    *lg.EventService

	stateParser     *parser.LGStateParser
	pushParser      *parser.LGPushParser
	stateNormalizer *normalizers.LGStateNormalizer

	deviceStateStore *cache.DeviceStateStore

	repository mongo.RawMessageRepository
	log        *zap.Logger

	clientID string

	devices map[string]*ManagedDevice
}

type ManagedDevice struct {
	Device lg.Device

	PushSubscribed  bool
	EventSubscribed bool
	EventTTL        int64

	LastState *parser.AirConditionerState
}

func NewLGService(cfg *config.Config, log *zap.Logger, repo mongo.RawMessageRepository, deviceStateStore *cache.DeviceStateStore) (*LGService, error) {
	lgClient, err := lg.NewLGAPIClient(cfg, log)
	if err != nil {
		return nil, err
	}

	return &LGService{
		deviceService:    lg.NewDeviceService(lgClient),
		pushService:      lg.NewPushService(lgClient),
		registryService:  lg.NewDeviceRegistryService(lgClient),
		eventService:     lg.NewEventService(lgClient),
		stateParser:      parser.NewLGStateParser(log),
		stateNormalizer:  normalizers.NewLGStateNormalizer(log),
		deviceStateStore: deviceStateStore,
		log:              log,
		clientID:         cfg.LGApi.ClientID,
		repository:       repo,
		devices:          make(map[string]*ManagedDevice),
	}, nil
}

func (s *LGService) Initialize(ctx context.Context) error {
	if err := s.syncDevices(ctx); err != nil {
		return err
	}

	if err := s.ensureRegistrySubscription(ctx); err != nil {
		return err
	}

	if err := s.ensureDeviceSubscriptions(ctx); err != nil {
		return err
	}

	return nil
}

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

func (s *LGService) ensureRegistrySubscription(ctx context.Context) error {
	clientIDs, err := s.registryService.List(ctx)
	if err != nil {
		return err
	}

	for _, id := range clientIDs {
		if string(id) == s.clientID {
			s.log.Info("Registry subscription already exists")
			return nil
		}
	}

	s.log.Info("Registry subscription not found, subscribing...")

	return s.registryService.Subscribe(ctx)
}

func (s *LGService) ensureDeviceSubscriptions(ctx context.Context) error {
	if err := s.ensurePushSubscriptions(ctx); err != nil {
		return err
	}

	if err := s.ensureEventSubscriptions(ctx); err != nil {
		return err
	}

	return nil
}

func (s *LGService) ensurePushSubscriptions(ctx context.Context) error {
	pushSubscriptions, err := s.pushService.List(ctx)
	if err != nil {
		return err
	}

	pushMap := make(map[string]struct{})
	for _, sub := range pushSubscriptions {
		pushMap[sub.DeviceID] = struct{}{}
	}

	failed := 0

	for deviceID, device := range s.devices {

		if _, ok := pushMap[deviceID]; ok {
			device.PushSubscribed = true
			continue
		}

		if err := s.pushService.Subscribe(ctx, deviceID); err != nil {
			s.log.Error(
				"failed to subscribe device to push",
				zap.String("deviceID", deviceID),
				zap.Error(err),
			)
			failed++
			continue
		}

		device.PushSubscribed = true
	}

	s.log.Info(
		"Push subscriptions synchronized",
		zap.Int("devices", len(s.devices)),
		zap.Int("failed", failed),
	)

	return nil
}

func (s *LGService) ensureEventSubscriptions(ctx context.Context) error {
	eventSubscriptions, err := s.eventService.List(ctx)
	if err != nil {
		return err
	}

	eventMap := make(map[string]lg.Event)
	for _, sub := range eventSubscriptions {
		eventMap[sub.DeviceID] = sub
	}

	const eventSubscriptionHours = 24

	failed := 0

	for deviceID, device := range s.devices {

		if event, ok := eventMap[deviceID]; ok {
			device.EventSubscribed = true
			device.EventTTL = event.TTL
			continue
		}

		if err := s.eventService.Subscribe(ctx, deviceID, eventSubscriptionHours); err != nil {
			s.log.Error(
				"failed to subscribe device to events",
				zap.String("deviceID", deviceID),
				zap.Error(err),
			)
			failed++
			continue
		}

		device.EventSubscribed = true
	}

	s.log.Info(
		"Event subscriptions synchronized",
		zap.Int("devices", len(s.devices)),
		zap.Int("failed", failed),
	)

	return nil
}

func (s *LGService) StartEventSubscriptionMonitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		s.log.Info("event subscription monitor started", zap.Duration("interval", interval))

		for {
			select {
			case <-ctx.Done():
				s.log.Info("event subscription monitor stopped")
				return
			case <-ticker.C:
				if err := s.ensureEventSubscriptions(ctx); err != nil {
					s.log.Error("failed to verify event subscriptions", zap.Error(err))
				}
			}
		}
	}()
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

func (s *LGService) HandlePushMessage(ctx context.Context, topic string, rawPayload []byte) error {
	msg, err := s.pushParser.Parse(topic, rawPayload)
	if err != nil {
		s.log.Error("failed to parse push message",
			zap.String("topic", topic),
			zap.Error(err),
		)
		return err
	}

	if len(msg.Report) == 0 {
		s.log.Debug("push message without report, skipping",
			zap.String("deviceID", msg.DeviceID),
			zap.String("pushType", msg.PushType),
		)
		return nil
	}

	mergedState, err := s.deviceStateStore.MergePartial(ctx, msg.DeviceID, msg.Report)
	if err != nil {
		s.log.Error("failed to merge device state in redis",
			zap.String("deviceID", msg.DeviceID),
			zap.Error(err),
		)
		return err
	}

	var reportMap map[string]json.RawMessage
	if err := json.Unmarshal(msg.Report, &reportMap); err != nil {
		s.log.Error("failed to inspect report fields",
			zap.String("deviceID", msg.DeviceID),
			zap.Error(err),
		)
		return err
	}

	operationRaw, hasOperation := reportMap["operation"]
	isPureOperationChange := hasOperation && len(reportMap) == 1

	if !isPureOperationChange {
		s.log.Debug("push message is not a dedicated operation change, state updated in redis only",
			zap.String("deviceID", msg.DeviceID),
		)
		return nil
	}

	var operation struct {
		AirConOperationMode string `json:"airConOperationMode"`
	}
	if err := json.Unmarshal(operationRaw, &operation); err != nil {
		s.log.Error("failed to parse operation field",
			zap.String("deviceID", msg.DeviceID),
			zap.Error(err),
		)
		return err
	}

	var state parser.AirConditionerState
	if err := json.Unmarshal(mergedState, &state); err != nil {
		s.log.Error("failed to unmarshal merged state",
			zap.String("deviceID", msg.DeviceID),
			zap.Error(err),
		)
		return err
	}
	var p map[string]any
	json.Unmarshal(mergedState, &p)

	if err := s.repository.SaveFromMQTT(ctx, msg.DeviceID, "LG", "push", topic, string(mergedState), p); err != nil {
		s.log.Error("failed to save push message",
			zap.String("deviceID", msg.DeviceID),
			zap.Error(err),
		)
	}

	var eventCode normalizers.EventCode
	switch operation.AirConOperationMode {
	case "POWER_ON":
		eventCode = normalizers.EventCodePowerOn
	case "POWER_OFF":
		eventCode = normalizers.EventCodePowerOff
	default:
		eventCode = normalizers.EventCodeTracking
	}

	normalized, err := s.stateNormalizer.NormalizeTelemetry(msg.DeviceID, msg.DeviceType, eventCode, &state)
	if err != nil {
		s.log.Error("failed to normalize push message",
			zap.String("deviceID", msg.DeviceID),
			zap.Error(err),
		)
		return err
	}

	// TODO: reemplazar este print por el envío real al otro servicio (gRPC)
	// una vez que esté listo.
	fmt.Println(string(normalized))

	return nil

}

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
