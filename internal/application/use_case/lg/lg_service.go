package lg_service

import (
	"context"
	"encoding/json"
	"mqtt-api-service/internal/adapters/api/lg"
	"mqtt-api-service/internal/adapters/cache"
	mongo "mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/adapters/parser"
	"mqtt-api-service/internal/application/commands"
	"mqtt-api-service/internal/application/normalizers"
	"mqtt-api-service/internal/domain/interfaces"
	"mqtt-api-service/internal/infrastructure/config"

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

	trackingClient interfaces.TrackingClient

	// confirmationManager es opcional (nil si LG_COMMANDS_ENABLED=false):
	// cuando está seteado, cada refresh de estado intenta confirmar por
	// estado cualquier comando LG pendiente (ver
	// SetConfirmationManager/state_polling.go).
	confirmationManager *commands.ConfirmationManager

	// debugStateLogs habilita el log "LG state parsed" (FASE LG-CMD-2E) en
	// state_polling.go/push_handler.go, con el diagnóstico de presencia del
	// campo Oscillation en el JSON crudo de LG.
	debugStateLogs bool
}

// SetConfirmationManager inyecta el ConfirmationManager del bridge de
// comandos LG (ver internal/application/commands). Se hace por setter, no
// por parámetro del constructor, porque es una dependencia opcional cuya
// construcción en main.go depende a su vez de trackingClient — ya inyectado
// aquí — evitando así un ciclo de inicialización.
func (s *LGService) SetConfirmationManager(cm *commands.ConfirmationManager) {
	s.confirmationManager = cm
}

type ManagedDevice struct {
	Device lg.Device

	PushSubscribed  bool
	EventSubscribed bool
	EventTTL        int64

	LastState *parser.AirConditionerState
}

func NewLGService(cfg *config.Config, log *zap.Logger, repo mongo.RawMessageRepository,
	deviceStateStore *cache.DeviceStateStore, trackingClient interfaces.TrackingClient) (*LGService, error) {
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
		stateNormalizer:  normalizers.NewLGStateNormalizer(log, cfg.LGCommands.DebugStateLogs),
		trackingClient:   trackingClient,
		deviceStateStore: deviceStateStore,
		log:              log,
		clientID:         cfg.LGApi.ClientID,
		repository:       repo,
		devices:          make(map[string]*ManagedDevice),
		debugStateLogs:   cfg.LGCommands.DebugStateLogs,
	}, nil
}

// logParsedStateIfEnabled loguea el estado LG ya parseado (equivalente a
// LGStateInfo) junto con un diagnóstico de presencia del campo Oscillation
// en el JSON crudo (FASE LG-CMD-2E), solo si LG_DEBUG_STATE_LOGS=true.
// Oscillation=false en el estado parseado puede significar "el dispositivo
// reportó apagada la oscilación" O "el campo windDirection.rotateUpDown no
// vino en esta respuesta y AirConditionerState lo defaulteó a false" — este
// log es la única forma de distinguir ambos casos sin adivinar.
func (s *LGService) logParsedStateIfEnabled(deviceID string, raw json.RawMessage, state *parser.AirConditionerState) {
	if !s.debugStateLogs || state == nil {
		return
	}

	diag := parser.InspectOscillationField(raw)

	s.log.Debug("LG state parsed",
		zap.String("deviceID", deviceID),
		zap.Bool("power", state.Operation.AirConOperationMode == "POWER_ON"),
		zap.String("mode", state.AirConJobMode.CurrentJobMode),
		zap.String("operationMode", state.Operation.AirConOperationMode),
		zap.String("airflow", state.AirFlow.WindStrength),
		zap.Bool("oscillation", state.WindDirection.RotateUpDown),
		zap.Bool("powersave", state.PowerSave.PowerSaveEnabled),
		zap.Bool("oscillationPresent", diag.Present),
		zap.String("oscillationSource", diag.Source),
		zap.Any("oscillationRaw", diag.Raw),
	)
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
