package lg_service

import (
	"context"
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
		stateNormalizer:  normalizers.NewLGStateNormalizer(log),
		trackingClient:   trackingClient,
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
