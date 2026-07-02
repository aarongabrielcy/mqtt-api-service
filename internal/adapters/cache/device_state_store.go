package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	deviceStateKeyPrefix = "device:state:"
	defaultStateTTL      = 24 * time.Hour
)

type DeviceStateStore struct {
	client *redis.Client
	log    *zap.Logger
}

func NewDeviceStateStore(client *redis.Client, log *zap.Logger) *DeviceStateStore {
	return &DeviceStateStore{
		client: client,
		log:    log,
	}
}

func stateKey(deviceID string) string {
	return deviceStateKeyPrefix + deviceID
}

// SetSnapshot sobreescribe por completo el estado guardado de un dispositivo.
// Se usa para el primer mensaje que llega (el snapshot completo).
func (s *DeviceStateStore) SetSnapshot(ctx context.Context, deviceID string, snapshot json.RawMessage) error {
	if err := s.client.Set(ctx, stateKey(deviceID), []byte(snapshot), defaultStateTTL).Err(); err != nil {
		return fmt.Errorf("failed to set snapshot for device %s: %w", deviceID, err)
	}

	s.log.Debug("device snapshot stored",
		zap.String("deviceID", deviceID),
		zap.Int("bytes", len(snapshot)),
	)

	return nil
}

func (s *DeviceStateStore) MergePartial(ctx context.Context, deviceID string, partial json.RawMessage) (json.RawMessage, error) {
	current, err := s.client.Get(ctx, stateKey(deviceID)).Bytes()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to read current state for device %s: %w", deviceID, err)
	}

	var currentMap map[string]interface{}
	if err == nil {
		if unmarshalErr := json.Unmarshal(current, &currentMap); unmarshalErr != nil {
			return nil, fmt.Errorf("failed to parse stored state for device %s: %w", deviceID, unmarshalErr)
		}
	} else {
		s.log.Warn("no previous state found, using partial as base",
			zap.String("deviceID", deviceID),
		)
		currentMap = make(map[string]interface{})
	}

	var partialMap map[string]interface{}
	if err := json.Unmarshal(partial, &partialMap); err != nil {
		return nil, fmt.Errorf("failed to parse partial update for device %s: %w", deviceID, err)
	}

	merged := deepMerge(currentMap, partialMap)

	mergedBytes, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize merged state for device %s: %w", deviceID, err)
	}

	if err := s.client.Set(ctx, stateKey(deviceID), mergedBytes, defaultStateTTL).Err(); err != nil {
		return nil, fmt.Errorf("failed to persist merged state for device %s: %w", deviceID, err)
	}

	s.log.Debug("device state merged",
		zap.String("deviceID", deviceID),
		zap.Int("bytes", len(mergedBytes)),
	)

	return mergedBytes, nil
}

func (s *DeviceStateStore) GetState(ctx context.Context, deviceID string) (json.RawMessage, error) {
	data, err := s.client.Get(ctx, stateKey(deviceID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("no state found for device %s", deviceID)
		}
		return nil, fmt.Errorf("failed to get state for device %s: %w", deviceID, err)
	}

	return data, nil
}

func deepMerge(base, patch map[string]interface{}) map[string]interface{} {
	for key, patchVal := range patch {
		baseVal, exists := base[key]
		if !exists {
			base[key] = patchVal
			continue
		}

		baseObj, baseIsObj := baseVal.(map[string]interface{})
		patchObj, patchIsObj := patchVal.(map[string]interface{})

		if baseIsObj && patchIsObj {
			base[key] = deepMerge(baseObj, patchObj)
			continue
		}

		base[key] = patchVal
	}

	return base
}
