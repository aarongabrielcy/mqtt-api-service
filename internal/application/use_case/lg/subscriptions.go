package lg_service

import (
	"context"
	"mqtt-api-service/internal/adapters/api/lg"
	"time"

	"go.uber.org/zap"
)

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
	entries := s.devices.Snapshot()

	for _, entry := range entries {
		deviceID := entry.DeviceID
		device := entry.Device

		if _, ok := pushMap[deviceID]; ok {
			device.SetPushSubscribed(true)
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

		device.SetPushSubscribed(true)
	}

	s.log.Info(
		"Push subscriptions synchronized",
		zap.Int("devices", len(entries)),
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
	entries := s.devices.Snapshot()

	for _, entry := range entries {
		deviceID := entry.DeviceID
		device := entry.Device

		if event, ok := eventMap[deviceID]; ok {
			device.SetEventSubscription(true, event.TTL)
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

		device.SetEventSubscription(true, 0)
	}

	s.log.Info(
		"Event subscriptions synchronized",
		zap.Int("devices", len(entries)),
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
