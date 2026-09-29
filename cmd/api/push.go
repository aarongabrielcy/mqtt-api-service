package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"mqtt-api-service/internal/adapters/mqtt"
	"mqtt-api-service/internal/infrastructure/config"
)

// pushQoS es el QoS de las suscripciones push/inbox LG (sin cambios respecto
// al contrato previo).
const pushQoS byte = 1

// pushClientFactory construye el cliente MQTT del broker LG; en tests se
// sustituye para probar la composición sin contactar a LG.
type pushClientFactory func(cfg config.Config, log *zap.Logger, subs []mqtt.Subscription) (mqtt.Client, error)

// pushSubscriptions es el conjunto de suscripciones requeridas del
// transporte push LG: app/clients/<LG_CLIENT_ID>/{push,inbox}, QoS 1.
func pushSubscriptions(cfg *config.Config, pushHandler, inboxHandler mqtt.MessageHandler) []mqtt.Subscription {
	return []mqtt.Subscription{
		{Topic: fmt.Sprintf("app/clients/%s/push", cfg.LG.ClientID), QoS: pushQoS, Handler: pushHandler},
		{Topic: fmt.Sprintf("app/clients/%s/inbox", cfg.LG.ClientID), QoS: pushQoS, Handler: inboxHandler},
	}
}

// startPushTransport abre el transporte push-MQTT LG solo si
// LG_PUSH_ENABLED lo permite (FR-02). Con push deshabilitado no se construye
// el cliente, no se conecta ni se suscribe, y devuelve nil.
//
// Nunca es fatal: un fallo del primer connect lo reintenta el supervisor del
// cliente, y un error de preparación (p. ej. TLS) deja el servicio sin push
// pero con polling, comandos y confirmación funcionando. Devuelve nil si no
// hay cliente que detener en el shutdown.
func startPushTransport(ctx context.Context, cfg *config.Config, log *zap.Logger, factory pushClientFactory, subs []mqtt.Subscription) mqtt.Client {
	if !cfg.LG.PushEnabled {
		log.Info("LG push MQTT disabled by policy",
			zap.String("appEnv", cfg.App.Environment),
			zap.Bool("explicit", cfg.LG.PushEnabledExplicit),
		)
		return nil
	}

	client, err := factory(*cfg, log, subs)
	if err != nil {
		log.Error("LG push MQTT unavailable; continuing without push", zap.Error(err))
		return nil
	}

	if err := client.Connect(ctx); err != nil {
		log.Error("LG push MQTT unavailable; continuing without push", zap.Error(err))
		return nil
	}

	return client
}

// stopPushTransport hace el Disconnect MQTT acotado en el shutdown (FR-05).
// Es seguro con client nil (push deshabilitado o nunca iniciado).
func stopPushTransport(ctx context.Context, client mqtt.Client, log *zap.Logger) {
	if client == nil {
		return
	}
	if err := client.Disconnect(ctx); err != nil {
		log.Warn("error disconnecting LG push MQTT", zap.Error(err))
	}
}
