package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	commandsdomain "mqtt-api-service/internal/domain/commands"
)

const (
	pendingKeyPrefix = "lg:command:pending:"
	seenKeyPrefix    = "lg:command:seen:"

	// pendingTTLMargin es el margen adicional sobre el ackTimeout al fijar
	// el TTL de Redis para una pendiente: el sweep periódico es la fuente de
	// verdad para el timeout, no la expiración de Redis (si la key
	// desaparece sola, nunca se publicaría el ACK ok:false de timeout).
	pendingTTLMargin = 60 * time.Second

	// pendingScanBatchSize es el COUNT usado en SCAN al barrer
	// lg:command:pending:* — evita traer todo el keyspace de una vez.
	pendingScanBatchSize = 100

	// PendingReasonDeviceTimeout marca una PendingConfirmation originada por
	// un LG "Device Timeout" (2211, ver lg.APIError.IsDeviceTimeout) en vez
	// del camino exitoso normal. TryConfirm/SweepTimeouts usan este campo
	// para elegir el detail correcto del ACK sintético (FASE LG-CMD-2D).
	PendingReasonDeviceTimeout = "device_timeout"
)

// PendingConfirmation es lo que se guarda en
// lg:command:pending:<imei> mientras se espera que el estado LG confirme
// (o no) un comando ya enviado (SENT).
type PendingConfirmation struct {
	CommandID  string        `json:"commandId"`
	IMEI       string        `json:"imei"`
	CommandKey string        `json:"commandKey"`
	Expected   ExpectedState `json:"expected"`
	SentAt     time.Time     `json:"sentAt"`
	ExpiresAt  time.Time     `json:"expiresAt"`
	Source     string        `json:"source"`
	// Reason es "" para el camino exitoso normal, o
	// PendingReasonDeviceTimeout cuando esta pendiente se originó porque LG
	// respondió 2211 (Device Timeout) en vez de OK.
	Reason string `json:"reason,omitempty"`
}

// ConfirmationManager administra la idempotencia por commandId
// (lg:command:seen:<commandId>) y la confirmación por estado
// (lg:command:pending:<imei>) del bridge de comandos LG.
type ConfirmationManager struct {
	redis           *redis.Client
	ackPublisher    *AckPublisher
	statusPublisher StatusPublisher
	log             *zap.Logger
	ackTimeout      time.Duration

	// debugStateLogs habilita el log "LG command confirmation check" en
	// TryConfirm (FASE LG-CMD-2E) — expected vs actual por comando
	// pendiente, solo cuando SÍ hay una pendiente (para no generar ruido en
	// cada poll/push sin comandos en curso).
	debugStateLogs bool
}

func NewConfirmationManager(redisClient *redis.Client, ackPublisher *AckPublisher, statusPublisher StatusPublisher, log *zap.Logger, ackTimeout time.Duration, debugStateLogs bool) *ConfirmationManager {
	return &ConfirmationManager{
		redis:           redisClient,
		ackPublisher:    ackPublisher,
		statusPublisher: statusPublisher,
		log:             log,
		ackTimeout:      ackTimeout,
		debugStateLogs:  debugStateLogs,
	}
}

func pendingKey(imei string) string   { return pendingKeyPrefix + imei }
func seenKey(commandID string) string { return seenKeyPrefix + commandID }

// MarkSeenIfNew implementa la idempotencia por commandId: la primera vez
// que se ve un commandId, lo marca y devuelve alreadySeen=false; llamadas
// subsecuentes con el mismo commandId (ej. redelivery de Kafka) devuelven
// alreadySeen=true sin volver a marcarlo.
func (m *ConfirmationManager) MarkSeenIfNew(ctx context.Context, commandID string, ttl time.Duration) (alreadySeen bool, err error) {
	wasSet, err := m.redis.SetNX(ctx, seenKey(commandID), "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency check failed for commandId %s: %w", commandID, err)
	}
	return !wasSet, nil
}

// SavePending guarda la confirmación pendiente para pending.IMEI. Si ya
// existía una pendiente activa para el mismo dispositivo con un commandId
// distinto, primero publica un ACK sintético ok:false
// (detail=superseded_by_new_command) para la anterior — LG no soporta
// comandos concurrentes reales, así que la más nueva siempre gana.
func (m *ConfirmationManager) SavePending(ctx context.Context, pending PendingConfirmation) error {
	existing, err := m.getPending(ctx, pending.IMEI)
	if err != nil {
		m.log.Warn("failed to read existing pending confirmation before save",
			zap.String("imei", pending.IMEI), zap.Error(err))
	}

	if existing != nil && existing.CommandID != pending.CommandID {
		if ackErr := m.ackPublisher.PublishSyntheticAck(ctx, existing.IMEI, SyntheticAck{
			CommandID: existing.CommandID,
			Command:   existing.CommandKey,
			OK:        false,
			Detail:    AckDetailSupersededByNewCommand,
		}); ackErr != nil {
			m.log.Warn("failed to publish superseded ack",
				zap.String("commandId", existing.CommandID), zap.Error(ackErr))
		}
	}

	data, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("marshal pending confirmation for %s: %w", pending.IMEI, err)
	}

	ttl := m.ackTimeout + pendingTTLMargin
	if err := m.redis.Set(ctx, pendingKey(pending.IMEI), data, ttl).Err(); err != nil {
		return fmt.Errorf("save pending confirmation for %s: %w", pending.IMEI, err)
	}

	return nil
}

func (m *ConfirmationManager) getPending(ctx context.Context, imei string) (*PendingConfirmation, error) {
	data, err := m.redis.Get(ctx, pendingKey(imei)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("read pending confirmation for %s: %w", imei, err)
	}

	var pending PendingConfirmation
	if err := json.Unmarshal(data, &pending); err != nil {
		return nil, fmt.Errorf("parse pending confirmation for %s: %w", imei, err)
	}

	return &pending, nil
}

// DeleteIfPending elimina la pendiente de imei solo si todavía pertenece a
// commandID (FASE LG-CMD-2G) — usado cuando el dispatcher registró la
// pendiente de forma optimista antes de ejecutar el comando LG (para no
// perder una confirmación que llegue como efecto del propio comando) y el
// comando termina fallando con un error definitivo: la pendiente ya no
// aplica y se elimina para no dejarla huérfana esperando una confirmación
// que nunca llegará. El chequeo de CommandID evita borrar una pendiente
// distinta que pudiera haber superseded a esta mientras tanto.
func (m *ConfirmationManager) DeleteIfPending(ctx context.Context, imei, commandID string) {
	pending, err := m.getPending(ctx, imei)
	if err != nil || pending == nil || pending.CommandID != commandID {
		return
	}
	m.deletePending(ctx, imei)
}

func (m *ConfirmationManager) deletePending(ctx context.Context, imei string) {
	if err := m.redis.Del(ctx, pendingKey(imei)).Err(); err != nil {
		m.log.Warn("failed to delete pending confirmation", zap.String("imei", imei), zap.Error(err))
	}
}

// TryConfirm compara el estado LG recién obtenido contra la confirmación
// pendiente de imei (si existe). Si coincide, publica el ACK sintético
// ok:true y borra la pendiente — con detail=confirmed_by_state para el
// camino exitoso normal, o detail=confirmed_after_device_timeout si esta
// pendiente se originó por un LG Device Timeout (2211, FASE LG-CMD-2D). No
// hace nada si no hay pendiente o si el estado todavía no coincide (queda
// esperando al siguiente poll/push o al timeout).
func (m *ConfirmationManager) TryConfirm(ctx context.Context, imei string, current CurrentState) {
	pending, err := m.getPending(ctx, imei)
	if err != nil {
		m.log.Warn("failed to read pending confirmation", zap.String("imei", imei), zap.Error(err))
		return
	}
	if pending == nil {
		return
	}

	matched := matchesExpected(pending.Expected, current)
	m.logConfirmationCheckIfEnabled(*pending, current, matched)

	if !matched {
		return
	}

	detail := AckDetailConfirmedByState
	if pending.Reason == PendingReasonDeviceTimeout {
		detail = AckDetailConfirmedAfterDeviceTimeout
	}

	if err := m.ackPublisher.PublishSyntheticAck(ctx, imei, SyntheticAck{
		CommandID: pending.CommandID,
		Command:   pending.CommandKey,
		OK:        true,
		Value:     pending.Expected.Value,
		Detail:    detail,
	}); err != nil {
		m.log.Warn("failed to publish confirmation ack",
			zap.String("commandId", pending.CommandID), zap.Error(err))
		return
	}

	m.deletePending(ctx, imei)
}

// logConfirmationCheckIfEnabled loguea expected vs actual para una
// confirmación pendiente (FASE LG-CMD-2E), solo si LG_DEBUG_STATE_LOGS=true
// — y solo cuando hay una pendiente real para evitar ruido en cada
// poll/push sin comandos en curso (el caso pending==nil ya corta antes de
// llegar aquí). No decide nada: matched ya viene calculado por el llamador,
// para no evaluar matchesExpected dos veces.
func (m *ConfirmationManager) logConfirmationCheckIfEnabled(pending PendingConfirmation, current CurrentState, matched bool) {
	if !m.debugStateLogs {
		return
	}

	m.log.Debug("LG command confirmation check",
		zap.String("commandId", pending.CommandID),
		zap.String("imei", pending.IMEI),
		zap.String("commandKey", pending.CommandKey),
		zap.Any("expected", map[string]any{"path": pending.Expected.Path, "value": pending.Expected.Value}),
		zap.Any("actual", extractActualByPath(pending.Expected.Path, current)),
		zap.Bool("matched", matched),
	)
}

// SweepTimeouts recorre lg:command:pending:* y publica un ACK sintético
// ok:false para toda pendiente cuyo ExpiresAt ya pasó: detail=ack_timeout
// para el caso normal, o detail=device_timeout_unconfirmed (más un
// device.command.publish_failed adicional, ver publishDeviceTimeoutFailed)
// si la pendiente se originó por un LG Device Timeout (2211, FASE
// LG-CMD-2D) que nunca llegó a confirmarse por estado. No depende de la
// expiración TTL de Redis: si esta rutina no corriera, la key desaparecería
// sola sin que nadie publique el timeout.
func (m *ConfirmationManager) SweepTimeouts(ctx context.Context) {
	iter := m.redis.Scan(ctx, 0, pendingKeyPrefix+"*", pendingScanBatchSize).Iterator()
	now := time.Now().UTC()

	for iter.Next(ctx) {
		key := iter.Val()

		data, err := m.redis.Get(ctx, key).Bytes()
		if err != nil {
			if err != redis.Nil {
				m.log.Warn("sweep: failed to read pending confirmation", zap.String("key", key), zap.Error(err))
			}
			continue
		}

		var pending PendingConfirmation
		if err := json.Unmarshal(data, &pending); err != nil {
			m.log.Warn("sweep: failed to parse pending confirmation", zap.String("key", key), zap.Error(err))
			continue
		}

		if now.Before(pending.ExpiresAt) {
			continue
		}

		detail := AckDetailAckTimeout
		if pending.Reason == PendingReasonDeviceTimeout {
			detail = AckDetailDeviceTimeoutUnconfirmed
			m.publishDeviceTimeoutFailed(ctx, pending)
		}

		if err := m.ackPublisher.PublishSyntheticAck(ctx, pending.IMEI, SyntheticAck{
			CommandID: pending.CommandID,
			Command:   pending.CommandKey,
			OK:        false,
			Detail:    detail,
		}); err != nil {
			m.log.Warn("sweep: failed to publish timeout ack",
				zap.String("commandId", pending.CommandID), zap.Error(err))
			continue
		}

		m.deletePending(ctx, pending.IMEI)
		m.log.Info("command ack timeout",
			zap.String("commandId", pending.CommandID),
			zap.String("imei", pending.IMEI),
			zap.String("commandKey", pending.CommandKey),
			zap.String("detail", detail),
		)
	}

	if err := iter.Err(); err != nil {
		m.log.Warn("sweep: scan error", zap.Error(err))
	}
}

// publishDeviceTimeoutFailed publica device.command.publish_failed para una
// pendiente que expiró sin confirmarse por estado y que se originó por un LG
// Device Timeout (2211) — a diferencia de un ack_timeout normal, aquí sí
// corresponde marcar la entrega como fallida ante Kafka, porque el intento
// original ya había quedado en un estado ambiguo sin garantía de entrega.
func (m *ConfirmationManager) publishDeviceTimeoutFailed(ctx context.Context, pending PendingConfirmation) {
	if m.statusPublisher == nil {
		return
	}

	mqttTopic := fmt.Sprintf(commandTopicFormat, pending.IMEI)

	if err := m.statusPublisher.PublishFailed(ctx, commandsdomain.CommandPublishFailedEvent{
		CommandID:    pending.CommandID,
		IMEI:         pending.IMEI,
		MQTTTopic:    mqttTopic,
		FailedAt:     time.Now().UTC().Format(time.RFC3339),
		Source:       sourceName,
		ErrorMessage: AckDetailDeviceTimeoutUnconfirmed,
	}); err != nil {
		m.log.Warn("sweep: failed to publish device.command.publish_failed for unconfirmed device timeout",
			zap.String("commandId", pending.CommandID), zap.Error(err))
	}
}

// StartSweep arranca SweepTimeouts en un ticker periódico, hasta que ctx se
// cancele.
func (m *ConfirmationManager) StartSweep(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		m.log.Info("command ack sweep started", zap.Duration("interval", interval))

		for {
			select {
			case <-ctx.Done():
				m.log.Info("command ack sweep stopped")
				return
			case <-ticker.C:
				m.SweepTimeouts(ctx)
			}
		}
	}()
}
