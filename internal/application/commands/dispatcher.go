package commands

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	lgapi "mqtt-api-service/internal/adapters/api/lg"
	commandsdomain "mqtt-api-service/internal/domain/commands"
)

// sourceName identifica a este servicio como origen de los eventos
// device.command.sent / device.command.publish_failed, igual que
// mqtt-adapter-service se identifica a sí mismo en los suyos.
const sourceName = "mqtt-api-service"

// commandTopicFormat construye el pseudo-topic devices/<imei>/cmd que viaja
// en mqttTopic por compatibilidad con el contrato Kafka actual, aunque LG
// no entregue el comando por MQTT (se entrega por HTTP a la LG API).
const commandTopicFormat = "devices/%s/cmd"

// LGCommander son los métodos de ejecución de comandos LG ya existentes en
// internal/application/use_case/lg (LGService). Se consume como interfaz
// para no crear una dependencia de este paquete hacia ese, y para poder
// testear el dispatcher con un fake.
type LGCommander interface {
	SetDevicePower(ctx context.Context, deviceID string, on bool) error
	SetDeviceTemperature(ctx context.Context, deviceID string, temperature float64) error
	SetOperationMode(ctx context.Context, deviceID string, mode string) error
	SetAirFlow(ctx context.Context, deviceID string, strength string) error
	SetOscillation(ctx context.Context, deviceID string, enabled bool) error
	SetPowerSave(ctx context.Context, deviceID string, enabled bool) error
}

// StatusPublisher publica los eventos Kafka de resultado de entrega
// (device.command.sent / device.command.publish_failed). Implementado por
// internal/adapters/kafka.CommandStatusPublisher.
type StatusPublisher interface {
	PublishSent(ctx context.Context, evt commandsdomain.CommandSentEvent) error
	PublishFailed(ctx context.Context, evt commandsdomain.CommandPublishFailedEvent) error
}

// CommandDispatcher consume un DeviceCommandEvent ya deserializado
// (device.command.requested), ejecuta el comando LG correspondiente y
// publica el resultado (sent/failed + confirmación pendiente o ACK
// sintético). Equivalente, en propósito, a mqtt-adapter-service's
// CommandDispatcher — pero entregando por HTTP a la LG API en vez de MQTT, y
// con confirmación real por estado en vez de "SENT == listo".
type CommandDispatcher struct {
	log             *zap.Logger
	lg              LGCommander
	confirmation    *ConfirmationManager
	ackPublisher    *AckPublisher
	statusPublisher StatusPublisher
	parseCfg        ParseConfig
	seenTTL         time.Duration
}

func NewCommandDispatcher(
	log *zap.Logger,
	lg LGCommander,
	confirmation *ConfirmationManager,
	ackPublisher *AckPublisher,
	statusPublisher StatusPublisher,
	parseCfg ParseConfig,
	seenTTL time.Duration,
) *CommandDispatcher {
	return &CommandDispatcher{
		log:             log,
		lg:              lg,
		confirmation:    confirmation,
		ackPublisher:    ackPublisher,
		statusPublisher: statusPublisher,
		parseCfg:        parseCfg,
		seenTTL:         seenTTL,
	}
}

// Dispatch procesa un evento device.command.requested. Nunca devuelve un
// error que amerite no-commit del mensaje Kafka: los fallos de negocio
// (payload inválido, comando no soportado, LG API caída) se resuelven
// publicando failed + ACK sintético ok:false, no reintentando el mismo
// mensaje. Solo devuelve error si el evento ni siquiera trae los campos
// mínimos para poder reportar un fallo (commandId/imei vacíos).
func (d *CommandDispatcher) Dispatch(ctx context.Context, event commandsdomain.DeviceCommandEvent) error {
	if event.CommandID == "" || event.IMEI == "" {
		d.log.Warn("dropping command event: missing commandId or imei",
			zap.String("commandId", event.CommandID),
			zap.String("imei", event.IMEI),
		)
		return nil
	}

	mqttTopic := fmt.Sprintf(commandTopicFormat, event.IMEI)

	commandKey, ok := ResolveCommandKey(event)
	if !ok {
		if !LooksLikeLGCommand(event) {
			// No indicio de LG en absoluto (ej. ESP32 legacy commandCode
			// 101-106 sin commandKey/metadata) — este evento no está
			// dirigido a este dispatcher. mqtt-adapter-service ya lo
			// procesa en su propio consumer group sobre el mismo topic;
			// publicar failed/ACK aquí competiría con su
			// device.command.sent legítimo para el mismo commandId (bug
			// confirmado en vivo, FASE LG-CMD-E2E-DIAG). Se ignora en
			// silencio.
			d.log.Debug("ignoring command event not addressed to LG",
				zap.String("commandId", event.CommandID),
				zap.String("imei", event.IMEI),
				zap.Int("commandCode", event.CommandCode),
			)
			return nil
		}

		d.log.Warn("unsupported command, cannot resolve commandKey",
			zap.String("commandId", event.CommandID),
			zap.String("imei", event.IMEI),
			zap.Int("commandCode", event.CommandCode),
			zap.String("commandType", event.CommandType),
		)
		d.publishFailed(ctx, event, mqttTopic, "unsupported command: cannot resolve commandKey")
		d.publishFailureAck(ctx, event, "", AckDetailUnsupportedCommand)
		return nil
	}

	alreadySeen, err := d.confirmation.MarkSeenIfNew(ctx, event.CommandID, d.seenTTL)
	if err != nil {
		d.log.Warn("idempotency check failed, proceeding anyway",
			zap.String("commandId", event.CommandID), zap.Error(err))
	} else if alreadySeen {
		d.log.Info("duplicate commandId skipped",
			zap.String("commandId", event.CommandID),
			zap.String("imei", event.IMEI),
			zap.String("commandKey", commandKey),
		)
		return nil
	}

	payload, err := ParseCommandPayload(commandKey, event.CommandCode, event.Payload, d.parseCfg)
	if err != nil {
		d.log.Warn("invalid command payload",
			zap.String("commandId", event.CommandID),
			zap.String("imei", event.IMEI),
			zap.String("commandKey", commandKey),
			zap.Error(err),
		)
		d.publishFailed(ctx, event, mqttTopic, "invalid payload: "+err.Error())
		d.publishFailureAck(ctx, event, commandKey, AckDetailInvalidPayload)
		return nil
	}

	if execErr := d.executeLGCommand(ctx, event.IMEI, commandKey, payload); execErr != nil {
		var apiErr *lgapi.APIError

		// LG "Device Timeout" (2211) es ambiguo: LG Cloud no confirmó a
		// tiempo, pero el dispositivo puede haber ejecutado el cambio de
		// todas formas. En vez de failure inmediato, se trata igual que el
		// camino exitoso (SENT + pending confirmation) y se deja que
		// polling/push confirmen o venza el timeout (FASE LG-CMD-2D).
		if errors.As(execErr, &apiErr) && apiErr.IsDeviceTimeout() {
			d.log.Warn("LG command timed out at provider, waiting for state confirmation",
				zap.String("commandId", event.CommandID),
				zap.String("imei", event.IMEI),
				zap.String("commandKey", commandKey),
				zap.String("detail", DetailDeviceTimeoutPendingConfirmation),
			)
			d.publishSentAndSavePending(ctx, event, mqttTopic, commandKey, payload, PendingReasonDeviceTimeout)
			return nil
		}

		detail := AckDetailLGAPIError
		if errors.As(execErr, &apiErr) && apiErr.IsDeviceNotConnected() {
			detail = AckDetailDeviceDisconnected
		}

		d.log.Warn("LG command execution failed",
			zap.String("commandId", event.CommandID),
			zap.String("imei", event.IMEI),
			zap.String("commandKey", commandKey),
			zap.String("detail", detail),
			zap.Error(execErr),
		)
		d.publishFailed(ctx, event, mqttTopic, detail)
		d.publishFailureAck(ctx, event, commandKey, detail)
		return nil
	}

	d.publishSentAndSavePending(ctx, event, mqttTopic, commandKey, payload, "")
	return nil
}

// publishSentAndSavePending publica device.command.sent y registra la
// confirmación pendiente por estado. Se usa tanto para el camino exitoso
// (reason="") como para un LG Device Timeout 2211 (reason=
// PendingReasonDeviceTimeout) — en ambos casos el comando ya se entregó al
// proveedor y la confirmación final se resuelve por polling/push o timeout.
func (d *CommandDispatcher) publishSentAndSavePending(
	ctx context.Context,
	event commandsdomain.DeviceCommandEvent,
	mqttTopic, commandKey string,
	payload Payload,
	reason string,
) {
	if err := d.statusPublisher.PublishSent(ctx, commandsdomain.CommandSentEvent{
		CommandID: event.CommandID,
		IMEI:      event.IMEI,
		MQTTTopic: mqttTopic,
		SentAt:    time.Now().UTC().Format(time.RFC3339),
		Source:    sourceName,
	}); err != nil {
		d.log.Warn("failed to publish device.command.sent",
			zap.String("commandId", event.CommandID), zap.Error(err))
	}

	pending := PendingConfirmation{
		CommandID:  event.CommandID,
		IMEI:       event.IMEI,
		CommandKey: commandKey,
		Expected:   buildExpectedState(commandKey, payload),
		SentAt:     time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(d.confirmation.ackTimeout),
		Source:     sourceName,
		Reason:     reason,
	}

	if err := d.confirmation.SavePending(ctx, pending); err != nil {
		d.log.Warn("failed to save pending confirmation",
			zap.String("commandId", event.CommandID), zap.Error(err))
	}
}

func (d *CommandDispatcher) executeLGCommand(ctx context.Context, imei, commandKey string, payload Payload) error {
	switch commandKey {
	case CommandKeyPower:
		return d.lg.SetDevicePower(ctx, imei, payload.Power)
	case CommandKeyTemperature:
		return d.lg.SetDeviceTemperature(ctx, imei, payload.Temperature)
	case CommandKeyMode:
		return d.lg.SetOperationMode(ctx, imei, payload.Mode)
	case CommandKeyAirflow:
		return d.lg.SetAirFlow(ctx, imei, payload.Strength)
	case CommandKeyOscillation:
		return d.lg.SetOscillation(ctx, imei, payload.Enabled)
	case CommandKeyPowerSave:
		return d.lg.SetPowerSave(ctx, imei, payload.Enabled)
	default:
		return fmt.Errorf("unsupported command key %q", commandKey)
	}
}

func (d *CommandDispatcher) publishFailed(ctx context.Context, event commandsdomain.DeviceCommandEvent, mqttTopic, errMsg string) {
	if err := d.statusPublisher.PublishFailed(ctx, commandsdomain.CommandPublishFailedEvent{
		CommandID:    event.CommandID,
		IMEI:         event.IMEI,
		MQTTTopic:    mqttTopic,
		FailedAt:     time.Now().UTC().Format(time.RFC3339),
		Source:       sourceName,
		ErrorMessage: errMsg,
	}); err != nil {
		d.log.Warn("failed to publish device.command.publish_failed",
			zap.String("commandId", event.CommandID), zap.Error(err))
	}
}

func (d *CommandDispatcher) publishFailureAck(ctx context.Context, event commandsdomain.DeviceCommandEvent, commandKey, detail string) {
	if err := d.ackPublisher.PublishSyntheticAck(ctx, event.IMEI, SyntheticAck{
		CommandID: event.CommandID,
		Command:   commandKey,
		OK:        false,
		Detail:    detail,
	}); err != nil {
		d.log.Warn("failed to publish failure ack",
			zap.String("commandId", event.CommandID), zap.Error(err))
	}
}
