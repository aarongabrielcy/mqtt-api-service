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

	// RefreshDeviceState (FASE LG-CMD-2H) hace una lectura puntual de
	// estado LG (GET /devices/:id/state) reutilizando el mismo pipeline del
	// polling periódico (parse, debug log, snapshot, TryConfirm, telemetry
	// por gRPC) — implementado por LGService.RefreshDeviceState. No marca
	// ACKNOWLEDGED por sí mismo: solo le da a TryConfirm una oportunidad de
	// confirmar antes del siguiente ciclo de polling o push.
	RefreshDeviceState(ctx context.Context, deviceID string) error

	// GetLastKnownPower (FASE LG-CMD-2I) devuelve si existe un estado
	// conocido para deviceID (known) y, si existe, si el A/C estaba
	// encendido en ese estado (powerOn). Implementado por
	// LGService.GetLastKnownPower leyendo el snapshot en Redis (el mismo
	// snapshot que alimentan tanto el polling periódico como los push de
	// LG). known=false si todavía no hay ningún snapshot guardado — el
	// llamador no debe bloquear por precondición en ese caso, para no
	// producir falsos negativos sobre un dispositivo que simplemente nunca
	// se leyó.
	GetLastKnownPower(ctx context.Context, deviceID string) (known bool, powerOn bool)
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
	log              *zap.Logger
	lg               LGCommander
	confirmation     *ConfirmationManager
	ackPublisher     *AckPublisher
	statusPublisher  StatusPublisher
	parseCfg         ParseConfig
	seenTTL          time.Duration
	postRefreshDelay time.Duration
}

func NewCommandDispatcher(
	log *zap.Logger,
	lg LGCommander,
	confirmation *ConfirmationManager,
	ackPublisher *AckPublisher,
	statusPublisher StatusPublisher,
	parseCfg ParseConfig,
	seenTTL time.Duration,
	postRefreshDelay time.Duration,
) *CommandDispatcher {
	return &CommandDispatcher{
		log:              log,
		lg:               lg,
		confirmation:     confirmation,
		ackPublisher:     ackPublisher,
		statusPublisher:  statusPublisher,
		parseCfg:         parseCfg,
		seenTTL:          seenTTL,
		postRefreshDelay: postRefreshDelay,
	}
}

// postCommandRefreshTimeout acota la llamada HTTP del refresh puntual
// disparado tras un comando — independiente de postRefreshDelay (la espera
// antes de disparar), este es el timeout de la propia petición GET
// /devices/:id/state una vez lanzada.
const postCommandRefreshTimeout = 10 * time.Second

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

	// COMMAND-ROUTING-CONTRACT-1 (Option C) ownership gate, strict cutover
	// (Corrective Cycle 1 / AC-08): this bridge executes only an explicit
	// VENDOR_CLOUD route. DIRECT_DEVICE, any unknown non-empty value, and a
	// missing (empty) route are all ignored — no fallback to the
	// LooksLikeLGCommand ambiguity heuristic. The producer-first runtime
	// gate proving all current producers emit a valid commandRoute has been
	// satisfied (see Task Human Runtime Gate Evidence), so the prior
	// missing-route LooksLikeLGCommand compatibility fallback is removed.
	switch event.CommandRoute {
	case commandsdomain.CommandRouteVendorCloud:
		// explicit ownership — proceed to resolve/execute below
	default:
		d.log.Debug("ignoring command event: commandRoute not owned by mqtt-api-service",
			zap.String("commandId", event.CommandID),
			zap.String("imei", event.IMEI),
			zap.String("commandRoute", event.CommandRoute),
		)
		return nil
	}

	commandKey, ok := ResolveCommandKey(event)
	if !ok {
		// event.CommandRoute == VENDOR_CLOUD is guaranteed at this point (the
		// switch above returns for every other value), so ownership is
		// always certain here: an unresolved commandKey is always a real
		// unsupported-command failure, never silently ignored.
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

	// FASE LG-CMD-2I — precondición power ON para lg.oscillation=true.
	// Evidencia real: LG no aplica ni reporta windDirection.rotateUpDown
	// mientras el A/C está apagado (airConOperationMode=POWER_OFF); enviar
	// el comando en ese estado nunca confirma por estado y termina en
	// ack_timeout pese a que la LG API respondió 200 OK. Se corta el flujo
	// aquí mismo — antes de registrar la pendiente y antes de llamar a la
	// LG API — solo para lg.oscillation enabled=true. No aplica a
	// enabled=false ni a ningún otro comando (power/temperature/mode/
	// airflow/powersave), que no tienen esta dependencia conocida. Si no
	// hay estado conocido (known=false), no se bloquea, para no producir
	// falsos negativos sobre un dispositivo nunca leído.
	if commandKey == CommandKeyOscillation && payload.Enabled {
		if known, powerOn := d.lg.GetLastKnownPower(ctx, event.IMEI); known && !powerOn {
			d.log.Warn("LG command precondition failed",
				zap.String("commandId", event.CommandID),
				zap.String("imei", event.IMEI),
				zap.String("commandKey", commandKey),
				zap.String("precondition", "power_on_required"),
			)
			d.publishFailed(ctx, event, mqttTopic, AckDetailPreconditionFailedPowerOff)
			d.publishFailureAck(ctx, event, commandKey, AckDetailPreconditionFailedPowerOff)
			return nil
		}
	}

	// FASE LG-CMD-2G — fix de carrera: la pendiente de confirmación se
	// registra ANTES de llamar a la LG API, no después. Evidencia real: LG
	// aplica el cambio físico y el dispositivo notifica por MQTT push casi
	// inmediatamente tras la respuesta HTTP de /control — si la pendiente
	// todavía no existiera en ese momento (como ocurría antes, cuando se
	// registraba solo tras un executeLGCommand exitoso), ese push —la única
	// confirmación que de hecho llega— se pierde porque TryConfirm no
	// encuentra ninguna pendiente que comparar, y el comando termina en
	// ack_timeout pese a que el equipo sí cambió. Registrarla antes
	// garantiza que cualquier confirmación que llegue como efecto del propio
	// comando (push o el siguiente poll) ya encuentre la pendiente guardada.
	if err := d.confirmation.SavePending(ctx, d.buildPending(event, commandKey, payload, "")); err != nil {
		d.log.Warn("failed to save pending confirmation before executing command, proceeding anyway",
			zap.String("commandId", event.CommandID), zap.Error(err))
	}

	if execErr := d.executeLGCommand(ctx, event.IMEI, commandKey, payload); execErr != nil {
		var apiErr *lgapi.APIError

		// LG "Device Timeout" (2211) es ambiguo: LG Cloud no confirmó a
		// tiempo, pero el dispositivo puede haber ejecutado el cambio de
		// todas formas. En vez de failure inmediato, se trata igual que el
		// camino exitoso (SENT + pending confirmation, ya registrada arriba)
		// y se deja que polling/push confirmen o venza el timeout (FASE
		// LG-CMD-2D) — solo se actualiza el Reason de la pendiente ya
		// existente para que el eventual timeout/confirmación usen el
		// detail correcto (device_timeout_unconfirmed /
		// confirmed_after_device_timeout).
		if errors.As(execErr, &apiErr) && apiErr.IsDeviceTimeout() {
			d.log.Warn("LG command timed out at provider, waiting for state confirmation",
				zap.String("commandId", event.CommandID),
				zap.String("imei", event.IMEI),
				zap.String("commandKey", commandKey),
				zap.String("detail", DetailDeviceTimeoutPendingConfirmation),
			)
			if err := d.confirmation.SavePending(ctx, d.buildPending(event, commandKey, payload, PendingReasonDeviceTimeout)); err != nil {
				d.log.Warn("failed to mark pending confirmation as device_timeout",
					zap.String("commandId", event.CommandID), zap.Error(err))
			}
			d.publishSent(ctx, event, mqttTopic)
			d.triggerPostCommandRefresh(event, commandKey)
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
		// Error definitivo (no ambiguo): la pendiente registrada
		// optimistamente arriba ya no aplica — se elimina para no dejarla
		// huérfana esperando una confirmación que nunca llegará.
		d.confirmation.DeleteIfPending(ctx, event.IMEI, event.CommandID)
		d.publishFailed(ctx, event, mqttTopic, detail)
		d.publishFailureAck(ctx, event, commandKey, detail)
		return nil
	}

	d.publishSent(ctx, event, mqttTopic)
	d.triggerPostCommandRefresh(event, commandKey)
	return nil
}

// buildPending construye la PendingConfirmation para event/commandKey/payload
// con el Reason indicado ("" para el camino exitoso normal,
// PendingReasonDeviceTimeout para un LG Device Timeout 2211). Guardar dos
// veces la misma pendiente (CommandID igual) bajo un Reason distinto es
// seguro: ConfirmationManager.SavePending solo trata como "superseded" una
// pendiente con un CommandID *distinto* para el mismo IMEI.
func (d *CommandDispatcher) buildPending(
	event commandsdomain.DeviceCommandEvent,
	commandKey string,
	payload Payload,
	reason string,
) PendingConfirmation {
	return PendingConfirmation{
		CommandID:  event.CommandID,
		IMEI:       event.IMEI,
		CommandKey: commandKey,
		Expected:   buildExpectedState(commandKey, payload),
		SentAt:     time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(d.confirmation.ackTimeout),
		Source:     sourceName,
		Reason:     reason,
	}
}

// publishSent publica device.command.sent — la pendiente de confirmación ya
// fue registrada antes de ejecutar el comando (ver Dispatch), así que aquí
// solo queda notificar la entrega a Kafka.
func (d *CommandDispatcher) publishSent(ctx context.Context, event commandsdomain.DeviceCommandEvent, mqttTopic string) {
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
}

// triggerPostCommandRefresh (FASE LG-CMD-2H) programa una lectura puntual
// de estado LG poco después de un comando exitoso o ambiguo (2211): LG
// puede tardar un instante en reflejar el cambio físico en su propio
// estado consultable, así que se espera postRefreshDelay antes de
// refrescar. Corre en su propia goroutine con un context propio (no el del
// mensaje Kafka en curso, que puede terminar antes de que venza el delay)
// para no bloquear al consumer. Reutiliza el mismo pipeline de refresh que
// el polling periódico vía LGCommander.RefreshDeviceState — nunca marca
// ACKNOWLEDGED por sí mismo: solo le da a TryConfirm el mismo estado que el
// siguiente poll habría visto, pero antes, sin esperar a que venza el
// ackTimeout.
func (d *CommandDispatcher) triggerPostCommandRefresh(event commandsdomain.DeviceCommandEvent, commandKey string) {
	d.log.Debug("LG post-command state refresh started",
		zap.String("commandId", event.CommandID),
		zap.String("imei", event.IMEI),
		zap.String("commandKey", commandKey),
		zap.Int64("delayMs", d.postRefreshDelay.Milliseconds()),
	)

	go func() {
		if d.postRefreshDelay > 0 {
			time.Sleep(d.postRefreshDelay)
		}

		ctx, cancel := context.WithTimeout(context.Background(), postCommandRefreshTimeout)
		defer cancel()

		if err := d.lg.RefreshDeviceState(ctx, event.IMEI); err != nil {
			d.log.Warn("post-command state refresh failed",
				zap.String("commandId", event.CommandID),
				zap.String("imei", event.IMEI),
				zap.String("commandKey", commandKey),
				zap.Error(err),
			)
		}
	}()
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
