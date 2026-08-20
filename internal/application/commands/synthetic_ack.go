package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"mqtt-api-service/internal/domain/interfaces"

	"go.uber.org/zap"
)

// Detalles posibles del campo "detail" de un SyntheticAck. No son un enum
// cerrado del contrato de la plataforma (DeviceCommandAck.detail es un
// string libre), son la convención interna usada por este servicio.
const (
	AckDetailConfirmedByState       = "confirmed_by_state"
	AckDetailAckTimeout             = "ack_timeout"
	AckDetailDeviceDisconnected     = "device_disconnected"
	AckDetailLGAPIError             = "lg_api_error"
	AckDetailUnsupportedCommand     = "unsupported_command"
	AckDetailInvalidPayload         = "invalid_payload"
	AckDetailSupersededByNewCommand = "superseded_by_new_command"

	// AckDetailConfirmedAfterDeviceTimeout / AckDetailDeviceTimeoutUnconfirmed
	// (FASE LG-CMD-2D) son los desenlaces de una pendiente originada por un
	// LG "Device Timeout" (2211, ver lg.APIError.IsDeviceTimeout): si el
	// estado esperado se confirma antes del timeout, ok:true con este
	// detail; si vence el timeout sin confirmar, ok:false con este otro
	// (además de un device.command.publish_failed, a diferencia de un
	// ack_timeout normal).
	AckDetailConfirmedAfterDeviceTimeout = "confirmed_after_device_timeout"
	AckDetailDeviceTimeoutUnconfirmed    = "device_timeout_unconfirmed"

	// DetailDeviceTimeoutPendingConfirmation no se publica como ACK (en ese
	// momento no se publica ningún ACK) — es el valor usado únicamente en el
	// log estructurado de dispatcher.go cuando LG responde 2211 y el comando
	// queda a la espera de confirmación por estado.
	DetailDeviceTimeoutPendingConfirmation = "device_timeout_pending_confirmation"

	// AckDetailPreconditionFailedPowerOff (FASE LG-CMD-2I) se usa cuando un
	// comando LG se rechaza antes de llamar a la LG API porque requiere el
	// A/C encendido y el último estado conocido indica power=false.
	// Evidencia real: LG no aplica (o no reporta) windDirection.rotateUpDown
	// mientras airConOperationMode=POWER_OFF, así que enviar
	// lg.oscillation=true en ese estado nunca confirma y termina en
	// ack_timeout — se trata como precondición de comando, no como fallo de
	// parser/polling.
	AckDetailPreconditionFailedPowerOff = "precondition_failed_power_off"
)

// SyntheticAck tiene la misma forma que DeviceCommandAck (el contrato a
// nivel dispositivo de tracking-platform, libs/contracts/src/device-commands/
// device-command-ack.interface.ts). Se sintetiza aquí porque LG no expone
// un ack nativo por MQTT/HTTP: se construye a partir de SENT + confirmación
// por estado o timeout.
type SyntheticAck struct {
	CommandID string `json:"commandId"`
	OK        bool   `json:"ok"`
	Command   string `json:"command,omitempty"`
	Value     any    `json:"value,omitempty"`
	Detail    string `json:"detail,omitempty"`
	DeviceID  string `json:"device_id,omitempty"`
	Dt        int64  `json:"dt"`
}

// AckPublisher envía el SyntheticAck a tracking-platform reutilizando el
// mismo gRPC IngestRaw que ya usa la telemetría LG — no crea ningún topic,
// proto ni endpoint nuevo. El topic sintético devices/<imei>/ack es el
// mismo que ya usa mqtt-adapter-service/ESP32 para ACKs reales; ingestion-service
// ya lo normaliza a device.ack.received sin cambios de este lado.
type AckPublisher struct {
	trackingClient interfaces.TrackingClient
	log            *zap.Logger
}

func NewAckPublisher(trackingClient interfaces.TrackingClient, log *zap.Logger) *AckPublisher {
	return &AckPublisher{
		trackingClient: trackingClient,
		log:            log,
	}
}

// PublishSyntheticAck envía el ack sintético para imei. DeviceID/Dt se
// completan automáticamente si no vienen seteados en ack.
//
// Verificado por inspección directa (solo lectura) de tracking-platform en
// FASE LG-CMD-2B: devices/<imei>/ack lo clasifica extractTopicKind()
// (ingestion-service/src/normalization/normalization.service.ts, split del
// topic por "/", kind=parts[2]="ack") y el mismo imei se resuelve desde
// parts[1] en tracking.controller.ts (extractTopicParts) — exactamente el
// shape que este método construye, sin cambios necesarios del lado de
// tracking-platform.
func (p *AckPublisher) PublishSyntheticAck(ctx context.Context, imei string, ack SyntheticAck) error {
	if ack.CommandID == "" {
		return fmt.Errorf("synthetic ack for %s: commandId must not be empty", imei)
	}

	ack.DeviceID = imei
	if ack.Dt == 0 {
		ack.Dt = time.Now().UTC().Unix()
	}

	payload, err := json.Marshal(ack)
	if err != nil {
		return fmt.Errorf("marshal synthetic ack for %s: %w", imei, err)
	}

	if p.trackingClient == nil {
		return nil
	}

	topic := fmt.Sprintf("devices/%s/ack", imei)

	if err := p.trackingClient.IngestRaw(ctx, interfaces.IngestRawInput{
		Topic:      topic,
		Payload:    payload,
		ReceivedAt: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("publish synthetic ack for %s: %w", imei, err)
	}

	p.log.Info("synthetic ack published",
		zap.String("imei", imei),
		zap.String("commandId", ack.CommandID),
		zap.String("command", ack.Command),
		zap.Bool("ok", ack.OK),
		zap.String("detail", ack.Detail),
	)

	return nil
}
