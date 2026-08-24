package commands

import (
	"strings"

	commandsdomain "mqtt-api-service/internal/domain/commands"
)

// lgCommandTypePrefix es el prefijo de commandType que usan las
// DeviceModelCommandCapability de LG (ej. "LG_POWER") — ver
// device-commands.service.ts en tracking-platform. lgCommandCodeRangeStart/
// End acotan el bloque de commandCode reservado para LG (201-206 hoy,
// pensado para crecer sin tocar este archivo).
const (
	lgCommandKeyPrefix      = "lg."
	lgCommandTypePrefix     = "LG_"
	lgCommandCodeRangeStart = 200
	lgCommandCodeRangeEnd   = 299
)

// Command keys oficiales LG. No confundir con commandType/commandCode del
// contrato Kafka: commandKey es el identificador estable que
// DeviceModelCommandCapability usará una vez configurado en
// tracking-platform (ver FASE P1, sección 7/8).
const (
	CommandKeyPower       = "lg.power"
	CommandKeyTemperature = "lg.temperature.set"
	CommandKeyMode        = "lg.mode.set"
	CommandKeyAirflow     = "lg.airflow.set"
	CommandKeyOscillation = "lg.oscillation"
	CommandKeyPowerSave   = "lg.powersave"
)

var validCommandKeys = map[string]bool{
	CommandKeyPower:       true,
	CommandKeyTemperature: true,
	CommandKeyMode:        true,
	CommandKeyAirflow:     true,
	CommandKeyOscillation: true,
	CommandKeyPowerSave:   true,
}

// commandCodeToKey son los códigos numéricos de fallback interno para LG
// (201-206). Todavía pueden no estar habilitados en la whitelist del DTO de
// comandos manuales de tracking-platform (@IsIn([101..106])); se soportan
// aquí para poder probar el bridge por Kafka directamente sin depender de
// ese endpoint.
var commandCodeToKey = map[int]string{
	201: CommandKeyPower,
	202: CommandKeyTemperature,
	203: CommandKeyMode,
	204: CommandKeyAirflow,
	205: CommandKeyOscillation,
	206: CommandKeyPowerSave,
}

// ResolveCommandKey determina el commandKey LG a ejecutar para un evento
// device.command.requested, en este orden:
//  1. event.metadata.commandKey (lo único que automation-service ya puebla)
//  2. event.commandKey a nivel raíz (variante futura, no usada hoy)
//  3. event.commandType, si ya coincide textualmente con un commandKey válido
//  4. fallback por commandCode (201-206)
func ResolveCommandKey(event commandsdomain.DeviceCommandEvent) (string, bool) {
	if event.Metadata != nil && validCommandKeys[event.Metadata.CommandKey] {
		return event.Metadata.CommandKey, true
	}

	if validCommandKeys[event.CommandKey] {
		return event.CommandKey, true
	}

	if validCommandKeys[event.CommandType] {
		return event.CommandType, true
	}

	if key, ok := commandCodeToKey[event.CommandCode]; ok {
		return key, true
	}

	return "", false
}

// LooksLikeLGCommand distingue "este evento no trae ningún indicio de estar
// dirigido a LG" (ej. un comando ESP32 legacy 101-106 sin commandKey/metadata
// — debe ignorarse en silencio, no es responsabilidad de este servicio) de
// "trae un indicio de LG pero no se pudo resolver" (ej. metadata.commandKey
// mal escrito, o un commandCode LG fuera de los 6 soportados — sí amerita
// publicar failed + ACK, para no ocultar un error real).
//
// Confirmado por evidencia en vivo (FASE LG-CMD-E2E-DIAG): antes de esto,
// CUALQUIER evento no resoluble (incluyendo comandos ESP32 legítimos, que
// llegan al mismo topic device.command.requested y ya son manejados por el
// consumer group de mqtt-adapter-service) se trataba como fallo, publicando
// device.command.publish_failed/ACK ok:false — que compite con el
// device.command.sent legítimo de mqtt-adapter-service para el mismo
// commandId y puede volcar el estado a FAILED en tracking-platform.
func LooksLikeLGCommand(event commandsdomain.DeviceCommandEvent) bool {
	if event.Metadata != nil && strings.HasPrefix(event.Metadata.CommandKey, lgCommandKeyPrefix) {
		return true
	}
	if strings.HasPrefix(event.CommandKey, lgCommandKeyPrefix) {
		return true
	}
	if strings.HasPrefix(event.CommandType, lgCommandTypePrefix) {
		return true
	}
	if event.CommandCode >= lgCommandCodeRangeStart && event.CommandCode <= lgCommandCodeRangeEnd {
		return true
	}
	return false
}
