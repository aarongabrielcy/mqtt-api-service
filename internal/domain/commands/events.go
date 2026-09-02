package commands

import "encoding/json"

// CommandRoute values (COMMAND-ROUTING-CONTRACT-1, Option C): the logical
// command-execution route / ownership discriminator carried top-level on
// device.command.requested. Not a device type, manufacturer, firmware
// family, transport, or microservice name. VendorCloud is the only route
// mqtt-api-service may own/execute.
const (
	CommandRouteDirectDevice = "DIRECT_DEVICE"
	CommandRouteVendorCloud  = "VENDOR_CLOUD"
)

// DeviceCommandEvent es la forma del evento Kafka device.command.requested
// tal como lo publica tracking-platform (ver libs/contracts de ese repo).
// Payload se mantiene como JSON crudo porque su forma varía según el origen
// (manual vs. automation) y según el commandKey resuelto.
type DeviceCommandEvent struct {
	CommandID         string          `json:"commandId"`
	CommandRoute      string          `json:"commandRoute,omitempty"`
	ClientID          string          `json:"clientId"`
	AssetID           string          `json:"assetId,omitempty"`
	DeviceID          string          `json:"deviceId,omitempty"`
	IMEI              string          `json:"imei"`
	Source            string          `json:"source"`
	RequestedByUserID string          `json:"requestedByUserId,omitempty"`
	CommandCode       int             `json:"commandCode"`
	CommandType       string          `json:"commandType"`
	MQTTTopic         string          `json:"mqttTopic"`
	Payload           json.RawMessage `json:"payload"`
	RequestedAt       string          `json:"requestedAt"`
	// CommandKey es una variante top-level no usada hoy por tracking-platform
	// (que solo envía commandKey dentro de metadata), soportada aquí solo
	// como compatibilidad futura.
	CommandKey string                 `json:"commandKey,omitempty"`
	Metadata   *DeviceCommandMetadata `json:"metadata,omitempty"`
}

// DeviceCommandMetadata replica libs/contracts' metadata.commandKey, el
// único lugar donde tracking-platform ya adjunta el commandKey hoy
// (poblado por automation-service, ausente en el path manual).
type DeviceCommandMetadata struct {
	CommandKey string `json:"commandKey,omitempty"`
}

// CommandSentEvent es el payload publicado a device.command.sent.
// Mantiene mqttTopic por compatibilidad con el contrato actual, aunque LG
// no use MQTT para entregar el comando (ver pseudo-topic devices/<imei>/cmd).
type CommandSentEvent struct {
	CommandID string `json:"commandId"`
	IMEI      string `json:"imei"`
	MQTTTopic string `json:"mqttTopic"`
	SentAt    string `json:"sentAt"`
	Source    string `json:"source"`
}

// CommandPublishFailedEvent es el payload publicado a
// device.command.publish_failed.
type CommandPublishFailedEvent struct {
	CommandID    string `json:"commandId"`
	IMEI         string `json:"imei"`
	MQTTTopic    string `json:"mqttTopic"`
	FailedAt     string `json:"failedAt"`
	Source       string `json:"source"`
	ErrorMessage string `json:"errorMessage"`
}
