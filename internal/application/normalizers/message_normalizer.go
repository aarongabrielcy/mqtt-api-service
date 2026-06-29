// internal/application/normalizers/message_normalizer.go
package normalizers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"mqtt-api-service/internal/adapters/parser"

	"go.uber.org/zap"
)

type LGMessageNormalizer struct {
	log *zap.Logger
}

func NewLGMessageNormalizer(log *zap.Logger) *LGMessageNormalizer {
	return &LGMessageNormalizer{
		log: log,
	}
}

// Normalize convierte un LGMessage → JSON normalizado para tracking-platform
// IMPORTANTE: Esta función es ESPECULATIVA hasta validar payloads LG reales
func (n *LGMessageNormalizer) Normalize(
	ctx context.Context,
	deviceID string,
	eventType string,
	lgMsg *parser.LGMessage,
) ([]byte, error) {

	// 1. Crear estructura normalizada
	normalized := map[string]interface{}{
		"device_id":    deviceID,
		"brand":        "LG",
		"event_type":   eventType,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
		"raw_message":  lgMsg,
	}

	// 2. Extraer y mapear datos según tipo de evento
	if lgMsg.Data != nil {
		dataMap, err := n.extractData(lgMsg.Data)
		if err == nil {
			normalized["data"] = dataMap
		}
	}

	// 3. Agregar metadata
	normalized["metadata"] = map[string]interface{}{
		"version": "1.0",
		"source":  "mqtt",
		"brand":   "LG",
	}

	// 4. Serializar a JSON
	jsonBytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("error serializing normalized event: %w", err)
	}

	n.log.Debug("Mensaje normalizado",
		zap.String("deviceId", deviceID),
		zap.String("eventType", eventType),
		zap.Int("payload_len", len(jsonBytes)),
	)

	return jsonBytes, nil
}

// extractData extrae y mapea campos de datos LG
func (n *LGMessageNormalizer) extractData(data interface{}) (map[string]interface{}, error) {
	normalized := make(map[string]interface{})

	// Si es JSON, convertir a map
	switch v := data.(type) {
	case map[string]interface{}:
		// Copiar todos los campos
		for k, val := range v {
			normalized[k] = val
		}

		// Mapear campos específicos LG
		n.mapCommonFields(normalized)

	case string:
		// Si es string JSON, parsear
		var dataMap map[string]interface{}
		if err := json.Unmarshal([]byte(v), &dataMap); err == nil {
			for k, val := range dataMap {
				normalized[k] = val
			}
			n.mapCommonFields(normalized)
		}
	}

	return normalized, nil
}

// mapCommonFields mapea campos comunes de LG
// Ej: "tempSetting" → "temperature"
func (n *LGMessageNormalizer) mapCommonFields(data map[string]interface{}) {
	// Mapeos comunes LG
	mappings := map[string]string{
		"tempSetting":        "temperature",
		"temp":               "temperature",
		"statusCode":         "device_status",
		"status":             "device_status",
		"powerUsage":         "power_consumption",
		"power":              "power_consumption",
		"humidity":           "humidity_level",
	}

	for lgField, standardField := range mappings {
		if val, exists := data[lgField]; exists {
			data[standardField] = val
			delete(data, lgField)
		}
	}

	// Mapear valores booleanos/estados
	if status, ok := data["device_status"].(string); ok {
		data["device_status"] = normalizeStatus(status)
	}
}

func normalizeStatus(status string) string {
	switch status {
	case "On", "ON", "on":
		return "on"
	case "Off", "OFF", "off":
		return "off"
	case "StandBy", "STANDBY", "standby":
		return "standby"
	case "Error", "ERROR":
		return "error"
	default:
		return status
	}
}

// ============================================================
// Event Classifier
// ============================================================

type EventClassifier struct {
	log *zap.Logger
}

func NewEventClassifier(log *zap.Logger) *EventClassifier {
	return &EventClassifier{
		log: log,
	}
}

// Classify determina el tipo de evento basado en campos de datos
func (c *EventClassifier) Classify(data map[string]interface{}) string {
	// Si tiene campo "status", es estado
	if _, hasStatus := data["status"]; hasStatus {
		return "state"
	}

	// Si tiene campos de métrica/potencia, es telemetría
	metricsFields := []string{"power", "temperature", "humidity", "energy", "voltage"}
	for _, field := range metricsFields {
		if _, exists := data[field]; exists {
			return "telemetry"
		}
	}

	// Si tiene campo de error, es alerta
	if _, hasError := data["error"]; hasError {
		return "alert"
	}

	// Si tiene commandId/ackId, es confirmación de comando
	if _, hasCmdAck := data["commandId"]; hasCmdAck {
		return "command_ack"
	}

	return "unknown"
}
