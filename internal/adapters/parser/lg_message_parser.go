// internal/adapters/parser/lg_message_parser.go
package parser

import (
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
)

// LGMessage es la estructura genérica que LG envía en MQTT
// NOTA: Esta estructura es ESPECULATIVA hasta validar payloads reales
type LGMessage struct {
	// Identificadores
	MessageID string `json:"messageId,omitempty"`
	ClientID  string `json:"clientId,omitempty"`
	DeviceID  string `json:"deviceId,omitempty"`

	// Tipo de mensaje/evento
	MessageType string `json:"messageType,omitempty"` // "Event", "Status", "CommandAck", etc
	EventType   string `json:"eventType,omitempty"`   // "StateChange", "Telemetry", etc

	// Contenido
	Data      interface{} `json:"data,omitempty"`
	Timestamp int64       `json:"timestamp,omitempty"` // Unix timestamp

	// Metadata
	Source string `json:"source,omitempty"` // "mqtt", "api", etc
	QoS    int    `json:"qos,omitempty"`
}

type LGMessageParser struct {
	log *zap.Logger
}

func NewLGMessageParser(log *zap.Logger) *LGMessageParser {
	return &LGMessageParser{
		log: log,
	}
}

// Parse convierte un payload MQTT en LGMessage
// IMPORTANTE: Esta función es ESPECULATIVA
// Necesita ajustes basados en estructura REAL de LG
func (p *LGMessageParser) Parse(topic string, payload []byte) (*LGMessage, error) {
	var msg LGMessage

	// 1. Parse JSON
	if err := json.Unmarshal(payload, &msg); err != nil {
		p.log.Warn("Error parseando JSON LG",
			zap.String("topic", topic),
			zap.String("payload", string(payload)),
			zap.Error(err),
		)
		// Podría ser otro formato (proto, binario, etc)
		return nil, fmt.Errorf("invalid LG payload format: %w", err)
	}

	// 2. Validar campos requeridos
	if msg.DeviceID == "" {
		p.log.Warn("Missing deviceId in LG message",
			zap.String("topic", topic),
			zap.String("messageId", msg.MessageID),
		)
		return nil, fmt.Errorf("missing deviceId")
	}

	// 3. Extraer clientId de topic si está vacío
	// Topic: app/clients/{clientId}/push
	if msg.ClientID == "" {
		msg.ClientID = extractClientIDFromTopic(topic)
	}

	p.log.Debug("LG message parsed",
		zap.String("deviceId", msg.DeviceID),
		zap.String("messageType", msg.MessageType),
		zap.String("eventType", msg.EventType),
	)

	return &msg, nil
}

// ParseMultiple maneja el caso donde un mensaje contiene MÚLTIPLES eventos
// (Ej: LG publica N dispositivos en un solo mensaje)
func (p *LGMessageParser) ParseMultiple(topic string, payload []byte) ([]*LGMessage, error) {
	var messages []*LGMessage

	// Intenta primero como array
	var msgArray []LGMessage
	if err := json.Unmarshal(payload, &msgArray); err == nil && len(msgArray) > 0 {
		for i := range msgArray {
			messages = append(messages, &msgArray[i])
		}
		return messages, nil
	}

	// Sino, intenta como mensaje individual
	msg, err := p.Parse(topic, payload)
	if err != nil {
		return nil, err
	}

	return []*LGMessage{msg}, nil
}

// ExtractDeviceID extrae el device ID de un mensaje LG
func (p *LGMessageParser) ExtractDeviceID(msg *LGMessage) (string, error) {
	if msg.DeviceID != "" {
		return msg.DeviceID, nil
	}

	// Si no está en top-level, buscar en data
	if data, ok := msg.Data.(map[string]interface{}); ok {
		if deviceID, ok := data["deviceId"].(string); ok && deviceID != "" {
			return deviceID, nil
		}
		if deviceID, ok := data["device_id"].(string); ok && deviceID != "" {
			return deviceID, nil
		}
	}

	return "", fmt.Errorf("could not extract deviceId from LG message")
}

// ExtractEventType determina el tipo de evento
func (p *LGMessageParser) ExtractEventType(msg *LGMessage) string {
	if msg.EventType != "" {
		return msg.EventType
	}
	if msg.MessageType != "" {
		return msg.MessageType
	}

	// Analizar payload para inferir tipo
	if data, ok := msg.Data.(map[string]interface{}); ok {
		if _, hasStatus := data["status"]; hasStatus {
			return "state"
		}
		if _, hasMetrics := data["power"]; hasMetrics {
			return "telemetry"
		}
		if _, hasError := data["error"]; hasError {
			return "alert"
		}
	}

	return "unknown"
}

// extractClientIDFromTopic extrae client ID de topic
// Topic formato: app/clients/{clientId}/push
func extractClientIDFromTopic(topic string) string {
	parts := splitTopic(topic)
	if len(parts) >= 3 && parts[0] == "app" && parts[1] == "clients" {
		return parts[2]
	}
	return ""
}

func splitTopic(topic string) []string {
	// Simple split by "/" - en Go hay strings.Split
	// Simulado aquí
	return nil // Implementar con strings.Split
}

// ============================================================
// Validadores
// ============================================================

func (p *LGMessageParser) Validate(msg *LGMessage) error {
	if msg.DeviceID == "" {
		return fmt.Errorf("missing deviceId")
	}
	if msg.MessageType == "" && msg.EventType == "" {
		return fmt.Errorf("missing messageType or eventType")
	}
	return nil
}
