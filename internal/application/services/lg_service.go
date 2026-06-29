// internal/application/services/lg_service.go
package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"mqtt-api-service/internal/adapters/api"
	"mqtt-api-service/internal/adapters/grpc"
	"mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/adapters/mqtt"
	"mqtt-api-service/internal/adapters/parser"
	"mqtt-api-service/internal/application/normalizers"
	"mqtt-api-service/internal/domain"

	"go.uber.org/zap"
)

type LGService struct {
	mqtt              mqtt.Client
	lgAPI             *api.LGAPIClient
	repository        mongo.Repository
	grpcClient        *grpc.Client
	messageParser     *parser.LGMessageParser
	messageNormalizer *normalizers.LGMessageNormalizer
	eventClassifier   *normalizers.EventClassifier
	log               *zap.Logger
}

func NewLGService(
	mqtt mqtt.Client,
	lgAPI *api.LGAPIClient,
	repository mongo.Repository,
	grpcClient *grpc.Client,
	messageParser *parser.LGMessageParser,
	messageNormalizer *normalizers.LGMessageNormalizer,
	eventClassifier *normalizers.EventClassifier,
	log *zap.Logger,
) *LGService {
	return &LGService{
		mqtt:              mqtt,
		lgAPI:             lgAPI,
		repository:        repository,
		grpcClient:        grpcClient,
		messageParser:     messageParser,
		messageNormalizer: messageNormalizer,
		eventClassifier:   eventClassifier,
		log:               log,
	}
}

// HandleLGMessage es el handler principal para todos los mensajes MQTT de LG
// Topics: app/clients/{clientId}/push, inbox, outbox
func (s *LGService) HandleLGMessage(ctx context.Context, topic string, payload []byte) error {
	s.log.Debug("Mensaje LG recibido",
		zap.String("topic", topic),
		zap.Int("payload_len", len(payload)),
	)

	// 1. Determinar tipo de topic
	topicType := s.classifyTopic(topic)

	// 2. Parsear mensaje LG (ESPECULATIVO - ajustar según payload real)
	lgMessages, err := s.messageParser.ParseMultiple(topic, payload)
	if err != nil {
		s.log.Error("Error parseando mensaje LG",
			zap.Error(err),
			zap.String("topic", topic),
		)
		// Guardar en dead-letter para análisis
		return s.saveDeadLetter(ctx, topic, payload, err)
	}

	// 3. Procesar cada mensaje
	for _, lgMsg := range lgMessages {
		if err := s.processMessage(ctx, topic, topicType, lgMsg); err != nil {
			s.log.Error("Error procesando mensaje LG",
				zap.Error(err),
				zap.String("deviceId", lgMsg.DeviceID),
			)
			continue // Continuar con siguientes mensajes
		}
	}

	return nil
}

// processMessage procesa UN mensaje LG
func (s *LGService) processMessage(
	ctx context.Context,
	topic string,
	topicType string,
	lgMsg *parser.LGMessage,
) error {
	// 1. Extraer device ID
	deviceID, err := s.messageParser.ExtractDeviceID(lgMsg)
	if err != nil {
		return fmt.Errorf("cannot extract deviceId: %w", err)
	}

	// 2. Determinar tipo de evento
	eventType := s.messageParser.ExtractEventType(lgMsg)

	// 3. Guardar raw data en MongoDB
	if err := s.saveRawMessage(ctx, deviceID, lgMsg, topic); err != nil {
		s.log.Warn("Error guardando raw message",
			zap.Error(err),
			zap.String("deviceId", deviceID),
		)
	}

	// 4. Normalizar mensaje
	normalizedEvent, err := s.messageNormalizer.Normalize(ctx, deviceID, eventType, lgMsg)
	if err != nil {
		return fmt.Errorf("normalization failed: %w", err)
	}

	// 5. Mapear topic a formato tracking-platform
	mappedTopic := s.mapTopicToTracking(deviceID, eventType)

	// 6. Enviar a tracking-platform por gRPC
	if err := s.grpcClient.IngestRaw(ctx, mappedTopic, normalizedEvent, 1); err != nil {
		s.log.Error("Error enviando a tracking-platform",
			zap.Error(err),
			zap.String("deviceId", deviceID),
		)
		return fmt.Errorf("grpc ingestion failed: %w", err)
	}

	s.log.Info("Mensaje procesado correctamente",
		zap.String("deviceId", deviceID),
		zap.String("eventType", eventType),
		zap.String("mappedTopic", mappedTopic),
	)

	return nil
}

// classifyTopic determina qué tipo de topic es
func (s *LGService) classifyTopic(topic string) string {
	// app/clients/{clientId}/push
	// app/clients/{clientId}/inbox
	// app/clients/{clientId}/outbox
	parts := strings.Split(topic, "/")
	if len(parts) >= 4 {
		return parts[3] // "push", "inbox", "outbox"
	}
	return "unknown"
}

// mapTopicToTracking convierte app/clients/xxx/push → devices/{deviceId}/status
func (s *LGService) mapTopicToTracking(deviceID string, eventType string) string {
	// tracking-platform espera: devices/{imei}/{kind}
	kind := s.mapEventTypeToKind(eventType)
	return fmt.Sprintf("devices/%s/%s", deviceID, kind)
}

// mapEventTypeToKind convierte tipos LG → kind tracking-platform
func (s *LGService) mapEventTypeToKind(eventType string) string {
	switch eventType {
	case "state", "status", "command_ack", "alert":
		return "status"
	case "telemetry", "metrics":
		return "telemetry"
	case "event":
		return "event"
	default:
		return eventType
	}
}

// saveRawMessage guarda el mensaje LG crudo en MongoDB
func (s *LGService) saveRawMessage(
	ctx context.Context,
	deviceID string,
	lgMsg *parser.LGMessage,
	topic string,
) error {
	// Convertir a modelo de dominio
	rawMsg := &domain.RawMessage{
		DeviceID:  deviceID,
		Brand:     "LG",
		Topic:     topic,
		Payload:   lgMsg,
		Timestamp: time.Now().UTC(),
	}

	return s.repository.SaveRawMessage(ctx, rawMsg)
}

// saveDeadLetter guarda mensajes que no pudieron procesarse
func (s *LGService) saveDeadLetter(
	ctx context.Context,
	topic string,
	payload []byte,
	err error,
) error {
	deadLetter := &domain.DeadLetterMessage{
		Topic:     topic,
		Payload:   payload,
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
	}

	return s.repository.SaveDeadLetterMessage(ctx, deadLetter)
}

// ============================================================
// Métodos auxiliares para polling (si necesitas después)
// ============================================================

// PollDeviceState obtiene estado de un device (si LG lo soporta)
// NOTA: Puede no ser necesario si MQTT publica todo
func (s *LGService) PollDeviceState(ctx context.Context, deviceID string) error {
	// TODO: Implementar llamada a API LG para polling
	// Esto depende de si LG permite polling de estado
	return nil
}

// ============================================================
// Métodos auxiliares para comandos (si necesitas después)
// ============================================================

// SendCommand envía un comando a LG
// NOTA: Revisar API LG para estructura exacta
func (s *LGService) SendCommand(ctx context.Context, deviceID string, action string, params map[string]interface{}) error {
	// TODO: Implementar llamada a API LG para enviar comando
	return nil
}
