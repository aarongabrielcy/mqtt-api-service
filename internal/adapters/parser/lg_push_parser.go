package parser

import (
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"mqtt-api-service/internal/adapters/mqtt"
)

type LGPushMessage struct {
	PushType   string          `json:"pushType"`
	ServiceID  string          `json:"serviceId"`
	DeviceID   string          `json:"deviceId"`
	UserList   []string        `json:"userList"`
	Report     json.RawMessage `json:"report"`
	DeviceType string          `json:"deviceType"`
}

type LGPushParser struct {
	log *zap.Logger
}

func NewLGPushParser(log *zap.Logger) *LGPushParser {
	return &LGPushParser{log: log}
}

func (p *LGPushParser) Parse(topic string, payload []byte) (*LGPushMessage, error) {
	var msg LGPushMessage

	if err := json.Unmarshal(payload, &msg); err != nil {
		// El topic app/clients/<id>/... puede contener el Client ID MQTT
		// real (LG_CLIENT_ID = LG_MQTT_CLIENT_ID): se loguea redactado.
		p.log.Warn("error parseando push message de LG",
			zap.String("topic", mqtt.RedactTopic(topic)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("invalid LG push payload: %w", err)
	}

	if msg.DeviceID == "" {
		return nil, fmt.Errorf("missing deviceId in LG push message")
	}

	return &msg, nil
}
