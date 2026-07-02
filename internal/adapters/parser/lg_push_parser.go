package parser

import (
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
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
		p.log.Warn("error parseando push message de LG",
			zap.String("topic", topic),
			zap.Error(err),
		)
		return nil, fmt.Errorf("invalid LG push payload: %w", err)
	}

	if msg.DeviceID == "" {
		return nil, fmt.Errorf("missing deviceId in LG push message")
	}

	return &msg, nil
}
