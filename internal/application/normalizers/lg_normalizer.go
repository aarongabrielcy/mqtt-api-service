package normalizers

import (
	"encoding/json"
	"fmt"
	"mqtt-api-service/internal/adapters/parser"
	"time"

	"go.uber.org/zap"
)

type EventCode int

const (
	EventCodeTracking EventCode = iota
	EventCodeMQTTNotification
	EventCodePushNotification
	EventCodeDeviceStateChange
)

type NormalizedMessage struct {
	IMEI       string      `json:"imei"`
	ReceivedAt string      `json:"receivedAt"`
	Topic      string      `json:"topic"`
	Payload    interface{} `json:"payload"`
}

type LGTelemetryPayload struct {
	EventCode  EventCode `json:"eventCode"`
	DeviceType string    `json:"deviceType"`
	Power      bool      `json:"power"`

	Temperature TemperaturePayload `json:"temperature"`

	Humidity *float64 `json:"humidity,omitempty"`
}

type LGPushTelemetry struct {
	EventCode EventCode `json:"eventCode"`

	DeviceType *string `json:"deviceType,omitempty"`

	Power *bool `json:"power,omitempty"`

	Temperature *TemperaturePayload `json:"temperature,omitempty"`

	Humidity *float64 `json:"humidity,omitempty"`
}

type TemperaturePayload struct {
	Current float64 `json:"current"`
	Target  float64 `json:"target"`
	Unit    string  `json:"unit"`
}

type LGStateNormalizer struct {
	log *zap.Logger
}

func NewLGStateNormalizer(log *zap.Logger) *LGStateNormalizer {
	return &LGStateNormalizer{log: log}
}

func (n *LGStateNormalizer) NormalizeTelemetry(
	deviceID string,
	deviceType string,
	eventCode EventCode,
	state *parser.AirConditionerState,
) ([]byte, error) {
	if state == nil {
		return nil, fmt.Errorf("cannot normalize nil state for device %s", deviceID)
	}

	payload := LGTelemetryPayload{
		EventCode:  eventCode,
		DeviceType: deviceType,
		Power:      state.Operation.AirConOperationMode == "POWER_ON",
	}
	payload.Temperature.Current = state.Temperature.CurrentTemperature
	payload.Temperature.Target = state.Temperature.TargetTemperature
	payload.Temperature.Unit = state.Temperature.Unit

	//TODO: Corregir topic
	msg := NormalizedMessage{
		IMEI:       deviceID,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339),
		Topic:      fmt.Sprintf("devices/%s/telemetry", deviceID),
		Payload:    payload,
	}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("error serializing normalized telemetry: %w", err)
	}

	n.log.Debug("LG telemetry normalized",
		zap.String("deviceId", deviceID),
		zap.Int("payload_len", len(jsonBytes)),
	)

	return jsonBytes, nil
}
