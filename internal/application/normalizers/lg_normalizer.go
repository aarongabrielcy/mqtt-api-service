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
	EventCodeDeviceStateChange
	EventCodePowerOn
	EventCodePowerOff
	EventCodeTemperatureChange
	EventCodeOperationModeChange
	EventCodeAirFlowChange
	EventCodeOscillationChange
	EventCodePowerSaveChange
)

// LGTelemetryEnvelope es el payload JSON directo que se envía a
// tracking-platform (RawMessage.Payload en el gRPC). No incluye "topic" (va
// en el campo dedicado del contrato) ni el raw completo de LG (eso se
// conserva en Mongo, ver internal/adapters/mongo).
type LGTelemetryEnvelope struct {
	Vendor      string        `json:"vendor"`
	Integration string        `json:"integration"`
	Event       EventCode     `json:"event"`
	Dt          int64         `json:"dt"`
	Device      LGDeviceRef   `json:"device"`
	State       LGStateInfo   `json:"state"`
	Climate     LGClimateInfo `json:"climate"`
}

type LGDeviceRef struct {
	ExternalID string `json:"externalId"`
	Type       string `json:"type"`
}

type LGStateInfo struct {
	Power         bool   `json:"power"`
	Mode          string `json:"mode"`
	OperationMode string `json:"operationMode"`
	Airflow       string `json:"airflow"`
	Oscillation   bool   `json:"oscillation"`
	PowerSave     bool   `json:"powersave"`
}

type LGClimateInfo struct {
	Temperature LGTemperatureInfo `json:"temperature"`
	Humidity    *float64          `json:"humidity"`
}

type LGTemperatureInfo struct {
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

// NormalizeTelemetry construye el topic (devices/<deviceID>/telemetry) y el
// payload JSON directo que se enviarán a tracking-platform vía
// TrackingClient.IngestRaw. LG no expone humedad en AirConditionerState, por
// lo que climate.humidity siempre viaja en null.
func (n *LGStateNormalizer) NormalizeTelemetry(
	deviceID string,
	deviceType string,
	eventCode EventCode,
	state *parser.AirConditionerState,
) (topic string, payload []byte, receivedAt time.Time, err error) {
	if state == nil {
		return "", nil, time.Time{}, fmt.Errorf("cannot normalize nil state for device %s", deviceID)
	}

	receivedAt = time.Now().UTC()

	envelope := LGTelemetryEnvelope{
		Vendor:      "lg",
		Integration: "lg-thinq",
		Event:       eventCode,
		Dt:          receivedAt.Unix(),
		Device: LGDeviceRef{
			ExternalID: deviceID,
			Type:       deviceType,
		},
		State: LGStateInfo{
			Power:         state.Operation.AirConOperationMode == "POWER_ON",
			Mode:          state.AirConJobMode.CurrentJobMode,
			OperationMode: state.Operation.AirConOperationMode,
			Airflow:       state.AirFlow.WindStrength,
			Oscillation:   state.WindDirection.RotateUpDown,
			PowerSave:     state.PowerSave.PowerSaveEnabled,
		},
	}
	envelope.Climate.Temperature.Current = state.Temperature.CurrentTemperature
	envelope.Climate.Temperature.Target = state.Temperature.TargetTemperature
	envelope.Climate.Temperature.Unit = state.Temperature.Unit

	jsonBytes, err := json.Marshal(envelope)
	if err != nil {
		return "", nil, time.Time{}, fmt.Errorf("error serializing normalized telemetry: %w", err)
	}

	topic = fmt.Sprintf("devices/%s/telemetry", deviceID)

	n.log.Debug("LG telemetry normalized",
		zap.String("deviceId", deviceID),
		zap.String("topic", topic),
		zap.Int("payload_len", len(jsonBytes)),
	)

	return topic, jsonBytes, receivedAt, nil
}
