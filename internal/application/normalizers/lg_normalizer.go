package normalizers

import (
	"encoding/json"
	"fmt"
	"mqtt-api-service/internal/adapters/parser"
	"mqtt-api-service/internal/infrastructure/debuglog"
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
	EventCodeEnergyUsage
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
	Energy      *LGEnergyInfo `json:"energy,omitempty"`
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

type LGEnergyInfo struct {
	DailyUsageKwh float64 `json:"dailyUsageKwh"`
	LastUpdated   int64   `json:"lastUpdated"`
}

type LGStateNormalizer struct {
	log *zap.Logger

	// debugStateLogs habilita el log "LG normalized telemetry payload"
	// (FASE LG-CMD-2E) — el payload JSON completo que se manda por gRPC,
	// para poder confirmar si trae state.oscillation/airflow/powersave.
	debugStateLogs bool
}

func NewLGStateNormalizer(log *zap.Logger, debugStateLogs bool) *LGStateNormalizer {
	return &LGStateNormalizer{log: log, debugStateLogs: debugStateLogs}
}

// buildEnvelope arma la parte común a cualquier evento de telemetry
// (device, state, climate) a partir del estado LG parseado. NormalizeTelemetry
// y NormalizeEnergyTelemetry comparten esta base y solo difieren en si
// completan Energy o no.
func buildEnvelope(
	deviceID string,
	deviceType string,
	eventCode EventCode,
	state *parser.AirConditionerState,
	receivedAt time.Time,
) LGTelemetryEnvelope {
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

	return envelope
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
	envelope := buildEnvelope(deviceID, deviceType, eventCode, state, receivedAt)

	return n.marshalAndLog(deviceID, envelope, receivedAt)
}

// NormalizeEnergyTelemetry construye el mismo envelope que NormalizeTelemetry
// (device/state/climate a partir del último estado LG conocido, típicamente
// leído desde el snapshot en Redis por el llamador) pero con el campo
// Energy poblado y eventCode=EventCodeEnergyUsage. Se recibe el eventCode
// explícito (en vez de fijarlo internamente) para dejar la puerta abierta a
// reusar esta función si en el futuro surge más de un tipo de evento de
// energía.
func (n *LGStateNormalizer) NormalizeEnergyTelemetry(
	deviceID string,
	deviceType string,
	eventCode EventCode,
	state *parser.AirConditionerState,
	energy LGEnergyInfo,
) (topic string, payload []byte, receivedAt time.Time, err error) {
	if state == nil {
		return "", nil, time.Time{}, fmt.Errorf("cannot normalize nil state for device %s", deviceID)
	}

	receivedAt = time.Now().UTC()
	envelope := buildEnvelope(deviceID, deviceType, eventCode, state, receivedAt)
	envelope.Energy = &energy

	return n.marshalAndLog(deviceID, envelope, receivedAt)
}

func (n *LGStateNormalizer) marshalAndLog(
	deviceID string,
	envelope LGTelemetryEnvelope,
	receivedAt time.Time,
) (topic string, payload []byte, out time.Time, err error) {
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

	if n.debugStateLogs {
		truncatedPayload, wasTruncated := debuglog.Truncate(jsonBytes, debuglog.DefaultMaxBodyLogLength)
		n.log.Debug("LG normalized telemetry payload",
			zap.String("deviceID", deviceID),
			zap.String("topic", topic),
			zap.Bool("payloadTruncated", wasTruncated),
			zap.Int("payloadLength", len(jsonBytes)),
			zap.ByteString("payload", truncatedPayload),
		)
	}

	return topic, jsonBytes, receivedAt, nil
}
