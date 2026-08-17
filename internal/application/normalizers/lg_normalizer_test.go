package normalizers

import (
	"encoding/json"
	"testing"

	"mqtt-api-service/internal/adapters/parser"

	"go.uber.org/zap"
)

func newTestState() *parser.AirConditionerState {
	state := &parser.AirConditionerState{}
	state.Operation.AirConOperationMode = "POWER_ON"
	state.AirConJobMode.CurrentJobMode = "COOL"
	state.Temperature.CurrentTemperature = 22
	state.Temperature.TargetTemperature = 24
	state.Temperature.Unit = "C"
	return state
}

func TestNormalizeTelemetry_Topic(t *testing.T) {
	n := NewLGStateNormalizer(zap.NewNop())

	topic, _, _, err := n.NormalizeTelemetry("device-123", "DEVICE_AIR_CONDITIONER", EventCodeTracking, newTestState())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const want = "devices/device-123/telemetry"
	if topic != want {
		t.Errorf("topic = %q, want %q", topic, want)
	}
}

func TestNormalizeTelemetry_PayloadShape(t *testing.T) {
	n := NewLGStateNormalizer(zap.NewNop())

	_, payload, receivedAt, err := n.NormalizeTelemetry("device-123", "DEVICE_AIR_CONDITIONER", EventCodeTracking, newTestState())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("payload no es JSON válido: %v", err)
	}

	if _, ok := got["topic"]; ok {
		t.Error(`el payload no debe incluir un campo "topic" (va en el gRPC aparte)`)
	}
	if _, ok := got["payload"]; ok {
		t.Error(`el payload no debe incluir un campo "payload" envolviendo el real`)
	}

	if got["vendor"] != "lg" {
		t.Errorf(`vendor = %v, want "lg"`, got["vendor"])
	}
	if got["integration"] != "lg-thinq" {
		t.Errorf(`integration = %v, want "lg-thinq"`, got["integration"])
	}
	if got["event"] != float64(EventCodeTracking) {
		t.Errorf("event = %v, want %v", got["event"], float64(EventCodeTracking))
	}
	dt, ok := got["dt"].(float64)
	if !ok {
		t.Fatalf("dt ausente o no numérico: %v", got["dt"])
	}
	if int64(dt) != receivedAt.Unix() {
		t.Errorf("dt = %v, want %v", int64(dt), receivedAt.Unix())
	}

	device, ok := got["device"].(map[string]interface{})
	if !ok {
		t.Fatalf("device no es un objeto: %v", got["device"])
	}
	if device["externalId"] != "device-123" {
		t.Errorf("device.externalId = %v, want device-123", device["externalId"])
	}
	if device["type"] != "DEVICE_AIR_CONDITIONER" {
		t.Errorf("device.type = %v, want DEVICE_AIR_CONDITIONER", device["type"])
	}

	state, ok := got["state"].(map[string]interface{})
	if !ok {
		t.Fatalf("state no es un objeto: %v", got["state"])
	}
	if state["power"] != true {
		t.Errorf("state.power = %v, want true", state["power"])
	}
	if state["mode"] != "COOL" {
		t.Errorf("state.mode = %v, want COOL", state["mode"])
	}
	if state["operationMode"] != "POWER_ON" {
		t.Errorf("state.operationMode = %v, want POWER_ON", state["operationMode"])
	}

	climate, ok := got["climate"].(map[string]interface{})
	if !ok {
		t.Fatalf("climate no es un objeto: %v", got["climate"])
	}
	temperature, ok := climate["temperature"].(map[string]interface{})
	if !ok {
		t.Fatalf("climate.temperature no es un objeto: %v", climate["temperature"])
	}
	if temperature["current"] != 22.0 {
		t.Errorf("climate.temperature.current = %v, want 22", temperature["current"])
	}
	if temperature["target"] != 24.0 {
		t.Errorf("climate.temperature.target = %v, want 24", temperature["target"])
	}
	if temperature["unit"] != "C" {
		t.Errorf("climate.temperature.unit = %v, want C", temperature["unit"])
	}
	if humidity, exists := climate["humidity"]; !exists || humidity != nil {
		t.Errorf("climate.humidity = %v, want null (LG no expone humedad)", humidity)
	}
}

func TestNormalizeTelemetry_NilStateReturnsError(t *testing.T) {
	n := NewLGStateNormalizer(zap.NewNop())

	if _, _, _, err := n.NormalizeTelemetry("device-123", "DEVICE_AIR_CONDITIONER", EventCodeTracking, nil); err == nil {
		t.Error("se esperaba un error para state nil, se obtuvo nil")
	}
}
