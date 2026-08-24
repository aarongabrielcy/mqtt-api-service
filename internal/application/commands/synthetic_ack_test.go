package commands

import (
	"context"
	"encoding/json"
	"testing"

	"go.uber.org/zap"
)

// TestPublishSyntheticAck_TopicAndPayloadShape verifica que
// PublishSyntheticAck llama a TrackingClient.IngestRaw con el topic exacto
// devices/<imei>/ack y un payload JSON con todas las keys/tipos esperados
// por el contrato DeviceCommandAck de tracking-platform (ver el comentario
// de PublishSyntheticAck en synthetic_ack.go para la verificación de
// compatibilidad hecha por inspección directa, solo lectura, de
// tracking-platform/apps/ingestion-service).
func TestPublishSyntheticAck_TopicAndPayloadShape(t *testing.T) {
	tracking := &fakeTrackingClient{}
	publisher := NewAckPublisher(tracking, zap.NewNop())

	err := publisher.PublishSyntheticAck(context.Background(), "test-imei-123", SyntheticAck{
		CommandID: "cmd_abc",
		OK:        true,
		Command:   CommandKeyPower,
		Value:     true,
		Detail:    AckDetailConfirmedByState,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tracking.callCount() != 1 {
		t.Fatalf("expected exactly 1 IngestRaw call, got %d", tracking.callCount())
	}

	call := tracking.calls[0]

	const wantTopic = "devices/test-imei-123/ack"
	if call.Topic != wantTopic {
		t.Errorf("Topic = %q, want %q", call.Topic, wantTopic)
	}

	var got map[string]any
	if err := json.Unmarshal(call.Payload, &got); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}

	// Presence + type checks against the DeviceCommandAck-like contract:
	// {commandId, ok, command, value, detail, device_id, dt}.
	commandID, ok := got["commandId"].(string)
	if !ok || commandID == "" {
		t.Errorf("commandId = %v (%T), want a non-empty string", got["commandId"], got["commandId"])
	}

	okField, ok := got["ok"].(bool)
	if !ok || !okField {
		t.Errorf("ok = %v (%T), want boolean true", got["ok"], got["ok"])
	}

	if _, ok := got["command"].(string); !ok {
		t.Errorf("command = %v (%T), want a string", got["command"], got["command"])
	}

	if _, present := got["value"]; !present {
		t.Error("value field is missing from the published ack")
	}

	if detail, ok := got["detail"].(string); !ok || detail == "" {
		t.Errorf("detail = %v (%T), want a non-empty string", got["detail"], got["detail"])
	}

	deviceID, ok := got["device_id"].(string)
	if !ok || deviceID != "test-imei-123" {
		t.Errorf("device_id = %v, want test-imei-123", got["device_id"])
	}

	dt, ok := got["dt"].(float64)
	if !ok || dt <= 0 {
		t.Errorf("dt = %v (%T), want a positive unix-seconds number", got["dt"], got["dt"])
	}
	// dt debe estar en segundos (10 dígitos aprox. para fechas actuales), no
	// en milisegundos (13 dígitos) — una fuente común de incompatibilidad
	// entre servicios que no comparten el mismo formato de timestamp.
	if dt > 9999999999 {
		t.Errorf("dt = %v looks like milliseconds, want unix seconds", dt)
	}
}

func TestPublishSyntheticAck_FailureShape(t *testing.T) {
	tracking := &fakeTrackingClient{}
	publisher := NewAckPublisher(tracking, zap.NewNop())

	err := publisher.PublishSyntheticAck(context.Background(), "test-imei-456", SyntheticAck{
		CommandID: "cmd_def",
		OK:        false,
		Command:   CommandKeyTemperature,
		Detail:    AckDetailAckTimeout,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ack := tracking.lastAck(t)
	if ack.OK {
		t.Error("OK = true, want false for a failure ack")
	}
	if ack.CommandID != "cmd_def" || ack.Detail != AckDetailAckTimeout {
		t.Errorf("unexpected ack: %+v", ack)
	}
}

func TestPublishSyntheticAck_EmptyCommandIdRejected(t *testing.T) {
	tracking := &fakeTrackingClient{}
	publisher := NewAckPublisher(tracking, zap.NewNop())

	err := publisher.PublishSyntheticAck(context.Background(), "test-imei-789", SyntheticAck{OK: true})
	if err == nil {
		t.Fatal("expected an error for an empty commandId, got nil")
	}
	if tracking.callCount() != 0 {
		t.Error("IngestRaw should never be called when commandId is empty")
	}
}

func TestPublishSyntheticAck_DtDefaultsToNowInUnixSeconds(t *testing.T) {
	tracking := &fakeTrackingClient{}
	publisher := NewAckPublisher(tracking, zap.NewNop())

	if err := publisher.PublishSyntheticAck(context.Background(), "test-imei-000", SyntheticAck{
		CommandID: "cmd_ghi",
		OK:        true,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ack := tracking.lastAck(t)
	if ack.Dt == 0 {
		t.Error("Dt should default to the current unix timestamp when not set")
	}
}
