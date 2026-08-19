package commands

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	lgapi "mqtt-api-service/internal/adapters/api/lg"
	commandsdomain "mqtt-api-service/internal/domain/commands"

	"go.uber.org/zap"
)

type fakeLGCommander struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeLGCommander) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeLGCommander) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeLGCommander) SetDevicePower(_ context.Context, _ string, _ bool) error {
	f.record("SetDevicePower")
	return f.err
}
func (f *fakeLGCommander) SetDeviceTemperature(_ context.Context, _ string, _ float64) error {
	f.record("SetDeviceTemperature")
	return f.err
}
func (f *fakeLGCommander) SetOperationMode(_ context.Context, _ string, _ string) error {
	f.record("SetOperationMode")
	return f.err
}
func (f *fakeLGCommander) SetAirFlow(_ context.Context, _ string, _ string) error {
	f.record("SetAirFlow")
	return f.err
}
func (f *fakeLGCommander) SetOscillation(_ context.Context, _ string, _ bool) error {
	f.record("SetOscillation")
	return f.err
}
func (f *fakeLGCommander) SetPowerSave(_ context.Context, _ string, _ bool) error {
	f.record("SetPowerSave")
	return f.err
}

type fakeStatusPublisher struct {
	mu     sync.Mutex
	sent   []commandsdomain.CommandSentEvent
	failed []commandsdomain.CommandPublishFailedEvent
}

func (f *fakeStatusPublisher) PublishSent(_ context.Context, evt commandsdomain.CommandSentEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, evt)
	return nil
}

func (f *fakeStatusPublisher) PublishFailed(_ context.Context, evt commandsdomain.CommandPublishFailedEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, evt)
	return nil
}

func (f *fakeStatusPublisher) counts() (sent, failed int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent), len(f.failed)
}

func newTestDispatcher(t *testing.T, lgErr error) (*CommandDispatcher, *fakeLGCommander, *fakeStatusPublisher, *fakeTrackingClient, *ConfirmationManager) {
	t.Helper()

	lg := &fakeLGCommander{err: lgErr}
	status := &fakeStatusPublisher{}
	tracking := &fakeTrackingClient{}
	ackPublisher := NewAckPublisher(tracking, zap.NewNop())
	confirmation := NewConfirmationManager(newTestRedis(t), ackPublisher, status, zap.NewNop(), 60*time.Second)

	dispatcher := NewCommandDispatcher(
		zap.NewNop(),
		lg,
		confirmation,
		ackPublisher,
		status,
		ParseConfig{TemperatureMinC: 16, TemperatureMaxC: 30},
		10*time.Minute,
	)

	return dispatcher, lg, status, tracking, confirmation
}

func TestDispatch_Success_PublishesSentAndSavesPending(t *testing.T) {
	dispatcher, lg, status, tracking, confirmation := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_1",
		IMEI:        "imei-1",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 1 {
		t.Errorf("expected 1 LG API call, got %d", lg.callCount())
	}

	sent, failed := status.counts()
	if sent != 1 || failed != 0 {
		t.Errorf("expected 1 sent / 0 failed, got sent=%d failed=%d", sent, failed)
	}
	if status.sent[0].MQTTTopic != "devices/imei-1/cmd" {
		t.Errorf("mqttTopic = %q, want devices/imei-1/cmd", status.sent[0].MQTTTopic)
	}

	if tracking.callCount() != 0 {
		t.Error("no ack should be published yet: confirmation is still pending")
	}

	pending, err := confirmation.getPending(ctx, "imei-1")
	if err != nil || pending == nil || pending.CommandID != "cmd_1" {
		t.Fatalf("expected a pending confirmation for cmd_1, got %+v, err=%v", pending, err)
	}
}

func TestDispatch_LGAPIError_PublishesFailedAndAckFalse(t *testing.T) {
	dispatcher, _, status, tracking, _ := newTestDispatcher(t, errBoom)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_2",
		IMEI:        "imei-2",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sent, failed := status.counts()
	if sent != 0 || failed != 1 {
		t.Errorf("expected 0 sent / 1 failed, got sent=%d failed=%d", sent, failed)
	}

	ack := tracking.lastAck(t)
	if ack.OK || ack.Detail != AckDetailLGAPIError {
		t.Errorf("unexpected ack: %+v", ack)
	}
}

func TestDispatch_DeviceDisconnected_DetailIsDeviceDisconnected(t *testing.T) {
	apiErr := &lgapi.APIError{StatusCode: 416, Code: "1222"}
	dispatcher, _, status, tracking, _ := newTestDispatcher(t, apiErr)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_3",
		IMEI:        "imei-3",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, failed := status.counts()
	if failed != 1 || status.failed[0].ErrorMessage != AckDetailDeviceDisconnected {
		t.Errorf("expected failed event with device_disconnected, got %+v", status.failed)
	}

	ack := tracking.lastAck(t)
	if ack.OK || ack.Detail != AckDetailDeviceDisconnected {
		t.Errorf("unexpected ack: %+v", ack)
	}
}

func TestDispatch_DeviceTimeout2211_DoesNotFailImmediately_RegistersPending(t *testing.T) {
	apiErr := &lgapi.APIError{StatusCode: 400, Code: "2211", Message: "Device Timeout"}
	dispatcher, lg, status, tracking, confirmation := newTestDispatcher(t, apiErr)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_timeout_1",
		IMEI:        "imei-timeout-1",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":false}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 1 {
		t.Errorf("expected 1 LG API call, got %d", lg.callCount())
	}

	// No failure immediato: SENT se publica (el comando llegó al proveedor,
	// solo la confirmación quedó ambigua) pero publish_failed NO.
	sent, failed := status.counts()
	if sent != 1 || failed != 0 {
		t.Errorf("expected 1 sent / 0 failed for a 2211 timeout, got sent=%d failed=%d", sent, failed)
	}

	if tracking.callCount() != 0 {
		t.Error("no synthetic ack should be published immediately for a 2211 device timeout")
	}

	pending, err := confirmation.getPending(ctx, "imei-timeout-1")
	if err != nil || pending == nil || pending.CommandID != "cmd_timeout_1" {
		t.Fatalf("expected a pending confirmation for cmd_timeout_1, got %+v, err=%v", pending, err)
	}
	if pending.Reason != PendingReasonDeviceTimeout {
		t.Errorf("expected pending.Reason = %q, got %q", PendingReasonDeviceTimeout, pending.Reason)
	}
}

func TestDispatch_DeviceTimeout2211_ThenStateConfirms_PublishesConfirmedAfterDeviceTimeoutAck(t *testing.T) {
	apiErr := &lgapi.APIError{StatusCode: 400, Code: "2211", Message: "Device Timeout"}
	dispatcher, _, _, tracking, confirmation := newTestDispatcher(t, apiErr)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_timeout_2",
		IMEI:        "imei-timeout-2",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":false}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	confirmation.TryConfirm(ctx, "imei-timeout-2", CurrentState{Power: false})

	ack := tracking.lastAck(t)
	if !ack.OK || ack.CommandID != "cmd_timeout_2" || ack.Detail != AckDetailConfirmedAfterDeviceTimeout {
		t.Fatalf("unexpected ack after state confirms a 2211 timeout: %+v", ack)
	}
}

func TestDispatch_DeviceTimeout2211_ThenSweepExpires_PublishesFailedAndUnconfirmedAck(t *testing.T) {
	apiErr := &lgapi.APIError{StatusCode: 400, Code: "2211", Message: "Device Timeout"}
	dispatcher, _, status, tracking, confirmation := newTestDispatcher(t, apiErr)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_timeout_3",
		IMEI:        "imei-timeout-3",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":false}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Fuerza la expiración de la pendiente sin esperar el ackTimeout real.
	pending, err := confirmation.getPending(ctx, "imei-timeout-3")
	if err != nil || pending == nil {
		t.Fatalf("expected a pending confirmation, got %+v, err=%v", pending, err)
	}
	pending.ExpiresAt = time.Now().UTC().Add(-1 * time.Second)
	if err := confirmation.SavePending(ctx, *pending); err != nil {
		t.Fatalf("failed to force-expire pending: %v", err)
	}

	confirmation.SweepTimeouts(ctx)

	_, failed := status.counts()
	if failed != 1 || status.failed[0].CommandID != "cmd_timeout_3" || status.failed[0].ErrorMessage != AckDetailDeviceTimeoutUnconfirmed {
		t.Fatalf("expected 1 publish_failed with device_timeout_unconfirmed, got %+v", status.failed)
	}

	ack := tracking.lastAck(t)
	if ack.OK || ack.CommandID != "cmd_timeout_3" || ack.Detail != AckDetailDeviceTimeoutUnconfirmed {
		t.Fatalf("unexpected ack for expired 2211 timeout: %+v", ack)
	}
}

func TestDispatch_DuplicateCommandId_DoesNotReexecute(t *testing.T) {
	dispatcher, lg, status, _, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_4",
		IMEI:        "imei-4",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error on first dispatch: %v", err)
	}
	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error on duplicate dispatch: %v", err)
	}

	if lg.callCount() != 1 {
		t.Errorf("expected exactly 1 LG API call across both dispatches, got %d", lg.callCount())
	}
	sent, _ := status.counts()
	if sent != 1 {
		t.Errorf("expected exactly 1 sent event, got %d", sent)
	}
}

func TestDispatch_UnsupportedCommandKey_PublishesFailedAndAck(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	// commandCode 250 is inside the LG-reserved 200-299 range (so
	// LooksLikeLGCommand is true — this event IS meant for LG) but isn't
	// one of the 6 mapped codes, so it stays unresolvable: a real
	// unsupported-command case, not an ESP32/foreign event.
	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_5",
		IMEI:        "imei-5",
		CommandCode: 250,
		Payload:     json.RawMessage(`{}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Error("LG API should never be called for an unsupported command")
	}
	_, failed := status.counts()
	if failed != 1 {
		t.Errorf("expected 1 failed event, got %d", failed)
	}
	ack := tracking.lastAck(t)
	if ack.OK || ack.Detail != AckDetailUnsupportedCommand {
		t.Errorf("unexpected ack: %+v", ack)
	}
}

func TestDispatch_ForeignCommand_SilentlyIgnored(t *testing.T) {
	// Evidencia real (FASE LG-CMD-E2E-DIAG): comandos ESP32 legacy
	// (commandCode 101, sin commandKey/metadata) llegan al mismo topic
	// device.command.requested que ya consume mqtt-adapter-service en su
	// propio consumer group. Antes de este fix, mqtt-api-service los
	// trataba como "unsupported" y publicaba device.command.publish_failed
	// + un ACK sintético ok:false — compitiendo con el device.command.sent
	// legítimo de mqtt-adapter-service para el mismo commandId. Ahora deben
	// ignorarse en silencio: cero llamadas a Kafka o al ACK sintético.
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_esp32_1",
		IMEI:        "esp32-imei-1",
		CommandCode: 101,
		CommandType: "OUTPUT_1",
		Payload:     json.RawMessage(`{"commandId":"cmd_esp32_1","source":"manual","commands":{"101":"1"}}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Error("LG API should never be called for a foreign (non-LG) command")
	}
	sent, failed := status.counts()
	if sent != 0 || failed != 0 {
		t.Errorf("no Kafka status event should be published for a foreign command, got sent=%d failed=%d", sent, failed)
	}
	if tracking.callCount() != 0 {
		t.Error("no synthetic ack should be published for a foreign command")
	}
}

func TestDispatch_InvalidPayload_PublishesFailedAndAck(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_6",
		IMEI:        "imei-6",
		CommandCode: 202, // lg.temperature.set
		Payload:     json.RawMessage(`{"temperature":100}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Error("LG API should never be called for an invalid payload")
	}
	_, failed := status.counts()
	if failed != 1 {
		t.Errorf("expected 1 failed event, got %d", failed)
	}
	ack := tracking.lastAck(t)
	if ack.OK || ack.Detail != AckDetailInvalidPayload {
		t.Errorf("unexpected ack: %+v", ack)
	}
}

func TestDispatch_MissingCommandIdOrIMEI_Dropped(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	if err := dispatcher.Dispatch(ctx, commandsdomain.DeviceCommandEvent{IMEI: "imei-7"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := dispatcher.Dispatch(ctx, commandsdomain.DeviceCommandEvent{CommandID: "cmd_8"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Error("LG API should never be called when commandId or imei is missing")
	}
	sent, failed := status.counts()
	if sent != 0 || failed != 0 {
		t.Errorf("nothing should be published when commandId/imei is missing, got sent=%d failed=%d", sent, failed)
	}
	if tracking.callCount() != 0 {
		t.Error("no ack should be published when commandId/imei is missing")
	}
}

// errBoom es un error genérico usado para simular un fallo cualquiera de la
// LG API (no clasificado como device disconnected).
var errBoom = &genericError{"boom"}

type genericError struct{ msg string }

func (e *genericError) Error() string { return e.msg }
