package commands

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"mqtt-api-service/internal/domain/interfaces"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// fakeTrackingClient captura cada IngestRaw llamado, para inspeccionar los
// ACK sintéticos publicados sin depender de un ingestion-service real.
type fakeTrackingClient struct {
	mu    sync.Mutex
	calls []interfaces.IngestRawInput
}

func (f *fakeTrackingClient) IngestRaw(_ context.Context, input interfaces.IngestRawInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, input)
	return nil
}

func (f *fakeTrackingClient) lastAck(t *testing.T) SyntheticAck {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		t.Fatal("expected at least one IngestRaw call, got none")
	}
	var ack SyntheticAck
	if err := json.Unmarshal(f.calls[len(f.calls)-1].Payload, &ack); err != nil {
		t.Fatalf("failed to unmarshal published ack: %v", err)
	}
	return ack
}

func (f *fakeTrackingClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func newTestConfirmationManager(t *testing.T) (*ConfirmationManager, *fakeTrackingClient) {
	t.Helper()
	cm, tracking, _ := newTestConfirmationManagerWithStatus(t)
	return cm, tracking
}

// newTestConfirmationManagerWithStatus expone también el fakeStatusPublisher,
// para los tests que necesitan verificar device.command.publish_failed
// (ej. timeout no confirmado tras un LG Device Timeout 2211, FASE LG-CMD-2D).
func newTestConfirmationManagerWithStatus(t *testing.T) (*ConfirmationManager, *fakeTrackingClient, *fakeStatusPublisher) {
	t.Helper()
	tracking := &fakeTrackingClient{}
	status := &fakeStatusPublisher{}
	ackPublisher := NewAckPublisher(tracking, zap.NewNop())
	cm := NewConfirmationManager(newTestRedis(t), ackPublisher, status, zap.NewNop(), 60*time.Second, false)
	return cm, tracking, status
}

// newTestConfirmationManagerWithDebugLogs es igual a
// newTestConfirmationManagerWithStatus pero con debugStateLogs=true (FASE
// LG-CMD-2E) — usado por los tests que verifican que activar el flag de
// diagnóstico no cambia ningún comportamiento funcional (solo agrega logs).
func newTestConfirmationManagerWithDebugLogs(t *testing.T) (*ConfirmationManager, *fakeTrackingClient, *fakeStatusPublisher) {
	t.Helper()
	tracking := &fakeTrackingClient{}
	status := &fakeStatusPublisher{}
	ackPublisher := NewAckPublisher(tracking, zap.NewNop())
	cm := NewConfirmationManager(newTestRedis(t), ackPublisher, status, zap.NewNop(), 60*time.Second, true)
	return cm, tracking, status
}

func TestConfirmationManager_MarkSeenIfNew(t *testing.T) {
	cm, _ := newTestConfirmationManager(t)
	ctx := context.Background()

	alreadySeen, err := cm.MarkSeenIfNew(ctx, "cmd_1", 10*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if alreadySeen {
		t.Error("first call should report alreadySeen=false")
	}

	alreadySeen, err = cm.MarkSeenIfNew(ctx, "cmd_1", 10*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !alreadySeen {
		t.Error("second call with same commandId should report alreadySeen=true")
	}
}

func TestConfirmationManager_TryConfirm_Power(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	pending := PendingConfirmation{
		CommandID:  "cmd_power",
		IMEI:       "imei-1",
		CommandKey: CommandKeyPower,
		Expected:   ExpectedState{Path: "state.power", Value: true},
		SentAt:     time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(60 * time.Second),
	}
	if err := cm.SavePending(ctx, pending); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.TryConfirm(ctx, "imei-1", CurrentState{Power: true})

	ack := tracking.lastAck(t)
	if !ack.OK || ack.CommandID != "cmd_power" || ack.Detail != AckDetailConfirmedByState {
		t.Fatalf("unexpected ack: %+v", ack)
	}

	if p, err := cm.getPending(ctx, "imei-1"); err != nil || p != nil {
		t.Error("pending confirmation should be deleted after confirming")
	}
}

func TestConfirmationManager_TryConfirm_TemperatureWithTolerance(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	pending := PendingConfirmation{
		CommandID:  "cmd_temp",
		IMEI:       "imei-2",
		CommandKey: CommandKeyTemperature,
		Expected:   ExpectedState{Path: "climate.temperature.target", Value: 24.0},
		ExpiresAt:  time.Now().UTC().Add(60 * time.Second),
	}
	if err := cm.SavePending(ctx, pending); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.TryConfirm(ctx, "imei-2", CurrentState{TemperatureTarget: 24.05})

	ack := tracking.lastAck(t)
	if !ack.OK || ack.CommandID != "cmd_temp" {
		t.Fatalf("unexpected ack: %+v", ack)
	}
}

func TestConfirmationManager_TryConfirm_ModeAndAirflow(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_mode", IMEI: "imei-3", CommandKey: CommandKeyMode,
		Expected: ExpectedState{Path: "state.mode", Value: "COOL"}, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}
	cm.TryConfirm(ctx, "imei-3", CurrentState{Mode: "COOL"})
	if ack := tracking.lastAck(t); !ack.OK || ack.CommandID != "cmd_mode" {
		t.Fatalf("unexpected ack for mode: %+v", ack)
	}

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_airflow", IMEI: "imei-4", CommandKey: CommandKeyAirflow,
		Expected: ExpectedState{Path: "state.airflow", Value: "HIGH"}, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}
	cm.TryConfirm(ctx, "imei-4", CurrentState{Airflow: "HIGH"})
	if ack := tracking.lastAck(t); !ack.OK || ack.CommandID != "cmd_airflow" {
		t.Fatalf("unexpected ack for airflow: %+v", ack)
	}
}

func TestConfirmationManager_TryConfirm_NoMatchDoesNotPublish(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_5", IMEI: "imei-5", CommandKey: CommandKeyPower,
		Expected: ExpectedState{Path: "state.power", Value: true}, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.TryConfirm(ctx, "imei-5", CurrentState{Power: false})

	if tracking.callCount() != 0 {
		t.Errorf("no ack should be published while state does not match, got %d calls", tracking.callCount())
	}
	if p, err := cm.getPending(ctx, "imei-5"); err != nil || p == nil {
		t.Error("pending confirmation should remain while unconfirmed")
	}
}

func TestConfirmationManager_TryConfirm_NoPendingIsNoop(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	cm.TryConfirm(context.Background(), "imei-none", CurrentState{Power: true})
	if tracking.callCount() != 0 {
		t.Error("no ack should be published when there is no pending confirmation")
	}
}

func TestConfirmationManager_SweepTimeouts_PublishesAckFalse(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_timeout", IMEI: "imei-6", CommandKey: CommandKeyPower,
		Expected: ExpectedState{Path: "state.power", Value: true},
		// Ya expirada.
		ExpiresAt: time.Now().UTC().Add(-1 * time.Second),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.SweepTimeouts(ctx)

	ack := tracking.lastAck(t)
	if ack.OK || ack.CommandID != "cmd_timeout" || ack.Detail != AckDetailAckTimeout {
		t.Fatalf("unexpected ack for timeout sweep: %+v", ack)
	}
	if p, err := cm.getPending(ctx, "imei-6"); err != nil || p != nil {
		t.Error("pending confirmation should be deleted after timeout sweep")
	}
}

func TestConfirmationManager_SweepTimeouts_SkipsNonExpired(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_active", IMEI: "imei-7", CommandKey: CommandKeyPower,
		Expected: ExpectedState{Path: "state.power", Value: true}, ExpiresAt: time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.SweepTimeouts(ctx)

	if tracking.callCount() != 0 {
		t.Error("sweep should not touch a still-active pending confirmation")
	}
}

func TestConfirmationManager_TryConfirm_DeviceTimeoutReason_ConfirmedAfterDeviceTimeout(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_timeout_confirmed", IMEI: "imei-9", CommandKey: CommandKeyPower,
		Expected:  ExpectedState{Path: "state.power", Value: false},
		ExpiresAt: time.Now().UTC().Add(time.Minute),
		Reason:    PendingReasonDeviceTimeout,
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.TryConfirm(ctx, "imei-9", CurrentState{Power: false})

	ack := tracking.lastAck(t)
	if !ack.OK || ack.CommandID != "cmd_timeout_confirmed" || ack.Detail != AckDetailConfirmedAfterDeviceTimeout {
		t.Fatalf("unexpected ack: %+v", ack)
	}
	if p, err := cm.getPending(ctx, "imei-9"); err != nil || p != nil {
		t.Error("pending confirmation should be deleted after confirming")
	}
}

func TestConfirmationManager_SweepTimeouts_DeviceTimeoutReason_PublishesFailedAndUnconfirmedAck(t *testing.T) {
	cm, tracking, status := newTestConfirmationManagerWithStatus(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_timeout_unconfirmed", IMEI: "imei-10", CommandKey: CommandKeyPower,
		Expected:  ExpectedState{Path: "state.power", Value: false},
		ExpiresAt: time.Now().UTC().Add(-1 * time.Second),
		Reason:    PendingReasonDeviceTimeout,
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.SweepTimeouts(ctx)

	ack := tracking.lastAck(t)
	if ack.OK || ack.CommandID != "cmd_timeout_unconfirmed" || ack.Detail != AckDetailDeviceTimeoutUnconfirmed {
		t.Fatalf("unexpected ack for unconfirmed device timeout: %+v", ack)
	}

	_, failed := status.counts()
	if failed != 1 || status.failed[0].CommandID != "cmd_timeout_unconfirmed" || status.failed[0].ErrorMessage != AckDetailDeviceTimeoutUnconfirmed {
		t.Fatalf("expected 1 publish_failed event with device_timeout_unconfirmed, got %+v", status.failed)
	}

	if p, err := cm.getPending(ctx, "imei-10"); err != nil || p != nil {
		t.Error("pending confirmation should be deleted after timeout sweep")
	}
}

func TestConfirmationManager_SweepTimeouts_NormalReason_DoesNotPublishFailed(t *testing.T) {
	cm, tracking, status := newTestConfirmationManagerWithStatus(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_timeout_normal", IMEI: "imei-11", CommandKey: CommandKeyPower,
		Expected:  ExpectedState{Path: "state.power", Value: true},
		ExpiresAt: time.Now().UTC().Add(-1 * time.Second),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.SweepTimeouts(ctx)

	ack := tracking.lastAck(t)
	if ack.OK || ack.Detail != AckDetailAckTimeout {
		t.Fatalf("unexpected ack for normal timeout: %+v", ack)
	}

	_, failed := status.counts()
	if failed != 0 {
		t.Errorf("a normal (non-device-timeout) pending timeout should not publish device.command.publish_failed, got %d", failed)
	}
}

// TestConfirmationManager_DebugStateLogs_DoesNotChangeBehavior cubre FASE
// LG-CMD-2E: LG_DEBUG_STATE_LOGS solo debe agregar visibilidad (logs), nunca
// alterar qué ACK se publica o cuándo. Se ejercitan TryConfirm (match y
// no-match) y SweepTimeouts con debugStateLogs=true y se verifica que el
// comportamiento observable es idéntico al de los tests equivalentes con el
// flag en false.
func TestConfirmationManager_DebugStateLogs_TryConfirmMatch_SameBehaviorAsWithoutDebug(t *testing.T) {
	cm, tracking, _ := newTestConfirmationManagerWithDebugLogs(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_debug_1", IMEI: "imei-debug-1", CommandKey: CommandKeyOscillation,
		Expected:  ExpectedState{Path: "state.oscillation", Value: true},
		ExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.TryConfirm(ctx, "imei-debug-1", CurrentState{Oscillation: true})

	ack := tracking.lastAck(t)
	if !ack.OK || ack.CommandID != "cmd_debug_1" || ack.Detail != AckDetailConfirmedByState {
		t.Fatalf("unexpected ack with debugStateLogs=true: %+v", ack)
	}
	if p, err := cm.getPending(ctx, "imei-debug-1"); err != nil || p != nil {
		t.Error("pending confirmation should still be deleted after confirming, even with debugStateLogs=true")
	}
}

func TestConfirmationManager_DebugStateLogs_TryConfirmNoMatch_DoesNotPublishOrPanic(t *testing.T) {
	cm, tracking, _ := newTestConfirmationManagerWithDebugLogs(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_debug_2", IMEI: "imei-debug-2", CommandKey: CommandKeyOscillation,
		Expected:  ExpectedState{Path: "state.oscillation", Value: true},
		ExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	// Reproduce el caso reportado (FASE LG-CMD-2E): expected oscillation
	// true, actual reporta false — no debe publicar nada, con o sin debug.
	cm.TryConfirm(ctx, "imei-debug-2", CurrentState{Oscillation: false})

	if tracking.callCount() != 0 {
		t.Errorf("no ack should be published while state does not match, got %d calls", tracking.callCount())
	}
}

func TestConfirmationManager_DebugStateLogs_SweepTimeouts_SameBehaviorAsWithoutDebug(t *testing.T) {
	cm, tracking, _ := newTestConfirmationManagerWithDebugLogs(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_debug_3", IMEI: "imei-debug-3", CommandKey: CommandKeyOscillation,
		Expected:  ExpectedState{Path: "state.oscillation", Value: true},
		ExpiresAt: time.Now().UTC().Add(-1 * time.Second),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	cm.SweepTimeouts(ctx)

	ack := tracking.lastAck(t)
	if ack.OK || ack.CommandID != "cmd_debug_3" || ack.Detail != AckDetailAckTimeout {
		t.Fatalf("unexpected ack for timeout sweep with debugStateLogs=true: %+v", ack)
	}
}

func TestConfirmationManager_SavePending_SupersedesExisting(t *testing.T) {
	cm, tracking := newTestConfirmationManager(t)
	ctx := context.Background()

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_old", IMEI: "imei-8", CommandKey: CommandKeyPower,
		Expected: ExpectedState{Path: "state.power", Value: true}, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("SavePending failed: %v", err)
	}

	if err := cm.SavePending(ctx, PendingConfirmation{
		CommandID: "cmd_new", IMEI: "imei-8", CommandKey: CommandKeyMode,
		Expected: ExpectedState{Path: "state.mode", Value: "COOL"}, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("second SavePending failed: %v", err)
	}

	ack := tracking.lastAck(t)
	if ack.OK || ack.CommandID != "cmd_old" || ack.Detail != AckDetailSupersededByNewCommand {
		t.Fatalf("expected superseded ack for old command, got: %+v", ack)
	}

	pending, err := cm.getPending(ctx, "imei-8")
	if err != nil || pending == nil || pending.CommandID != "cmd_new" {
		t.Fatalf("expected the new pending confirmation to replace the old one, got: %+v, err=%v", pending, err)
	}
}
