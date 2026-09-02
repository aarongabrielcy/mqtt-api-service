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

	// duringExec, si está seteado, se invoca desde dentro de SetDevicePower
	// — simula la carrera real (FASE LG-CMD-2G): LG aplica el cambio físico
	// y el push/poll que lo confirma llega mientras la llamada HTTP a
	// /control todavía está en curso desde el punto de vista del
	// dispatcher.
	duringExec func()

	// onRefresh / refreshCalled / refreshErr / refreshCallCnt (FASE
	// LG-CMD-2H) simulan LGService.RefreshDeviceState. Deliberadamente NO
	// pasan por record()/calls: triggerPostCommandRefresh corre en su
	// propia goroutine, así que si RefreshDeviceState compartiera el mismo
	// contador que SetDevicePower/etc., los tests existentes que ya
	// afirman callCount()==N justo después de Dispatch() se volverían
	// flaky (la goroutine puede o no haber corrido todavía). onRefresh
	// permite que un test dispare TryConfirm con un estado elegido (como
	// haría un refresh real exitoso o no); refreshCalled (si no es nil)
	// recibe una señal no bloqueante para esperar de forma determinista a
	// que la goroutine haya corrido, sin sleeps arbitrarios.
	onRefresh      func()
	refreshCalled  chan struct{}
	refreshErr     error
	refreshCallCnt int

	// knownPower / powerOn (FASE LG-CMD-2I) controlan lo que devuelve
	// GetLastKnownPower: por defecto knownPower=false (sin estado conocido,
	// no bloquea por precondición), como en un dispositivo recién agregado
	// del que todavía no se leyó ningún estado.
	knownPower bool
	powerOn    bool
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
	if f.duringExec != nil {
		f.duringExec()
	}
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

func (f *fakeLGCommander) RefreshDeviceState(_ context.Context, _ string) error {
	f.mu.Lock()
	f.refreshCallCnt++
	f.mu.Unlock()

	if f.onRefresh != nil {
		f.onRefresh()
	}
	if f.refreshCalled != nil {
		select {
		case f.refreshCalled <- struct{}{}:
		default:
		}
	}
	return f.refreshErr
}

func (f *fakeLGCommander) refreshCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshCallCnt
}

func (f *fakeLGCommander) GetLastKnownPower(_ context.Context, _ string) (known bool, powerOn bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.knownPower, f.powerOn
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
	confirmation := NewConfirmationManager(newTestRedis(t), ackPublisher, status, zap.NewNop(), 60*time.Second, false)

	dispatcher := NewCommandDispatcher(
		zap.NewNop(),
		lg,
		confirmation,
		ackPublisher,
		status,
		ParseConfig{TemperatureMinC: 16, TemperatureMaxC: 30},
		10*time.Minute,
		0, // postRefreshDelay=0: dispara el refresh post-comando de inmediato en los tests.
	)

	return dispatcher, lg, status, tracking, confirmation
}

func TestDispatch_Success_PublishesSentAndSavesPending(t *testing.T) {
	dispatcher, lg, status, tracking, confirmation := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_1",
		IMEI:         "imei-1",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
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

// TestDispatch_PostCommandRefresh_TriggeredAfterSuccess cubre FASE
// LG-CMD-2H, tarea 4: tras un comando exitoso, el dispatcher debe disparar
// un refresh de estado puntual (LGCommander.RefreshDeviceState) sin esperar
// al siguiente ciclo de polling normal.
func TestDispatch_PostCommandRefresh_TriggeredAfterSuccess(t *testing.T) {
	dispatcher, lg, _, _, _ := newTestDispatcher(t, nil)
	lg.refreshCalled = make(chan struct{}, 1)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_refresh_1",
		IMEI:         "imei-refresh-1",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-lg.refreshCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected RefreshDeviceState to be triggered after a successful command")
	}
}

// TestDispatch_PostCommandRefresh_ConfirmsWhenStateMatches simula un refresh
// que sí observa el estado esperado (el AC ya cambió físicamente): debe
// confirmar antes de que venza ningún timeout, exactamente el
// comportamiento que faltaba en el bug real reportado en esta fase.
func TestDispatch_PostCommandRefresh_ConfirmsWhenStateMatches(t *testing.T) {
	dispatcher, lg, _, tracking, confirmation := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_refresh_2",
		IMEI:         "imei-refresh-2",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	refreshDone := make(chan struct{})
	lg.onRefresh = func() {
		confirmation.TryConfirm(context.Background(), "imei-refresh-2", CurrentState{Power: true})
		close(refreshDone)
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-refreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the post-command refresh to run")
	}

	ack := tracking.lastAck(t)
	if !ack.OK || ack.CommandID != "cmd_refresh_2" || ack.Detail != AckDetailConfirmedByState {
		t.Fatalf("unexpected ack: %+v", ack)
	}
	if p, err := confirmation.getPending(ctx, "imei-refresh-2"); err != nil || p != nil {
		t.Error("pending confirmation should be cleared once the post-command refresh confirms it")
	}
}

// TestDispatch_PostCommandRefresh_DoesNotFalselyConfirmOnMismatch cubre la
// regla explícita de la fase: "No marcar ACKNOWLEDGED solo porque LG API
// respondió OK" — si el refresh puntual observa un estado que NO coincide
// con lo esperado, no debe confirmar nada; el comando sigue esperando
// push/polling/timeout como antes.
func TestDispatch_PostCommandRefresh_DoesNotFalselyConfirmOnMismatch(t *testing.T) {
	dispatcher, lg, _, tracking, confirmation := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_refresh_3",
		IMEI:         "imei-refresh-3",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	refreshDone := make(chan struct{})
	lg.onRefresh = func() {
		// El refresh todavía observa power=false: LG no reflejó el cambio
		// físico a tiempo. No debe confirmar.
		confirmation.TryConfirm(context.Background(), "imei-refresh-3", CurrentState{Power: false})
		close(refreshDone)
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-refreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the post-command refresh to run")
	}

	if tracking.callCount() != 0 {
		t.Errorf("no ack should be published when the refreshed state does not match expected, got %d calls", tracking.callCount())
	}
	p, err := confirmation.getPending(ctx, "imei-refresh-3")
	if err != nil || p == nil || p.CommandID != "cmd_refresh_3" {
		t.Errorf("pending confirmation should remain when the refresh does not match, got %+v, err=%v", p, err)
	}
}

// TestDispatch_DeviceTimeout2211_PostCommandRefresh_AlsoTriggered cubre la
// tarea 4 del pedido: el refresh post-comando también debe dispararse para
// un 2211 (ambiguo) — puede confirmar antes del timeout sin esperar a que
// LG mande push o al siguiente poll normal.
func TestDispatch_DeviceTimeout2211_PostCommandRefresh_AlsoTriggered(t *testing.T) {
	apiErr := &lgapi.APIError{StatusCode: 400, Code: "2211", Message: "Device Timeout"}
	dispatcher, lg, _, _, _ := newTestDispatcher(t, apiErr)
	lg.refreshCalled = make(chan struct{}, 1)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_refresh_4",
		IMEI:         "imei-refresh-4",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
		Payload:     json.RawMessage(`{"power":false}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-lg.refreshCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected RefreshDeviceState to be triggered even for a 2211 device timeout")
	}
}

// TestDispatch_Oscillation_PowerOffPrecondition_FailsFastWithoutCallingLGAPI
// cubre FASE LG-CMD-2I: lg.oscillation enabled=true con el A/C conocido
// como apagado (power=false) debe rechazarse de inmediato, sin llamar a la
// LG API ni dejar ninguna pendiente de confirmación — evidencia real: LG
// nunca aplica/reporta oscillation=true en ese estado, así que esperar a un
// ack_timeout (90s) es puro desperdicio.
func TestDispatch_Oscillation_PowerOffPrecondition_FailsFastWithoutCallingLGAPI(t *testing.T) {
	dispatcher, lg, status, tracking, confirmation := newTestDispatcher(t, nil)
	lg.knownPower = true
	lg.powerOn = false
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_osc_1",
		IMEI:         "imei-osc-1",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  205, // lg.oscillation
		Payload:     json.RawMessage(`{"enabled":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Errorf("LG API /control should never be called when power is known to be off, got %d calls", lg.callCount())
	}

	sent, failed := status.counts()
	if sent != 0 || failed != 1 {
		t.Errorf("expected 0 sent / 1 failed, got sent=%d failed=%d", sent, failed)
	}
	if status.failed[0].ErrorMessage != AckDetailPreconditionFailedPowerOff {
		t.Errorf("expected publish_failed errorMessage=%q, got %q", AckDetailPreconditionFailedPowerOff, status.failed[0].ErrorMessage)
	}

	ack := tracking.lastAck(t)
	if ack.OK || ack.CommandID != "cmd_osc_1" || ack.Detail != AckDetailPreconditionFailedPowerOff {
		t.Fatalf("unexpected ack: %+v", ack)
	}

	if p, err := confirmation.getPending(ctx, "imei-osc-1"); err != nil || p != nil {
		t.Errorf("no pending confirmation should ever be created for a precondition-failed command, got %+v, err=%v", p, err)
	}
}

// TestDispatch_Oscillation_PowerOn_NormalFlow confirma que la precondición
// no interfiere cuando el A/C sí está encendido: debe seguir el flujo
// normal (LG API llamada, sent publicado, pendiente registrada).
func TestDispatch_Oscillation_PowerOn_NormalFlow(t *testing.T) {
	dispatcher, lg, status, tracking, confirmation := newTestDispatcher(t, nil)
	lg.knownPower = true
	lg.powerOn = true
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_osc_2",
		IMEI:         "imei-osc-2",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  205,
		Payload:     json.RawMessage(`{"enabled":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 1 {
		t.Errorf("expected 1 LG API call when power is known to be on, got %d", lg.callCount())
	}
	sent, failed := status.counts()
	if sent != 1 || failed != 0 {
		t.Errorf("expected 1 sent / 0 failed, got sent=%d failed=%d", sent, failed)
	}
	if tracking.callCount() != 0 {
		t.Error("no ack should be published yet: confirmation is still pending")
	}
	pending, err := confirmation.getPending(ctx, "imei-osc-2")
	if err != nil || pending == nil || pending.CommandID != "cmd_osc_2" {
		t.Fatalf("expected a pending confirmation for cmd_osc_2, got %+v, err=%v", pending, err)
	}
}

// TestDispatch_Oscillation_Disable_NotBlockedByPowerOffPrecondition cubre la
// regla explícita: la precondición solo aplica a enabled=true, nunca a
// enabled=false (apagar la oscilación con el A/C apagado no tiene el mismo
// problema reportado).
func TestDispatch_Oscillation_Disable_NotBlockedByPowerOffPrecondition(t *testing.T) {
	dispatcher, lg, _, _, _ := newTestDispatcher(t, nil)
	lg.knownPower = true
	lg.powerOn = false
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_osc_3",
		IMEI:         "imei-osc-3",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  205,
		Payload:     json.RawMessage(`{"enabled":false}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 1 {
		t.Errorf("lg.oscillation enabled=false should not be blocked by the power-on precondition, got %d calls", lg.callCount())
	}
}

// TestDispatch_Oscillation_UnknownPowerState_NotBlocked cubre la regla
// explícita: si no hay estado conocido todavía (dispositivo nunca leído),
// no se debe bloquear por precondición, para no producir falsos negativos.
func TestDispatch_Oscillation_UnknownPowerState_NotBlocked(t *testing.T) {
	dispatcher, lg, _, _, _ := newTestDispatcher(t, nil)
	// knownPower queda en su zero-value (false): simula que todavía no hay
	// ningún snapshot de estado guardado para este dispositivo.
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_osc_4",
		IMEI:         "imei-osc-4",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  205,
		Payload:     json.RawMessage(`{"enabled":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 1 {
		t.Errorf("without a known power state, the command should not be blocked, got %d calls", lg.callCount())
	}
}

// TestDispatch_OtherCommands_NotAffectedByOscillationPrecondition confirma
// que power/temperature/mode/airflow/powersave nunca se bloquean por esta
// precondición, incluso con el A/C conocido como apagado — solo
// lg.oscillation enabled=true la aplica.
func TestDispatch_OtherCommands_NotAffectedByOscillationPrecondition(t *testing.T) {
	cases := []struct {
		name        string
		commandCode int
		payload     string
	}{
		{"power", 201, `{"power":true}`},
		{"temperature", 202, `{"temperature":24}`},
		{"mode", 203, `{"mode":"COOL"}`},
		{"airflow", 204, `{"strength":"HIGH"}`},
		{"powersave", 206, `{"enabled":true}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dispatcher, lg, _, _, _ := newTestDispatcher(t, nil)
			lg.knownPower = true
			lg.powerOn = false
			ctx := context.Background()

			event := commandsdomain.DeviceCommandEvent{
				CommandID:    "cmd_other_" + tc.name,
				IMEI:         "imei-other-" + tc.name,
				CommandRoute: commandsdomain.CommandRouteVendorCloud,
				CommandCode:  tc.commandCode,
				Payload:      json.RawMessage(tc.payload),
			}

			if err := dispatcher.Dispatch(ctx, event); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if lg.callCount() != 1 {
				t.Errorf("%s should not be blocked by the oscillation power-on precondition, got %d calls", tc.name, lg.callCount())
			}
		})
	}
}

// TestDispatch_RaceFix_StateConfirmedDuringExecution_DoesNotLoseConfirmation
// reproduce exactamente el bug real de FASE LG-CMD-2G: lg.power con
// power=true, el AC encendió físicamente, pero terminaba en
// "synthetic ack published ok=false detail=ack_timeout" porque la
// telemetría/push que confirmaba POWER_ON llegaba (y llamaba a TryConfirm)
// ANTES de que la pendiente de confirmación existiera — se registraba
// después de executeLGCommand, así que ese único TryConfirm que sí
// coincidía se perdía sin encontrar nada que comparar.
//
// Este test simula la carrera: el fake LGCommander llama a
// confirmation.TryConfirm con el estado ya cambiado desde DENTRO de
// SetDevicePower, en el mismo punto temporal donde la llamada HTTP real a
// /control estaría en curso. Con el fix (pending registrado ANTES de
// executeLGCommand), esa llamada a TryConfirm debe encontrar la pendiente y
// confirmar de inmediato — sin esperar ningún poll/push posterior ni el
// ackTimeout.
func TestDispatch_RaceFix_StateConfirmedDuringExecution_DoesNotLoseConfirmation(t *testing.T) {
	dispatcher, lg, _, tracking, confirmation := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_race_1",
		IMEI:         "imei-race-1",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	lg.duringExec = func() {
		confirmation.TryConfirm(ctx, "imei-race-1", CurrentState{Power: true})
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ack := tracking.lastAck(t)
	if !ack.OK || ack.CommandID != "cmd_race_1" || ack.Detail != AckDetailConfirmedByState {
		t.Fatalf("the confirmation observed during LG API execution should not be lost: %+v", ack)
	}

	if p, err := confirmation.getPending(ctx, "imei-race-1"); err != nil || p != nil {
		t.Error("pending confirmation should already be cleared: it was confirmed during execution")
	}
}

// TestDispatch_RaceFix_PendingExistsBeforeLGAPICallReturns confirma
// directamente el orden de operaciones: la pendiente ya debe existir en el
// instante en que se invoca executeLGCommand, no después.
func TestDispatch_RaceFix_PendingExistsBeforeLGAPICallReturns(t *testing.T) {
	dispatcher, lg, _, _, confirmation := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_race_2",
		IMEI:         "imei-race-2",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	var pendingExistedDuringExec bool
	lg.duringExec = func() {
		p, err := confirmation.getPending(ctx, "imei-race-2")
		pendingExistedDuringExec = err == nil && p != nil && p.CommandID == "cmd_race_2"
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !pendingExistedDuringExec {
		t.Error("pending confirmation should already exist while executeLGCommand is running, not only after it returns")
	}
}

func TestDispatch_LGAPIError_PublishesFailedAndAckFalse(t *testing.T) {
	dispatcher, _, status, tracking, confirmation := newTestDispatcher(t, errBoom)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_2",
		IMEI:         "imei-2",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
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

	// FASE LG-CMD-2G: la pendiente se registra de forma optimista ANTES de
	// executeLGCommand — un error definitivo debe eliminarla, no dejarla
	// huérfana esperando una confirmación que nunca llegará.
	if p, err := confirmation.getPending(ctx, "imei-2"); err != nil || p != nil {
		t.Errorf("expected no orphaned pending confirmation after a definitive LG API error, got %+v, err=%v", p, err)
	}
}

func TestDispatch_DeviceDisconnected_DetailIsDeviceDisconnected(t *testing.T) {
	apiErr := &lgapi.APIError{StatusCode: 416, Code: "1222"}
	dispatcher, _, status, tracking, confirmation := newTestDispatcher(t, apiErr)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_3",
		IMEI:         "imei-3",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
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

	// FASE LG-CMD-2G: 1222 (dispositivo desconectado) es un error
	// definitivo — no debe quedar ninguna pendiente huérfana.
	if p, err := confirmation.getPending(ctx, "imei-3"); err != nil || p != nil {
		t.Errorf("expected no orphaned pending confirmation after 1222, got %+v, err=%v", p, err)
	}
}

func TestDispatch_DeviceTimeout2211_DoesNotFailImmediately_RegistersPending(t *testing.T) {
	apiErr := &lgapi.APIError{StatusCode: 400, Code: "2211", Message: "Device Timeout"}
	dispatcher, lg, status, tracking, confirmation := newTestDispatcher(t, apiErr)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_timeout_1",
		IMEI:         "imei-timeout-1",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
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
		CommandID:    "cmd_timeout_2",
		IMEI:         "imei-timeout-2",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
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
		CommandID:    "cmd_timeout_3",
		IMEI:         "imei-timeout-3",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
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
		CommandID:    "cmd_4",
		IMEI:         "imei-4",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
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

	// commandCode 250 is inside the LG-reserved 200-299 range but isn't one
	// of the 6 mapped codes, so it stays unresolvable. With explicit
	// VENDOR_CLOUD routing, ownership is certain: a real unsupported-command
	// case, not an ESP32/foreign event.
	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_5",
		IMEI:         "imei-5",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  250,
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
	// propio consumer group. mqtt-api-service no debe tratarlos como
	// "unsupported" ni publicar device.command.publish_failed + un ACK
	// sintético ok:false — competiría con el device.command.sent legítimo
	// de mqtt-adapter-service para el mismo commandId. Deben ignorarse en
	// silencio: cero llamadas a Kafka o al ACK sintético. Bajo el strict
	// cutover (Corrective Cycle 1 / AC-08) este evento no trae commandRoute,
	// así que se ignora directamente en el ownership gate — no llega a
	// ResolveCommandKey/LooksLikeLGCommand.
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

// ── COMMAND-ROUTING-CONTRACT-1 tests ─────────────────────────────────────────

func TestDispatch_CommandRouteVendorCloud_Executes(t *testing.T) {
	dispatcher, lg, status, _, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_route_1",
		IMEI:         "imei-route-1",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  201,
		Payload:      json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 1 {
		t.Errorf("expected 1 LG API call for explicit VENDOR_CLOUD, got %d", lg.callCount())
	}
	sent, failed := status.counts()
	if sent != 1 || failed != 0 {
		t.Errorf("expected 1 sent / 0 failed, got sent=%d failed=%d", sent, failed)
	}
}

func TestDispatch_CommandRouteDirectDevice_NotExecuted(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	// commandCode 201 is inside the LG-reserved range and would otherwise
	// resolve — explicit DIRECT_DEVICE routing must still block execution,
	// with no fallback to the commandCode/commandType heuristic.
	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_route_2",
		IMEI:         "imei-route-2",
		CommandRoute: commandsdomain.CommandRouteDirectDevice,
		CommandCode:  201,
		Payload:      json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Error("LG API should never be called for commandRoute=DIRECT_DEVICE")
	}
	sent, failed := status.counts()
	if sent != 0 || failed != 0 {
		t.Errorf("expected 0 status calls for commandRoute=DIRECT_DEVICE, got sent=%d failed=%d", sent, failed)
	}
	if tracking.callCount() != 0 {
		t.Error("no synthetic ack should be published for commandRoute=DIRECT_DEVICE")
	}
}

func TestDispatch_CommandRouteUnknown_NotExecuted(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_route_3",
		IMEI:         "imei-route-3",
		CommandRoute: "SOME_UNKNOWN_ROUTE",
		CommandCode:  201,
		Payload:      json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Error("LG API should never be called for an unknown commandRoute")
	}
	sent, failed := status.counts()
	if sent != 0 || failed != 0 {
		t.Errorf("expected 0 status calls for an unknown commandRoute, got sent=%d failed=%d", sent, failed)
	}
	if tracking.callCount() != 0 {
		t.Error("no synthetic ack should be published for an unknown commandRoute")
	}
}

func TestDispatch_CommandRouteVendorCloud_UnresolvedCommandKey_PublishesFailedAndAck(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	// commandCode 999 has no LG mapping and no metadata/commandKey. With no
	// explicit route, this would be silently ignored (foreign/ESP32-shaped).
	// With explicit VENDOR_CLOUD routing, ownership is certain, so it must
	// surface as a real unsupported-command failure instead.
	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_route_4",
		IMEI:         "imei-route-4",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  999,
		Payload:      json.RawMessage(`{}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Error("LG API should never be called for an unresolvable commandKey")
	}
	_, failed := status.counts()
	if failed != 1 {
		t.Errorf("expected 1 failed event for explicit VENDOR_CLOUD with unresolvable commandKey, got %d", failed)
	}
	ack := tracking.lastAck(t)
	if ack.OK || ack.Detail != AckDetailUnsupportedCommand {
		t.Errorf("unexpected ack: %+v", ack)
	}
}

// COMMAND-ROUTING-CONTRACT-1 Corrective Cycle 1 (AC-08): the producer-first
// runtime gate is satisfied, so the legacy missing-route LooksLikeLGCommand
// compatibility fallback is removed. Missing commandRoute must now be
// non-executable exactly like an unknown/opposite route — even for a
// commandCode within the LG-reserved numeric range that the old heuristic
// would have recognized.
func TestDispatch_CommandRouteMissing_NotExecuted(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:   "cmd_route_5",
		IMEI:        "imei-route-5",
		CommandCode: 201,
		Payload:     json.RawMessage(`{"power":true}`),
	}

	if err := dispatcher.Dispatch(ctx, event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if lg.callCount() != 0 {
		t.Errorf("expected 0 LG API calls for missing commandRoute (strict cutover), got %d", lg.callCount())
	}
	sent, failed := status.counts()
	if sent != 0 || failed != 0 {
		t.Errorf("expected 0 status calls for missing commandRoute, got sent=%d failed=%d", sent, failed)
	}
	if tracking.callCount() != 0 {
		t.Error("no synthetic ack should be published for missing commandRoute")
	}
}

func TestDispatch_InvalidPayload_PublishesFailedAndAck(t *testing.T) {
	dispatcher, lg, status, tracking, _ := newTestDispatcher(t, nil)
	ctx := context.Background()

	event := commandsdomain.DeviceCommandEvent{
		CommandID:    "cmd_6",
		IMEI:         "imei-6",
		CommandRoute: commandsdomain.CommandRouteVendorCloud,
		CommandCode:  202, // lg.temperature.set
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
