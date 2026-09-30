package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"mqtt-api-service/internal/infrastructure/config"
)

// rawClientID simula el runtime observado: LG_MQTT_CLIENT_ID igual a
// LG_CLIENT_ID, así que también aparece en los topics.
const rawClientID = "raw-transport-id-0f3c9a"

type fakeToken struct{ err error }

func (t fakeToken) Wait() bool                     { return true }
func (t fakeToken) WaitTimeout(time.Duration) bool { return true }
func (t fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (t fakeToken) Error() error { return t.err }

type subscribeCall struct {
	topic string
	qos   byte
}

type fakePaho struct {
	mu sync.Mutex

	opts *paho.ClientOptions

	connectErrs  []error // se consumen en orden; vacío = éxito
	connectCalls int
	subscribeErr map[string]error
	subscribes   []subscribeCall
	connected    bool

	disconnectCalls int
}

func (f *fakePaho) Connect() paho.Token {
	f.mu.Lock()
	f.connectCalls++
	var err error
	if len(f.connectErrs) > 0 {
		err = f.connectErrs[0]
		f.connectErrs = f.connectErrs[1:]
	}
	if err == nil {
		f.connected = true
	}
	onConnect := f.opts.OnConnect
	f.mu.Unlock()

	if err == nil && onConnect != nil {
		onConnect(nil)
	}
	return fakeToken{err: err}
}

func (f *fakePaho) Subscribe(topic string, qos byte, _ paho.MessageHandler) paho.Token {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribes = append(f.subscribes, subscribeCall{topic: topic, qos: qos})
	return fakeToken{err: f.subscribeErr[topic]}
}

func (f *fakePaho) Publish(string, byte, bool, interface{}) paho.Token { return fakeToken{} }

func (f *fakePaho) Disconnect(uint) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnectCalls++
	f.connected = false
}

func (f *fakePaho) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

// loseConnection simula al broker cerrando la conexión (EOF).
func (f *fakePaho) loseConnection(err error) {
	f.mu.Lock()
	f.connected = false
	handler := f.opts.OnConnectionLost
	f.mu.Unlock()
	handler(nil, err)
}

func (f *fakePaho) snapshot() (connects int, subs []subscribeCall, disconnects int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connectCalls, append([]subscribeCall(nil), f.subscribes...), f.disconnectCalls
}

type harness struct {
	c      *client
	fake   *fakePaho
	logs   *observer.ObservedLogs
	mu     sync.Mutex
	delays []time.Duration
	clock  time.Time

	// holdTimers hace que los retrasos del backoff nunca venzan, para
	// probar que Disconnect cancela un reintento pendiente.
	holdTimers bool
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = h.clock.Add(d)
}

func (h *harness) recordedDelays() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.delays...)
}

func newHarness(t *testing.T, fake *fakePaho) *harness {
	t.Helper()

	core, logs := observer.New(zapcore.DebugLevel)

	cfg := config.Config{}
	cfg.MQTT.Endpoint = "mqtts://broker.invalid:8883"
	cfg.MQTT.ClientID = rawClientID
	cfg.MQTT.KeepAlive = 60
	cfg.LG.ClientID = rawClientID

	noop := func(context.Context, string, []byte) error { return nil }
	subs := []Subscription{
		{Topic: "app/clients/" + rawClientID + "/push", QoS: 1, Handler: noop},
		{Topic: "app/clients/" + rawClientID + "/inbox", QoS: 1, Handler: noop},
	}

	h := &harness{fake: fake, logs: logs, clock: time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)}

	c := newClient(cfg, zap.New(core), subs, testPolicy)
	c.buildTLS = func() (*tls.Config, error) { return &tls.Config{}, nil }
	c.newPaho = func(opts *paho.ClientOptions) pahoClient {
		fake.mu.Lock()
		fake.opts = opts
		fake.mu.Unlock()
		return fake
	}
	c.after = func(d time.Duration) <-chan time.Time {
		h.mu.Lock()
		h.delays = append(h.delays, d)
		hold := h.holdTimers
		h.mu.Unlock()
		ch := make(chan time.Time, 1)
		if !hold {
			ch <- time.Time{}
		}
		return ch
	}
	c.now = func() time.Time {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.clock
	}

	h.c = c
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	return h
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

var wantSubs = []subscribeCall{
	{topic: "app/clients/" + rawClientID + "/push", qos: 1},
	{topic: "app/clients/" + rawClientID + "/inbox", qos: 1},
}

func assertSubs(t *testing.T, got []subscribeCall, rounds int) {
	t.Helper()
	if len(got) != rounds*len(wantSubs) {
		t.Fatalf("got %d subscribe calls, want %d: %+v", len(got), rounds*len(wantSubs), got)
	}
	for i, s := range got {
		if want := wantSubs[i%len(wantSubs)]; s != want {
			t.Fatalf("subscribe[%d] = %+v, want %+v", i, s, want)
		}
	}
}

func TestConnect_OptionsDisablePahoReconnectAndKeepCleanSession(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	o := fake.opts
	if o.ClientID != rawClientID {
		t.Fatal("client id must be LG_MQTT_CLIENT_ID exactly")
	}
	if !o.CleanSession || o.AutoReconnect || o.ConnectRetry {
		t.Fatalf("CleanSession=%v AutoReconnect=%v ConnectRetry=%v", o.CleanSession, o.AutoReconnect, o.ConnectRetry)
	}
	if o.KeepAlive != 60 || o.ConnectTimeout != connectTimeout {
		t.Fatalf("KeepAlive=%d ConnectTimeout=%s", o.KeepAlive, o.ConnectTimeout)
	}
}

func TestConnect_InitialSubscribeExactlyOnce(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	connects, subs, _ := fake.snapshot()
	if connects != 1 {
		t.Fatalf("connect calls = %d, want 1", connects)
	}
	assertSubs(t, subs, 1)
}

func TestReconnect_ResubscribesAfterEveryReconnect(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	for round := 2; round <= 4; round++ {
		h.advance(10 * time.Minute) // conexión estable
		fake.loseConnection(io.EOF)
		waitFor(t, "reconnect", func() bool {
			c, _, _ := fake.snapshot()
			return c == round && h.c.IsConnected()
		})
		_, subs, _ := fake.snapshot()
		assertSubs(t, subs, round)
	}

	for i, d := range h.recordedDelays() {
		if d != testPolicy.InitialDelay {
			t.Fatalf("stable reconnect %d delay = %s, want %s", i, d, testPolicy.InitialDelay)
		}
	}
}

func TestReconnect_RapidChurnEscalatesBackoffAndFlagsPossibleCollision(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	const cycles = 12
	for i := 1; i <= cycles; i++ {
		h.advance(3 * time.Second) // cada conexión dura menos que StableConnection
		fake.loseConnection(io.EOF)
		waitFor(t, "reconnect", func() bool {
			c, _, _ := fake.snapshot()
			return c == i+1
		})
	}

	delays := h.recordedDelays()
	if len(delays) != cycles {
		t.Fatalf("got %d reconnect waits, want %d (one per loss, no extra attempts)", len(delays), cycles)
	}
	for i, d := range delays {
		want := testPolicy.InitialDelay << i
		if want > testPolicy.MaxDelay {
			want = testPolicy.MaxDelay
		}
		if d != want {
			t.Fatalf("delay[%d] = %s, want %s (all: %v)", i, d, want, delays)
		}
	}

	warn := h.logs.FilterMessageSnippet("possible duplicate LG_MQTT_CLIENT_ID").All()
	if want := cycles - testPolicy.InstabilityThreshold + 1; len(warn) != want {
		t.Fatalf("instability diagnostics = %d, want %d", len(warn), want)
	}
	if !strings.Contains(warn[0].Message, "heuristic, not proven") {
		t.Fatalf("collision diagnostic must be labelled heuristic: %q", warn[0].Message)
	}
}

func TestReconnect_FailedAttemptsBackOff(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	fake.mu.Lock()
	fake.connectErrs = []error{errors.New("dial failed"), errors.New("dial failed"), errors.New("dial failed")}
	fake.mu.Unlock()

	h.advance(10 * time.Minute)
	fake.loseConnection(io.EOF)
	waitFor(t, "reconnect after failures", func() bool {
		c, _, _ := fake.snapshot()
		return c == 5 && h.c.IsConnected()
	})

	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	got := h.recordedDelays()
	if len(got) != len(want) {
		t.Fatalf("delays = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delays = %v, want %v", got, want)
		}
	}
	if n := len(h.logs.FilterMessage("LG push MQTT reconnect attempt failed").All()); n != 3 {
		t.Fatalf("failed-attempt diagnostics = %d, want 3", n)
	}
	_, subs, _ := fake.snapshot()
	assertSubs(t, subs, 2)
}

func TestDisconnect_GracefulOnceAndIdempotent(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := h.c.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := h.c.Disconnect(context.Background()); err != nil {
		t.Fatalf("second Disconnect: %v", err)
	}

	connects, _, disconnects := fake.snapshot()
	if disconnects != 1 {
		t.Fatalf("paho Disconnect calls = %d, want exactly 1", disconnects)
	}

	// Una pérdida reportada después del shutdown no debe reconectar.
	fake.loseConnection(io.EOF)
	time.Sleep(20 * time.Millisecond)
	if c, _, _ := fake.snapshot(); c != connects {
		t.Fatalf("reconnect after shutdown: connect calls %d -> %d", connects, c)
	}
	if len(h.logs.FilterMessage("LG push MQTT disconnected gracefully").All()) != 1 {
		t.Fatal("missing graceful disconnect diagnostic")
	}
}

func TestDisconnect_SafeWhenNeverConnected(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)

	if err := h.c.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if h.c.IsConnected() {
		t.Fatal("must not report connected")
	}
	if _, _, d := fake.snapshot(); d != 0 {
		t.Fatalf("paho Disconnect calls = %d, want 0", d)
	}
}

// Un fallo del primer connect (EOF del broker) no es fatal: Connect devuelve
// nil y el mismo supervisor reintenta con el backoff acotado hasta conectar.
func TestConnect_InitialFailureIsNonFatalAndRetriesWithBackoff(t *testing.T) {
	fake := &fakePaho{connectErrs: []error{io.EOF, io.EOF, io.EOF}}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("initial connect failure must not be returned as an error: %v", err)
	}

	waitFor(t, "connect after initial failures", func() bool {
		c, _, _ := fake.snapshot()
		return c == 4 && h.c.IsConnected()
	})

	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	got := h.recordedDelays()
	if len(got) != len(want) {
		t.Fatalf("delays = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delays = %v, want %v", got, want)
		}
	}

	if n := len(h.logs.FilterMessage("LG push MQTT initial connect failed").All()); n != 1 {
		t.Fatalf("initial connect failure diagnostics = %d, want 1", n)
	}
	if n := len(h.logs.FilterMessage("LG push MQTT reconnect scheduled").All()); n != 3 {
		t.Fatalf("reconnect scheduled diagnostics = %d, want 3", n)
	}
	if n := len(h.logs.FilterMessage("LG push MQTT reconnect attempt failed").All()); n != 2 {
		t.Fatalf("reconnect attempt failed diagnostics = %d, want 2", n)
	}

	// Suscripciones solo tras la conexión exitosa, una vez.
	_, subs, _ := fake.snapshot()
	assertSubs(t, subs, 1)
}

// El backoff del fallo inicial y el de las pérdidas posteriores es el
// mismo: una conexión de vida corta tras el fallo inicial no lo reinicia.
func TestConnect_InitialFailureSharesBackoffWithLaterLosses(t *testing.T) {
	fake := &fakePaho{connectErrs: []error{io.EOF}}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitFor(t, "first successful connect", func() bool {
		c, _, _ := fake.snapshot()
		return c == 2 && h.c.IsConnected()
	})

	h.advance(3 * time.Second) // conexión de vida corta
	fake.loseConnection(io.EOF)
	waitFor(t, "reconnect", func() bool {
		c, _, _ := fake.snapshot()
		return c == 3 && h.c.IsConnected()
	})

	got := h.recordedDelays()
	want := []time.Duration{2 * time.Second, 4 * time.Second}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("delays = %v, want %v (single shared backoff)", got, want)
	}
}

// SIGINT/SIGTERM durante un reintento pendiente tras un fallo inicial:
// Disconnect cancela la espera, el supervisor termina y no hay más intentos.
func TestDisconnect_CancelsPendingRetryAfterInitialFailure(t *testing.T) {
	fake := &fakePaho{connectErrs: []error{io.EOF}}
	h := newHarness(t, fake)
	h.holdTimers = true

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitFor(t, "retry scheduled", func() bool { return len(h.recordedDelays()) == 1 })

	start := time.Now()
	if err := h.c.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= disconnectTimeout {
		t.Fatalf("Disconnect took %s; pending retry was not cancelled", elapsed)
	}

	select {
	case <-h.c.done:
	default:
		t.Fatal("supervisor must have exited after Disconnect")
	}

	time.Sleep(20 * time.Millisecond)
	if c, subs, d := fake.snapshot(); c != 1 || len(subs) != 0 || d != 0 {
		t.Fatalf("after shutdown: connects=%d subs=%d disconnects=%d, want 1/0/0", c, len(subs), d)
	}
	if len(h.recordedDelays()) != 1 {
		t.Fatalf("no retry may be scheduled after shutdown, delays=%v", h.recordedDelays())
	}
	if len(h.logs.FilterMessage("LG push MQTT stopped (no active connection)").All()) != 1 {
		t.Fatal("missing stopped diagnostic")
	}
}

func TestConnect_TLSFailureDoesNotConstructClient(t *testing.T) {
	fake := &fakePaho{}
	h := newHarness(t, fake)
	h.c.buildTLS = func() (*tls.Config, error) { return nil, errors.New("CA error") }

	if err := h.c.Connect(context.Background()); err == nil {
		t.Fatal("expected TLS error")
	}
	if fake.opts != nil {
		t.Fatal("paho client must not be constructed when TLS fails")
	}
}

func TestDiagnostics_NeverContainRawClientID(t *testing.T) {
	fake := &fakePaho{subscribeErr: map[string]error{
		"app/clients/" + rawClientID + "/inbox": errors.New("not authorized"),
	}}
	h := newHarness(t, fake)

	if err := h.c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	for i := 1; i <= testPolicy.InstabilityThreshold+1; i++ {
		h.advance(time.Second)
		fake.loseConnection(io.EOF)
		waitFor(t, "reconnect", func() bool {
			c, _, _ := fake.snapshot()
			return c == i+1
		})
	}
	if err := h.c.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	fp := ClientIDFingerprint(rawClientID)
	required := []string{
		"LG push MQTT connecting",
		"LG push MQTT connected",
		"LG push MQTT subscriptions installed",
		"LG push MQTT subscribe failed",
		"LG push MQTT connection lost",
		"LG push MQTT reconnect scheduled",
		"possible duplicate LG_MQTT_CLIENT_ID",
		"LG push MQTT disconnected gracefully",
	}
	for _, msg := range required {
		if len(h.logs.FilterMessageSnippet(msg).All()) == 0 {
			t.Errorf("missing diagnostic %q", msg)
		}
	}

	sawFingerprint := false
	for _, entry := range h.logs.All() {
		if strings.Contains(entry.Message, rawClientID) {
			t.Errorf("raw client id in message %q", entry.Message)
		}
		for k, v := range entry.ContextMap() {
			s, _ := v.(string)
			if strings.Contains(s, rawClientID) {
				t.Errorf("raw client id in field %q of %q", k, entry.Message)
			}
			if s == fp {
				sawFingerprint = true
			}
		}
	}
	if !sawFingerprint {
		t.Error("expected client id fingerprint in diagnostics")
	}
}

func TestClientIDFingerprintAndRedactTopic(t *testing.T) {
	fp := ClientIDFingerprint(rawClientID)
	if len(fp) != 12 || strings.Contains(fp, rawClientID) || fp != ClientIDFingerprint(rawClientID) {
		t.Fatalf("fingerprint %q must be stable 12-hex and not contain the raw id", fp)
	}
	if fp == ClientIDFingerprint(rawClientID+"x") {
		t.Fatal("different ids must have different fingerprints")
	}

	got := RedactTopic("app/clients/" + rawClientID + "/push")
	if got != "app/clients/fp:"+fp+"/push" {
		t.Fatalf("RedactTopic = %q", got)
	}
	if other := RedactTopic("devices/123/ack"); other != "devices/123/ack" {
		t.Fatalf("non-LG topic must be unchanged, got %q", other)
	}
}
