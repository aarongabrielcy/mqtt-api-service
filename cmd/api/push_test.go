package main

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"mqtt-api-service/internal/adapters/mqtt"
	"mqtt-api-service/internal/infrastructure/config"
)

type fakePushClient struct {
	connectErr      error
	connectCalls    int
	disconnectCalls int
}

func (f *fakePushClient) Connect(context.Context) error {
	f.connectCalls++
	return f.connectErr
}
func (f *fakePushClient) Publish(context.Context, string, []byte) error { return nil }
func (f *fakePushClient) Disconnect(context.Context) error {
	f.disconnectCalls++
	return nil
}
func (f *fakePushClient) IsConnected() bool { return f.connectCalls > 0 && f.connectErr == nil }

func testPushConfig(enabled bool) *config.Config {
	cfg := &config.Config{}
	cfg.App.Environment = "local"
	cfg.LG.ClientID = "lg-app-client"
	cfg.LG.PushEnabled = enabled
	cfg.MQTT.ClientID = "transport-id"
	return cfg
}

func noopHandler(context.Context, string, []byte) error { return nil }

func TestStartPushTransport_DisabledNeverConstructsOrConnects(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	cfg := testPushConfig(false)

	factory := func(config.Config, *zap.Logger, []mqtt.Subscription) (mqtt.Client, error) {
		t.Fatal("vendor MQTT client must not be constructed when push is disabled")
		return nil, nil
	}

	client := startPushTransport(context.Background(), cfg, zap.New(core), factory,
		pushSubscriptions(cfg, noopHandler, noopHandler))
	if client != nil {
		t.Fatalf("got client=%v, want nil", client)
	}
	if len(logs.FilterMessage("LG push MQTT disabled by policy").All()) != 1 {
		t.Fatal("missing push-disabled-by-policy diagnostic")
	}

	// El shutdown con push deshabilitado es un no-op seguro.
	stopPushTransport(context.Background(), client, zap.New(core))
}

func TestStartPushTransport_EnabledConnectsWithRequiredSubscriptions(t *testing.T) {
	cfg := testPushConfig(true)
	fake := &fakePushClient{}

	var gotSubs []mqtt.Subscription
	factory := func(_ config.Config, _ *zap.Logger, subs []mqtt.Subscription) (mqtt.Client, error) {
		gotSubs = subs
		return fake, nil
	}

	client := startPushTransport(context.Background(), cfg, zap.NewNop(), factory,
		pushSubscriptions(cfg, noopHandler, noopHandler))
	if client == nil {
		t.Fatal("expected a push client")
	}
	if fake.connectCalls != 1 {
		t.Fatalf("connect calls = %d, want 1", fake.connectCalls)
	}

	want := []struct {
		topic string
		qos   byte
	}{
		{"app/clients/lg-app-client/push", 1},
		{"app/clients/lg-app-client/inbox", 1},
	}
	if len(gotSubs) != len(want) {
		t.Fatalf("subscriptions = %d, want %d", len(gotSubs), len(want))
	}
	for i, w := range want {
		if gotSubs[i].Topic != w.topic || gotSubs[i].QoS != w.qos || gotSubs[i].Handler == nil {
			t.Fatalf("subscription[%d] = %q qos=%d", i, gotSubs[i].Topic, gotSubs[i].QoS)
		}
	}

	stopPushTransport(context.Background(), client, zap.NewNop())
	if fake.disconnectCalls != 1 {
		t.Fatalf("disconnect calls = %d, want 1", fake.disconnectCalls)
	}
}

// Un error de preparación del push (p. ej. TLS) nunca es fatal: se loguea y
// el servicio sigue sin push.
func TestStartPushTransport_SetupErrorIsNonFatal(t *testing.T) {
	cfg := testPushConfig(true)

	for name, factory := range map[string]pushClientFactory{
		"connect setup error": func(config.Config, *zap.Logger, []mqtt.Subscription) (mqtt.Client, error) {
			return &fakePushClient{connectErr: errors.New("CA error")}, nil
		},
		"factory error": func(config.Config, *zap.Logger, []mqtt.Subscription) (mqtt.Client, error) {
			return nil, errors.New("factory failed")
		},
	} {
		t.Run(name, func(t *testing.T) {
			core, logs := observer.New(zapcore.InfoLevel)

			client := startPushTransport(context.Background(), cfg, zap.New(core), factory,
				pushSubscriptions(cfg, noopHandler, noopHandler))
			if client != nil {
				t.Fatalf("got client=%v, want nil", client)
			}
			if len(logs.FilterMessage("LG push MQTT unavailable; continuing without push").All()) != 1 {
				t.Fatal("missing non-fatal push-unavailable diagnostic")
			}
			stopPushTransport(context.Background(), client, zap.New(core))
		})
	}
}
