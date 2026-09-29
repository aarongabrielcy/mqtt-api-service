package parser

import (
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"mqtt-api-service/internal/adapters/mqtt"
)

// sentinelClientID simula el runtime validado, donde LG_CLIENT_ID =
// LG_MQTT_CLIENT_ID y por lo tanto el topic push contiene el Client ID MQTT.
const sentinelClientID = "sentinel-mqtt-client-id-7d41"

func TestLGPushParser_ParseErrorLogRedactsClientID(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	p := NewLGPushParser(zap.New(core))

	topic := "app/clients/" + sentinelClientID + "/push"
	if _, err := p.Parse(topic, []byte("{not json")); err == nil {
		t.Fatal("expected parse error for invalid payload")
	}

	entries := logs.FilterMessage("error parseando push message de LG").All()
	if len(entries) != 1 {
		t.Fatalf("parse-error log entries = %d, want 1", len(entries))
	}

	fields := entries[0].ContextMap()
	if got, want := fields["topic"], mqtt.RedactTopic(topic); got != want {
		t.Fatalf("topic field = %v, want redacted %q", got, want)
	}
	if _, ok := fields["error"]; !ok {
		t.Fatal("parse-error log must keep the error classification")
	}

	for _, entry := range logs.All() {
		if strings.Contains(entry.Message, sentinelClientID) {
			t.Errorf("raw client id in message %q", entry.Message)
		}
		for k, v := range entry.ContextMap() {
			s, _ := v.(string)
			if strings.Contains(s, sentinelClientID) || strings.Contains(s, topic) {
				t.Errorf("raw client id/topic in field %q", k)
			}
		}
	}
}
