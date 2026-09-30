package lg_service

import (
	"context"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"mqtt-api-service/internal/adapters/mqtt"
	"mqtt-api-service/internal/adapters/parser"
)

// sentinelClientID simula el runtime validado, donde LG_CLIENT_ID =
// LG_MQTT_CLIENT_ID y el topic push contiene el Client ID MQTT real.
const sentinelClientID = "sentinel-mqtt-client-id-7d41"

// Los errores de parseo del camino push (payload inválido o sin deviceId)
// nunca deben loguear el topic crudo ni el Client ID (FR-07 / AC-08).
func TestHandlePushMessage_ParseErrorLogsRedactTopic(t *testing.T) {
	topic := "app/clients/" + sentinelClientID + "/push"

	cases := map[string][]byte{
		"invalid json":     []byte("{not json"),
		"missing deviceId": []byte(`{"pushType":"DEVICE_STATUS","report":{"operation":{}}}`),
	}

	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			core, logs := observer.New(zapcore.DebugLevel)
			log := zap.New(core)
			s := &LGService{log: log, pushParser: parser.NewLGPushParser(log)}

			if err := s.HandlePushMessage(context.Background(), topic, payload); err == nil {
				t.Fatal("expected parse error")
			}

			entries := logs.FilterMessage("failed to parse push message").All()
			if len(entries) != 1 {
				t.Fatalf("parse-failure log entries = %d, want 1", len(entries))
			}
			fields := entries[0].ContextMap()
			if got, want := fields["topic"], mqtt.RedactTopic(topic); got != want {
				t.Fatalf("topic field = %v, want redacted %q", got, want)
			}
			if _, ok := fields["error"]; !ok {
				t.Fatal("parse-failure log must keep the error")
			}

			for _, entry := range logs.All() {
				if strings.Contains(entry.Message, sentinelClientID) {
					t.Errorf("raw client id in message %q", entry.Message)
				}
				for k, v := range entry.ContextMap() {
					s, _ := v.(string)
					if strings.Contains(s, sentinelClientID) || strings.Contains(s, topic) {
						t.Errorf("raw client id/topic in field %q of %q", k, entry.Message)
					}
				}
			}
		})
	}
}
