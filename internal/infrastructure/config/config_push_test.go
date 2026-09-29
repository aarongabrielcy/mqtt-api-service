package config

import (
	"strings"
	"testing"
)

func loadPushConfig(t *testing.T, appEnv, pushEnabled string) *Config {
	t.Helper()
	clearEcosystemEnv(t)
	t.Setenv("APP_ENV", appEnv)
	t.Setenv("LG_PUSH_ENABLED", pushEnabled)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return cfg
}

func TestLoadConfig_LGPushEnabled(t *testing.T) {
	cases := []struct {
		name         string
		appEnv       string
		pushEnabled  string
		wantEnabled  bool
		wantExplicit bool
	}{
		{"APP_ENV unset defaults to local -> OFF", "", "", false, false},
		{"local unset -> OFF", "local", "", false, false},
		{"LOCAL mixed case unset -> OFF", " Local ", "", false, false},
		{"local explicit true -> ON", "local", "true", true, true},
		{"local explicit false -> OFF", "local", "false", false, true},
		{"sandbox unset keeps pre-task enabled contract", "sandbox", "", true, false},
		{"production unset keeps pre-task enabled contract", "production", "", true, false},
		{"sandbox explicit false -> OFF", "sandbox", "false", false, true},
		{"sandbox explicit true -> ON", "sandbox", "true", true, true},
		{"invalid value falls back to OFF", "sandbox", "maybe", false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadPushConfig(t, tc.appEnv, tc.pushEnabled)
			if cfg.LG.PushEnabled != tc.wantEnabled {
				t.Errorf("LG.PushEnabled = %v, want %v", cfg.LG.PushEnabled, tc.wantEnabled)
			}
			if cfg.LG.PushEnabledExplicit != tc.wantExplicit {
				t.Errorf("LG.PushEnabledExplicit = %v, want %v", cfg.LG.PushEnabledExplicit, tc.wantExplicit)
			}
		})
	}
}

func TestValidatePush_EnabledRequiresDedicatedMQTTClientID(t *testing.T) {
	for _, mqttID := range []string{"", "   "} {
		loadPushConfig(t, "sandbox", "true")
		t.Setenv("LG_CLIENT_ID", "lg-app-client-value")
		t.Setenv("LG_API_CLIENT_ID", "lg-api-client-value")
		t.Setenv("LG_MQTT_CLIENT_ID", mqttID)

		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = cfg.ValidatePush()
		if err == nil {
			t.Fatalf("LG_MQTT_CLIENT_ID=%q with push enabled must fail", mqttID)
		}
		if !strings.Contains(err.Error(), "LG_MQTT_CLIENT_ID") {
			t.Errorf("error must name the missing key: %v", err)
		}
		if strings.Contains(err.Error(), "lg-app-client-value") || strings.Contains(err.Error(), "lg-api-client-value") {
			t.Errorf("error must not contain identifier values: %v", err)
		}
		// Sin fallback: el Client ID MQTT no toma LG_CLIENT_ID/LG_API_CLIENT_ID.
		if cfg.MQTT.ClientID != mqttID {
			t.Errorf("MQTT.ClientID = %q, want the configured value %q (no fallback)", cfg.MQTT.ClientID, mqttID)
		}
	}
}

func TestValidatePush_OKWhenEnabledWithIDOrDisabled(t *testing.T) {
	cfg := loadPushConfig(t, "sandbox", "true")
	cfg.MQTT.ClientID = "dedicated-transport-id"
	if err := cfg.ValidatePush(); err != nil {
		t.Fatalf("enabled with id: %v", err)
	}

	cfg = loadPushConfig(t, "local", "")
	cfg.MQTT.ClientID = ""
	if err := cfg.ValidatePush(); err != nil {
		t.Fatalf("disabled push must not require LG_MQTT_CLIENT_ID: %v", err)
	}
}

func TestLoadConfig_MQTTClientIDIsStableAcrossLoads(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_MQTT_CLIENT_ID", "stable-transport-id")

	first, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first.MQTT.ClientID != "stable-transport-id" || second.MQTT.ClientID != first.MQTT.ClientID {
		t.Fatalf("MQTT.ClientID must be exactly the configured value on every load (no per-start generation)")
	}
}

func TestIsLocalEnvironment(t *testing.T) {
	for env, want := range map[string]bool{
		"local": true, "LOCAL": true, " local ": true,
		"sandbox": false, "production": false, "": false, "localhost": false,
	} {
		if got := IsLocalEnvironment(env); got != want {
			t.Errorf("IsLocalEnvironment(%q) = %v, want %v", env, got, want)
		}
	}
}
