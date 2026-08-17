package config

import (
	"testing"
	"time"
)

// clearEcosystemEnv fuerza a "" las variables con default esperado, para
// que LoadConfig las trate como no definidas (getEnv trata "" como unset).
func clearEcosystemEnv(t *testing.T) {
	for _, key := range []string{
		"REDIS_ADDR",
		"MONGO_DB_NAME",
		"MONGO_COLLECTION_NAME",
		"MONGO_URI",
		"TRACKING_PLATFORM_GRPC_ADDRESS",
		"DEVICE_CONTROL_GRPC_ADDRESS",
		"APP_ENV",
		"LOG_LEVEL",
		"COUNTRY_CODE",
		"LANGUAGE_CODE",
		"SERVICE_PHASE",
		"TRACKING_GRPC_REQUEST_TIMEOUT_SECONDS",
		"TRACKING_GRPC_MAX_ATTEMPTS",
		"TRACKING_GRPC_RETRY_INITIAL_BACKOFF_MS",
		"TRACKING_GRPC_RETRY_MAX_BACKOFF_MS",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadConfig_DefaultsWhenEnvUnset(t *testing.T) {
	clearEcosystemEnv(t)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"Redis.Addr", cfg.Redis.Addr, "redis:6379"},
		{"Mongo.DBName", cfg.Mongo.DBName, "mqtt-api-service"},
		{"Mongo.CollectionName", cfg.Mongo.CollectionName, "raw_lg_messages"},
		{"Mongo.URI", cfg.Mongo.URI, "mongodb://mongo:27017"},
		{"GRPC.Address", cfg.GRPC.Address, "ingestion-service:50051"},
		{"DeviceControlGRPC.Address", cfg.DeviceControlGRPC.Address, ":50052"},
		{"App.Environment", cfg.App.Environment, "local"},
		{"App.LogLevel", cfg.App.LogLevel, "info"},
		{"LGApi.CountryCode", cfg.LGApi.CountryCode, "MX"},
		{"LGApi.LanguageCode", cfg.LGApi.LanguageCode, "es-MX"},
		{"LGApi.ServicePhase", cfg.LGApi.ServicePhase, "OP"},
	}

	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}

	if cfg.GRPC.RequestTimeout != 10*time.Second {
		t.Errorf("GRPC.RequestTimeout = %v, want 10s", cfg.GRPC.RequestTimeout)
	}
	if cfg.GRPC.MaxAttempts != 3 {
		t.Errorf("GRPC.MaxAttempts = %v, want 3", cfg.GRPC.MaxAttempts)
	}
	if cfg.GRPC.RetryInitialBackoff != 1000*time.Millisecond {
		t.Errorf("GRPC.RetryInitialBackoff = %v, want 1000ms", cfg.GRPC.RetryInitialBackoff)
	}
	if cfg.GRPC.RetryMaxBackoff != 4000*time.Millisecond {
		t.Errorf("GRPC.RetryMaxBackoff = %v, want 4000ms", cfg.GRPC.RetryMaxBackoff)
	}
}

func TestLoadConfig_ReadsFromEnv(t *testing.T) {
	clearEcosystemEnv(t)

	t.Setenv("REDIS_ADDR", "custom-redis:1234")
	t.Setenv("MONGO_DB_NAME", "custom-db")
	t.Setenv("MONGO_COLLECTION_NAME", "custom-collection")
	t.Setenv("TRACKING_PLATFORM_GRPC_ADDRESS", "custom-ingestion:9999")
	t.Setenv("DEVICE_CONTROL_GRPC_ADDRESS", ":9090")
	t.Setenv("LG_CLIENT_ID", "test-lg-client-id")
	t.Setenv("LG_MQTT_CLIENT_ID", "test-mqtt-client-id")
	t.Setenv("LG_API_CLIENT_ID", "test-api-client-id")
	t.Setenv("TRACKING_GRPC_REQUEST_TIMEOUT_SECONDS", "20")
	t.Setenv("TRACKING_GRPC_MAX_ATTEMPTS", "5")
	t.Setenv("TRACKING_GRPC_RETRY_INITIAL_BACKOFF_MS", "500")
	t.Setenv("TRACKING_GRPC_RETRY_MAX_BACKOFF_MS", "8000")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Redis.Addr != "custom-redis:1234" {
		t.Errorf("Redis.Addr = %q, want custom-redis:1234", cfg.Redis.Addr)
	}
	if cfg.Mongo.DBName != "custom-db" {
		t.Errorf("Mongo.DBName = %q, want custom-db", cfg.Mongo.DBName)
	}
	if cfg.Mongo.CollectionName != "custom-collection" {
		t.Errorf("Mongo.CollectionName = %q, want custom-collection", cfg.Mongo.CollectionName)
	}
	if cfg.GRPC.Address != "custom-ingestion:9999" {
		t.Errorf("GRPC.Address = %q, want custom-ingestion:9999", cfg.GRPC.Address)
	}
	if cfg.DeviceControlGRPC.Address != ":9090" {
		t.Errorf("DeviceControlGRPC.Address = %q, want :9090", cfg.DeviceControlGRPC.Address)
	}

	// LG_CLIENT_ID, LG_MQTT_CLIENT_ID y LG_API_CLIENT_ID son tres
	// identidades distintas (topic namespace, sesión MQTT, headers HTTP LG
	// API) y deben quedar en campos separados, no pisarse entre sí.
	if cfg.LG.ClientID != "test-lg-client-id" {
		t.Errorf("LG.ClientID = %q, want test-lg-client-id", cfg.LG.ClientID)
	}
	if cfg.MQTT.ClientID != "test-mqtt-client-id" {
		t.Errorf("MQTT.ClientID = %q, want test-mqtt-client-id", cfg.MQTT.ClientID)
	}
	if cfg.LGApi.ClientID != "test-api-client-id" {
		t.Errorf("LGApi.ClientID = %q, want test-api-client-id", cfg.LGApi.ClientID)
	}

	if cfg.GRPC.RequestTimeout != 20*time.Second {
		t.Errorf("GRPC.RequestTimeout = %v, want 20s", cfg.GRPC.RequestTimeout)
	}
	if cfg.GRPC.MaxAttempts != 5 {
		t.Errorf("GRPC.MaxAttempts = %v, want 5", cfg.GRPC.MaxAttempts)
	}
	if cfg.GRPC.RetryInitialBackoff != 500*time.Millisecond {
		t.Errorf("GRPC.RetryInitialBackoff = %v, want 500ms", cfg.GRPC.RetryInitialBackoff)
	}
	if cfg.GRPC.RetryMaxBackoff != 8000*time.Millisecond {
		t.Errorf("GRPC.RetryMaxBackoff = %v, want 8000ms", cfg.GRPC.RetryMaxBackoff)
	}
}

func TestLoadConfig_InvalidIntEnvFallsBackToDefault(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("TRACKING_GRPC_MAX_ATTEMPTS", "not-a-number")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.GRPC.MaxAttempts != 3 {
		t.Errorf("GRPC.MaxAttempts = %v, want fallback 3 for invalid input", cfg.GRPC.MaxAttempts)
	}
}
