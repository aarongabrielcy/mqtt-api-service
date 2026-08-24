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
		"APP_ENV",
		"LOG_LEVEL",
		"COUNTRY_CODE",
		"LANGUAGE_CODE",
		"SERVICE_PHASE",
		"TRACKING_GRPC_REQUEST_TIMEOUT_SECONDS",
		"TRACKING_GRPC_MAX_ATTEMPTS",
		"TRACKING_GRPC_RETRY_INITIAL_BACKOFF_MS",
		"TRACKING_GRPC_RETRY_MAX_BACKOFF_MS",
		"KAFKA_BROKERS",
		"KAFKA_COMMAND_TOPIC",
		"KAFKA_COMMAND_CONSUMER_GROUP",
		"KAFKA_COMMAND_SENT_TOPIC",
		"KAFKA_COMMAND_PUBLISH_FAILED_TOPIC",
		"LG_COMMANDS_ENABLED",
		"LG_COMMAND_ACK_TIMEOUT_SECONDS",
		"LG_COMMAND_ACK_SWEEP_SECONDS",
		"LG_COMMAND_SEEN_TTL_SECONDS",
		"LG_COMMAND_TEMPERATURE_MIN_C",
		"LG_COMMAND_TEMPERATURE_MAX_C",
		"LG_DEBUG_STATE_LOGS",
		"LG_STATE_POLL_INTERVAL_SECONDS",
		"LG_EVENT_SUBSCRIPTION_MONITOR_INTERVAL_SECONDS",
		"LG_COMMAND_POST_REFRESH_DELAY_MS",
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

	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "kafka:9092" {
		t.Errorf("Kafka.Brokers = %v, want [kafka:9092]", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.CommandTopic != "device.command.requested" {
		t.Errorf("Kafka.CommandTopic = %q, want device.command.requested", cfg.Kafka.CommandTopic)
	}
	if cfg.Kafka.CommandConsumerGroup != "mqtt-api-service-lg-commands" {
		t.Errorf("Kafka.CommandConsumerGroup = %q, want mqtt-api-service-lg-commands", cfg.Kafka.CommandConsumerGroup)
	}
	if cfg.Kafka.CommandSentTopic != "device.command.sent" {
		t.Errorf("Kafka.CommandSentTopic = %q, want device.command.sent", cfg.Kafka.CommandSentTopic)
	}
	if cfg.Kafka.CommandPublishFailedTopic != "device.command.publish_failed" {
		t.Errorf("Kafka.CommandPublishFailedTopic = %q, want device.command.publish_failed", cfg.Kafka.CommandPublishFailedTopic)
	}

	if !cfg.LGCommands.Enabled {
		t.Error("LGCommands.Enabled = false, want true by default")
	}
	// FASE LG-CMD-2H: default subido de 60s a 90s, para dar margen frente a
	// LG.StatePollInterval (default 30s) — antes quedaba casi exactamente
	// al filo de un solo ciclo de polling.
	if cfg.LGCommands.AckTimeout != 90*time.Second {
		t.Errorf("LGCommands.AckTimeout = %v, want 90s", cfg.LGCommands.AckTimeout)
	}
	if cfg.LGCommands.AckSweepInterval != 5*time.Second {
		t.Errorf("LGCommands.AckSweepInterval = %v, want 5s", cfg.LGCommands.AckSweepInterval)
	}
	if cfg.LGCommands.SeenTTL != 600*time.Second {
		t.Errorf("LGCommands.SeenTTL = %v, want 600s", cfg.LGCommands.SeenTTL)
	}
	if cfg.LGCommands.TemperatureMinC != 16 {
		t.Errorf("LGCommands.TemperatureMinC = %v, want 16", cfg.LGCommands.TemperatureMinC)
	}
	if cfg.LGCommands.TemperatureMaxC != 30 {
		t.Errorf("LGCommands.TemperatureMaxC = %v, want 30", cfg.LGCommands.TemperatureMaxC)
	}
	if cfg.LGCommands.DebugStateLogs {
		t.Error("LGCommands.DebugStateLogs = true, want false by default")
	}

	// FASE LG-CMD-2H: polling normal de estado configurable (default 30s,
	// antes hardcodeado a 2 minutos en cmd/api/main.go), renovación de
	// suscripción configurable (default 30 minutos, mismo valor que antes
	// hardcodeado), y delay del refresh post-comando (default 1000ms).
	if cfg.LG.StatePollInterval != 30*time.Second {
		t.Errorf("LG.StatePollInterval = %v, want 30s", cfg.LG.StatePollInterval)
	}
	if cfg.LG.EventSubscriptionMonitorInterval != 30*time.Minute {
		t.Errorf("LG.EventSubscriptionMonitorInterval = %v, want 30m", cfg.LG.EventSubscriptionMonitorInterval)
	}
	if cfg.LGCommands.PostRefreshDelay != 1000*time.Millisecond {
		t.Errorf("LGCommands.PostRefreshDelay = %v, want 1000ms", cfg.LGCommands.PostRefreshDelay)
	}
}

func TestLoadConfig_LGStatePollInterval_CustomEnv(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_STATE_POLL_INTERVAL_SECONDS", "45")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LG.StatePollInterval != 45*time.Second {
		t.Errorf("LG.StatePollInterval = %v, want 45s", cfg.LG.StatePollInterval)
	}
}

func TestLoadConfig_LGEventSubscriptionMonitorInterval_CustomEnv(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_EVENT_SUBSCRIPTION_MONITOR_INTERVAL_SECONDS", "900")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LG.EventSubscriptionMonitorInterval != 900*time.Second {
		t.Errorf("LG.EventSubscriptionMonitorInterval = %v, want 900s", cfg.LG.EventSubscriptionMonitorInterval)
	}
}

func TestLoadConfig_LGCommandPostRefreshDelay_CustomEnv(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_COMMAND_POST_REFRESH_DELAY_MS", "2500")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LGCommands.PostRefreshDelay != 2500*time.Millisecond {
		t.Errorf("LGCommands.PostRefreshDelay = %v, want 2500ms", cfg.LGCommands.PostRefreshDelay)
	}
}

func TestLoadConfig_KafkaBrokersCommaSeparated(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("KAFKA_BROKERS", "kafka1:9092, kafka2:9092 ,kafka3:9092")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"}
	if len(cfg.Kafka.Brokers) != len(want) {
		t.Fatalf("Kafka.Brokers = %v, want %v", cfg.Kafka.Brokers, want)
	}
	for i, w := range want {
		if cfg.Kafka.Brokers[i] != w {
			t.Errorf("Kafka.Brokers[%d] = %q, want %q", i, cfg.Kafka.Brokers[i], w)
		}
	}
}

func TestLoadConfig_LGCommandsEnabled_Unset(t *testing.T) {
	clearEcosystemEnv(t)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.LGCommands.Enabled {
		t.Error("LGCommands.Enabled = false, want the explicit default (true) when LG_COMMANDS_ENABLED is unset")
	}
}

func TestLoadConfig_LGCommandsEnabled_True(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_COMMANDS_ENABLED", "true")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.LGCommands.Enabled {
		t.Error("LGCommands.Enabled = false, want true when LG_COMMANDS_ENABLED=true")
	}
}

func TestLoadConfig_LGCommandsEnabled_False(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_COMMANDS_ENABLED", "false")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LGCommands.Enabled {
		t.Error("LGCommands.Enabled = true, want false when LG_COMMANDS_ENABLED=false")
	}
}

func TestLoadConfig_LGCommandsCustomEnv(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_COMMAND_ACK_TIMEOUT_SECONDS", "90")
	t.Setenv("LG_COMMAND_ACK_SWEEP_SECONDS", "10")
	t.Setenv("LG_COMMAND_SEEN_TTL_SECONDS", "1200")
	t.Setenv("LG_COMMAND_TEMPERATURE_MIN_C", "18")
	t.Setenv("LG_COMMAND_TEMPERATURE_MAX_C", "28")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LGCommands.AckTimeout != 90*time.Second {
		t.Errorf("LGCommands.AckTimeout = %v, want 90s", cfg.LGCommands.AckTimeout)
	}
	if cfg.LGCommands.AckSweepInterval != 10*time.Second {
		t.Errorf("LGCommands.AckSweepInterval = %v, want 10s", cfg.LGCommands.AckSweepInterval)
	}
	if cfg.LGCommands.SeenTTL != 1200*time.Second {
		t.Errorf("LGCommands.SeenTTL = %v, want 1200s", cfg.LGCommands.SeenTTL)
	}
	if cfg.LGCommands.TemperatureMinC != 18 {
		t.Errorf("LGCommands.TemperatureMinC = %v, want 18", cfg.LGCommands.TemperatureMinC)
	}
	if cfg.LGCommands.TemperatureMaxC != 28 {
		t.Errorf("LGCommands.TemperatureMaxC = %v, want 28", cfg.LGCommands.TemperatureMaxC)
	}
}

// TestLoadConfig_LGCommandsEnabled_InvalidFallsBackToFalse cubre el
// endurecimiento de FASE LG-CMD-2B: un valor inválido para
// LG_COMMANDS_ENABLED (typo, valor vacío con espacios, etc.) NUNCA debe
// activar el bridge de comandos "por accidente" — a diferencia del resto de
// los env vars numéricos de este archivo (que caen a su default normal ante
// un valor inválido), acá el fallback ante valor-inválido es
// deliberadamente más conservador que el default de "no seteado".
func TestLoadConfig_LGCommandsEnabled_InvalidFallsBackToFalse(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_COMMANDS_ENABLED", "not-a-bool")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LGCommands.Enabled {
		t.Error("LGCommands.Enabled = true, want false (safe fallback) for an invalid value")
	}
}

// TestLoadConfig_LGDebugStateLogs_* cubre FASE LG-CMD-2E: a diferencia de
// LGCommands.Enabled, LG_DEBUG_STATE_LOGS no tiene ningún comportamiento
// "seguro por defecto" que justifique defaults distintos entre unset e
// inválido — ambos casos deben caer a false.
func TestLoadConfig_LGDebugStateLogs_DefaultFalseWhenUnset(t *testing.T) {
	clearEcosystemEnv(t)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LGCommands.DebugStateLogs {
		t.Error("LGCommands.DebugStateLogs = true, want false when LG_DEBUG_STATE_LOGS is unset")
	}
}

func TestLoadConfig_LGDebugStateLogs_True(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_DEBUG_STATE_LOGS", "true")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.LGCommands.DebugStateLogs {
		t.Error("LGCommands.DebugStateLogs = false, want true when LG_DEBUG_STATE_LOGS=true")
	}
}

func TestLoadConfig_LGDebugStateLogs_False(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_DEBUG_STATE_LOGS", "false")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LGCommands.DebugStateLogs {
		t.Error("LGCommands.DebugStateLogs = true, want false when LG_DEBUG_STATE_LOGS=false")
	}
}

func TestLoadConfig_LGDebugStateLogs_InvalidFallsBackToFalse(t *testing.T) {
	clearEcosystemEnv(t)
	t.Setenv("LG_DEBUG_STATE_LOGS", "not-a-bool")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.LGCommands.DebugStateLogs {
		t.Error("LGCommands.DebugStateLogs = true, want false (safe default) for an invalid value")
	}
}

func TestLoadConfig_ReadsFromEnv(t *testing.T) {
	clearEcosystemEnv(t)

	t.Setenv("REDIS_ADDR", "custom-redis:1234")
	t.Setenv("MONGO_DB_NAME", "custom-db")
	t.Setenv("MONGO_COLLECTION_NAME", "custom-collection")
	t.Setenv("TRACKING_PLATFORM_GRPC_ADDRESS", "custom-ingestion:9999")
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
