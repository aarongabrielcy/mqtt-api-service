package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config se alimenta 100% de variables de entorno (ver .env.example para el
// contrato completo). No hay runtime dependiente de YAML: config/config.yaml
// ya no existe ni se lee.
type Config struct {
	App struct {
		Environment string
		LogLevel    string
	}

	// LG.ClientID identifica la app/tenant LG (LG_CLIENT_ID). Se usa para
	// construir los topics app/clients/<ClientID>/{push|inbox} y para
	// comparar contra las suscripciones existentes en el registry LG.
	// Es distinto de MQTT.ClientID (la sesión MQTT/AWS IoT, ver abajo) y de
	// LGApi.ClientID (usado en headers HTTP hacia la LG API).
	LG struct {
		ClientID string

		// StatePollInterval (FASE LG-CMD-2H) es cada cuánto se hace un GET
		// /devices/:id/state normal (event=0/EventCodeTracking) para todos
		// los dispositivos administrados. Antes de esta fase estaba
		// hardcodeado a 2 minutos en cmd/api/main.go — con
		// LG_COMMAND_ACK_TIMEOUT_SECONDS en 60s (ahora 90s), un intervalo de
		// 2 minutos podía dejar vencer la confirmación de un comando antes
		// de que corriera el siguiente ciclo de polling, si LG no mandaba
		// push inmediato. Default 30s.
		StatePollInterval time.Duration

		// EventSubscriptionMonitorInterval es cada cuánto se revalida la
		// suscripción push/event LG (antes hardcodeado a 30 minutos, ahora
		// configurable con el mismo default).
		EventSubscriptionMonitorInterval time.Duration
	}

	MQTT struct {
		Endpoint          string
		ClientID          string
		KeepAlive         int
		QoS               int
		ReconnectInterval string

		TLS struct {
			CAFile   string
			CertFile string
			KeyFile  string
		}
	}

	GRPC struct {
		Address           string
		ConnectionTimeout time.Duration

		// Timeout/retry por request de IngestRaw. Pensados para tolerar
		// que ingestion-service tarde en resolver por DNS Docker o en
		// estar listo durante el arranque (ver FASE LG-1B).
		RequestTimeout      time.Duration
		MaxAttempts         int
		RetryInitialBackoff time.Duration
		RetryMaxBackoff     time.Duration
	}

	Mongo struct {
		URI            string
		DBName         string
		CollectionName string
	}

	Redis struct {
		Addr string
	}

	LGApi struct {
		BaseURL      string
		APIKey       string
		AccessToken  string
		ClientID     string
		CountryCode  string
		LanguageCode string
		ServicePhase string
		Timeout      string
	}

	// Kafka es el bridge de comandos oficial de la plataforma
	// (device.command.requested / device.command.sent /
	// device.command.publish_failed). No se usa para telemetría (eso sigue
	// yendo por gRPC IngestRaw).
	Kafka struct {
		Brokers                   []string
		CommandTopic              string
		CommandConsumerGroup      string
		CommandSentTopic          string
		CommandPublishFailedTopic string
	}

	// LGCommands controla el bridge de comandos LG por Kafka (ver
	// internal/application/commands). Si Enabled es false, el consumer de
	// Kafka no arranca y el servicio se comporta igual que antes de esta
	// fase (solo telemetría). Enabled se resuelve vía getEnvBoolStrict con
	// dos defaults distintos: LG_COMMANDS_ENABLED sin definir usa el
	// default explícito (true, documentado en .env.example), pero un valor
	// definido y no parseable ("maybe", etc.) nunca activa el bridge por
	// accidente — cae a false con un warning en stderr, no al default.
	LGCommands struct {
		Enabled          bool
		AckTimeout       time.Duration
		AckSweepInterval time.Duration
		SeenTTL          time.Duration
		TemperatureMinC  float64
		TemperatureMaxC  float64

		// DebugStateLogs activa instrumentación de diagnóstico verbosa
		// (FASE LG-CMD-2E): JSON raw de estado LG, diagnóstico de presencia
		// del campo Oscillation, payload normalizado antes de gRPC, y
		// expected-vs-actual en ConfirmationManager.TryConfirm. Nunca
		// incluye headers/tokens. Default false en ambos casos
		// (LG_DEBUG_STATE_LOGS sin definir o con valor inválido) — a
		// diferencia de LGCommands.Enabled, este flag no tiene ningún
		// comportamiento "seguro por defecto" que justifique defaults
		// distintos entre unset e inválido.
		DebugStateLogs bool

		// PostRefreshDelay (FASE LG-CMD-2H) es cuánto esperar tras un
		// executeLGCommand exitoso (u ambiguo, 2211) antes de hacer un GET
		// /devices/:id/state puntual — LG puede tardar un instante en
		// reflejar el cambio físico en su propio estado consultable.
		// Default 1000ms.
		PostRefreshDelay time.Duration
	}
}

// LoadConfig carga la configuración únicamente desde variables de entorno.
// Si existe un archivo .env en el directorio de trabajo (uso local/dev), se
// carga primero con godotenv; en runtime dentro de saas-system-iot las
// variables llegan directamente vía env_file de docker-compose.
func LoadConfig() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}

	cfg.App.Environment = getEnv("APP_ENV", "local")
	cfg.App.LogLevel = getEnv("LOG_LEVEL", "info")

	cfg.LG.ClientID = getEnv("LG_CLIENT_ID", "")
	cfg.LG.StatePollInterval = time.Duration(getEnvInt("LG_STATE_POLL_INTERVAL_SECONDS", 30)) * time.Second
	cfg.LG.EventSubscriptionMonitorInterval = time.Duration(getEnvInt("LG_EVENT_SUBSCRIPTION_MONITOR_INTERVAL_SECONDS", 1800)) * time.Second

	cfg.LGApi.BaseURL = getEnv("LG_API_BASE_URL", "")
	cfg.LGApi.APIKey = getEnv("LG_API_KEY", "")
	cfg.LGApi.AccessToken = getEnv("LG_ACCESS_TOKEN", "")
	cfg.LGApi.ClientID = getEnv("LG_API_CLIENT_ID", "")
	cfg.LGApi.CountryCode = getEnv("COUNTRY_CODE", "MX")
	cfg.LGApi.LanguageCode = getEnv("LANGUAGE_CODE", "es-MX")
	cfg.LGApi.ServicePhase = getEnv("SERVICE_PHASE", "OP")
	// Timeout de la LG API: valor interno fijo, no forma parte del
	// contrato de env vars de esta fase.
	cfg.LGApi.Timeout = "30s"

	cfg.MQTT.Endpoint = getEnv("LG_MQTT_ENDPOINT", "")
	cfg.MQTT.ClientID = getEnv("LG_MQTT_CLIENT_ID", "")
	cfg.MQTT.TLS.CAFile = getEnv("LG_MQTT_CA_CERT_PATH", "/app/certs/AmazonRootCA1.pem")
	cfg.MQTT.TLS.CertFile = getEnv("LG_MQTT_CLIENT_CERT_PATH", "/app/certs/lg-client.crt")
	cfg.MQTT.TLS.KeyFile = getEnv("LG_MQTT_CLIENT_KEY_PATH", "/app/certs/lg-client.key")
	// KeepAlive/QoS/ReconnectInterval no forman parte del contrato de env
	// vars de esta fase (ya eran valores fijos en config.yaml antes de
	// LG-1A); ReconnectInterval y QoS tampoco se leían realmente en
	// internal/adapters/mqtt/client.go (valores hardcodeados ahí).
	cfg.MQTT.KeepAlive = 60
	cfg.MQTT.QoS = 1
	cfg.MQTT.ReconnectInterval = "30s"

	cfg.GRPC.Address = getEnv("TRACKING_PLATFORM_GRPC_ADDRESS", "ingestion-service:50051")
	cfg.GRPC.ConnectionTimeout = 5 * time.Second
	cfg.GRPC.RequestTimeout = time.Duration(getEnvInt("TRACKING_GRPC_REQUEST_TIMEOUT_SECONDS", 10)) * time.Second
	cfg.GRPC.MaxAttempts = getEnvInt("TRACKING_GRPC_MAX_ATTEMPTS", 3)
	cfg.GRPC.RetryInitialBackoff = time.Duration(getEnvInt("TRACKING_GRPC_RETRY_INITIAL_BACKOFF_MS", 1000)) * time.Millisecond
	cfg.GRPC.RetryMaxBackoff = time.Duration(getEnvInt("TRACKING_GRPC_RETRY_MAX_BACKOFF_MS", 4000)) * time.Millisecond

	cfg.Mongo.URI = getEnv("MONGO_URI", "mongodb://mongo:27017")
	cfg.Mongo.DBName = getEnv("MONGO_DB_NAME", "mqtt-api-service")
	cfg.Mongo.CollectionName = getEnv("MONGO_COLLECTION_NAME", "raw_lg_messages")

	cfg.Redis.Addr = getEnv("REDIS_ADDR", "redis:6379")

	cfg.Kafka.Brokers = splitCSV(getEnv("KAFKA_BROKERS", "kafka:9092"))
	cfg.Kafka.CommandTopic = getEnv("KAFKA_COMMAND_TOPIC", "device.command.requested")
	cfg.Kafka.CommandConsumerGroup = getEnv("KAFKA_COMMAND_CONSUMER_GROUP", "mqtt-api-service-lg-commands")
	cfg.Kafka.CommandSentTopic = getEnv("KAFKA_COMMAND_SENT_TOPIC", "device.command.sent")
	cfg.Kafka.CommandPublishFailedTopic = getEnv("KAFKA_COMMAND_PUBLISH_FAILED_TOPIC", "device.command.publish_failed")

	cfg.LGCommands.Enabled = getEnvBoolStrict("LG_COMMANDS_ENABLED", true, false)
	// AckTimeout default subido de 60s a 90s (FASE LG-CMD-2H): con
	// StatePollInterval en 30s, 90s da margen para ~2-3 ciclos de polling
	// normal más margen de red/procesamiento, en vez de quedar casi
	// exactamente al filo de un solo ciclo de 60s como antes.
	cfg.LGCommands.AckTimeout = time.Duration(getEnvInt("LG_COMMAND_ACK_TIMEOUT_SECONDS", 90)) * time.Second
	cfg.LGCommands.AckSweepInterval = time.Duration(getEnvInt("LG_COMMAND_ACK_SWEEP_SECONDS", 5)) * time.Second
	cfg.LGCommands.SeenTTL = time.Duration(getEnvInt("LG_COMMAND_SEEN_TTL_SECONDS", 600)) * time.Second
	cfg.LGCommands.TemperatureMinC = getEnvFloat("LG_COMMAND_TEMPERATURE_MIN_C", 16)
	cfg.LGCommands.TemperatureMaxC = getEnvFloat("LG_COMMAND_TEMPERATURE_MAX_C", 30)
	cfg.LGCommands.DebugStateLogs = getEnvBoolStrict("LG_DEBUG_STATE_LOGS", false, false)
	cfg.LGCommands.PostRefreshDelay = time.Duration(getEnvInt("LG_COMMAND_POST_REFRESH_DELAY_MS", 1000)) * time.Millisecond

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// getEnvBoolStrict resuelve un booleano con dos defaults distintos: uno
// para cuando la variable no está definida (defaultUnset), y otro más
// conservador para cuando SÍ está definida pero con un valor no parseable
// (fallbackInvalid). Esto evita que un typo en el env var (ej.
// LG_COMMANDS_ENABLED=tru) active silenciosamente un comportamiento que
// requiere defaultUnset=true — un valor inválido siempre cae a
// fallbackInvalid, nunca a defaultUnset, y se reporta a stderr para que no
// pase desapercibido en logs de arranque (todavía no existe un logger
// zap en este punto de LoadConfig).
func getEnvBoolStrict(key string, defaultUnset bool, fallbackInvalid bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultUnset
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: invalid value %q for %s, falling back to %v\n", v, key, fallbackInvalid)
		return fallbackInvalid
	}
	return b
}

func getEnvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

// splitCSV soporta KAFKA_BROKERS como lista separada por coma
// ("kafka:9092,kafka2:9092"), recortando espacios y descartando entradas
// vacías.
func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
