package config

import (
	"os"
	"strconv"
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

	DeviceControlGRPC struct {
		Address string
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

	cfg.DeviceControlGRPC.Address = getEnv("DEVICE_CONTROL_GRPC_ADDRESS", ":50052")

	cfg.Mongo.URI = getEnv("MONGO_URI", "mongodb://mongo:27017")
	cfg.Mongo.DBName = getEnv("MONGO_DB_NAME", "mqtt-api-service")
	cfg.Mongo.CollectionName = getEnv("MONGO_COLLECTION_NAME", "raw_lg_messages")

	cfg.Redis.Addr = getEnv("REDIS_ADDR", "redis:6379")

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
