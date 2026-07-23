package config

import (
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	App struct {
		Name        string `yaml:"name"`
		LogLevel    string `yaml:"log_level"`
		Environment string `yaml:"environment"`
	} `yaml:"app"`

	MQTT struct {
		Endpoint          string `yaml:"endpoint"`
		ClientID          string `yaml:"client_id"`
		KeepAlive         int    `yaml:"keepalive"`
		QoS               int    `yaml:"qos"`
		ReconnectInterval string `yaml:"reconnect_interval"`

		TLS struct {
			CAFile   string `yaml:"ca_file"`
			CertFile string `yaml:"cert_file"`
			KeyFile  string `yaml:"key_file"`
		} `yaml:"tls"`
	} `yaml:"mqtt"`

	GRPC struct {
		//TODO: ADAPTAR A OFICIAL
		Address           string        `yaml:"address"`
		ConnectionTimeout time.Duration `yaml:"connection_timeout"`
	} `yaml:"grpc"`

	Mongo struct {
		URI            string `yaml:"uri"`
		DBName         string `yaml:"db_name"`
		CollectionName string `yaml:"collection_name"`
	} `yaml:"mongo"`

	LGApi struct {
		BaseURL      string `yaml:"base_url"`
		APIKey       string `yaml:"api_key"`
		AccessToken  string `yaml:"access_token"`
		ClientID     string `yaml:"client_id"`
		CountryCode  string `yaml:"country_code"`
		LanguageCode string `yaml:"language_code"`
		ServicePhase string `yaml:"service_phase"`
		Timeout      string `yaml:"timeout"`
	} `yaml:"lg_api"`
}

func LoadConfig(path string) (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("no se pudo leer %s: %w", path, err)
	}

	data = []byte(os.ExpandEnv(string(data)))

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("error al parsear %s: %w", path, err)
	}

	return cfg, nil
}
