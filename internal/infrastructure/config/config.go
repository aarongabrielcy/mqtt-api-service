package config

type Config struct {
App struct {
Name        string
LogLevel    string
Environment string
}
MQTT struct {
Broker   string
Port     int
Username string
Password string
ClientID string
}
GRPC struct {
Target string
}
Mongo struct {
URI string
}
LGApi struct {
BaseURL string
}
}

func LoadConfig(path string) (*Config, error) {
cfg := &Config{}
cfg.App.Name = "mqtt-api-service"
cfg.App.LogLevel = "info"
cfg.App.Environment = "production"
cfg.MQTT.Broker = "mqtt.lgeapi.com"
cfg.MQTT.Port = 8883
cfg.MQTT.ClientID = "test-client"
cfg.GRPC.Target = "localhost:50051"
cfg.Mongo.URI = "mongodb://localhost:27017"
cfg.LGApi.BaseURL = "https://smartsolution.developer.lge.com/api"
return cfg, nil
}
