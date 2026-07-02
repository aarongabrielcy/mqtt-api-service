// cmd/api/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	//"mqtt-api-service/internal/adapters/api"
	//"mqtt-api-service/internal/adapters/grpc"
	//"mqtt-api-service/internal/adapters/mongo"

	adaptercache "mqtt-api-service/internal/adapters/cache"
	mongo "mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/adapters/mqtt"
	lg_service "mqtt-api-service/internal/application/use_case/lg"
	infracache "mqtt-api-service/internal/infrastructure/cache"

	//"mqtt-api-service/internal/adapters/parser"
	//"mqtt-api-service/internal/application/normalizers"

	//"mqtt-api-service/internal/infrastructure/config"
	"mqtt-api-service/internal/infrastructure/config"
	"mqtt-api-service/internal/infrastructure/logger"

	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Configuración
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error cargando config: %v\n", err)
		os.Exit(1)
	}

	// 2. Logger
	log := logger.NewLogger(cfg.App.LogLevel)
	defer log.Sync()

	log.Info("Iniciando mqtt-api-service",
		zap.String("version", "1.0.0"),
		zap.String("environment", cfg.App.Environment),
	)

	mongoClient, err := mongo.NewMongoClient(ctx, cfg)
	if err != nil {
		log.Fatal("failed to connect to mongo", zap.Error(err))
	}

	rawMessageRepo := mongo.NewRawMessageService(mongoClient, "mqtt_api_service", "raw_messages")

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisClient, err := infracache.NewRedisClient(ctx, redisAddr, log)
	if err != nil {
		log.Fatal("failed to connect to redis", zap.Error(err))
	}

	deviceStateStore := adaptercache.NewDeviceStateStore(redisClient, log)

	// 3. MQTT Client (LG broker)
	client, err := mqtt.NewClient(*cfg, log)
	if err != nil {
		log.Fatal("mqtt client error", zap.Error(err))
	}

	log.Info("Intentando conectar a MQTT...")

	if err := client.Connect(ctx); err != nil {
		log.Fatal("MQTT connect failed", zap.Error(err))
	}

	log.Info("MQTT CONECTADO EXITOSAMENTE")

	lgService, err := lg_service.NewLGService(cfg, log, rawMessageRepo, deviceStateStore)
	if err != nil {
		log.Fatal("Error creando LGService", zap.Error(err))
	}

	err = lgService.Initialize(ctx)
	if err != nil {
		log.Fatal("Error inicializando LGService", zap.Error(err))
	}

	lgService.StartEventSubscriptionMonitor(ctx, 30*time.Minute)
	lgService.StartDeviceStateMonitor(ctx, 2*time.Minute)

	inboxHandler := func(ctx context.Context, topic string, payload []byte) error {
		log.Info("Mensaje recibido",
			zap.String("topic", topic),
			zap.ByteString("payload", payload),
		)
		return nil
	}

	pushTopic := fmt.Sprintf("app/clients/%s/push", cfg.MQTT.ClientID)
	inboxTopic := fmt.Sprintf("app/clients/%s/inbox", cfg.MQTT.ClientID)

	if err := client.Subscribe(ctx, pushTopic, lgService.HandlePushMessage); err != nil {
		log.Error("Error suscribiendo a topic", zap.String("topic", pushTopic), zap.Error(err))
	}
	log.Info("Suscrito a topic", zap.String("topic", pushTopic))

	if err := client.Subscribe(ctx, inboxTopic, inboxHandler); err != nil {
		log.Error("Error suscribiendo a topic", zap.String("topic", inboxTopic), zap.Error(err))
	}
	log.Info("Suscrito a topic", zap.String("topic", inboxTopic))

	//lgService.SetDevicePower(ctx, "c31d67537eaaad08efeb6ee111c5ecd2b8316b79147e5a69d18642ab78bea3ca", true)

	// // 4. Componentes
	// lgAPIClient := api.NewLGAPIClient(cfg.LGApi, log)
	// mongoRepo, err := mongo.NewRepository(cfg.Mongo, log)
	// if err != nil {
	// 	log.Fatal("Error conectando MongoDB", zap.Error(err))
	// }
	// defer mongoRepo.Close(ctx)

	// grpcClient, err := grpc.NewClient(cfg.GRPC, log)
	// if err != nil {
	// 	log.Fatal("Error creando gRPC client", zap.Error(err))
	// }

	// // 5. Parsers y normalizers
	// lgParser := parser.NewLGMessageParser(log)
	// messageNormalizer := normalizers.NewLGMessageNormalizer(log)
	// eventClassifier := normalizers.NewEventClassifier(log)

	// // 6. Servicio principal
	// lgService := services.NewLGService(
	// 	mqttClient,
	// 	lgAPIClient,
	// 	mongoRepo,
	// 	grpcClient,
	// 	lgParser,
	// 	messageNormalizer,
	// 	eventClassifier,
	// 	log,
	// )

	// // 7. Conectar MQTT
	// if err := mqttClient.Connect(ctx); err != nil {
	// 	log.Fatal("Error conectando a MQTT broker LG", zap.Error(err))
	// }
	// defer mqttClient.Disconnect(ctx)

	// // 8. Suscribirse a topics de LG
	// subscriptions := []string{
	// 	fmt.Sprintf("app/clients/%s/push", cfg.MQTT.ClientID),
	// 	fmt.Sprintf("app/clients/%s/inbox", cfg.MQTT.ClientID),
	// 	fmt.Sprintf("app/clients/%s/outbox", cfg.MQTT.ClientID),
	// }

	// for _, topic := range subscriptions {
	// 	if err := mqttClient.Subscribe(ctx, topic, lgService.HandleLGMessage); err != nil {
	// 		log.Error("Error suscribiendo a topic", zap.String("topic", topic), zap.Error(err))
	// 	}
	// 	log.Info("Suscrito a topic", zap.String("topic", topic))
	// }

	// 9. Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Info("Señal de shutdown recibida")
	cancel()
	log.Info("mqtt-api-service detenido")
}
