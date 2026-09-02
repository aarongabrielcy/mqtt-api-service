// cmd/api/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	adaptercache "mqtt-api-service/internal/adapters/cache"
	grpcclient "mqtt-api-service/internal/adapters/grpc/client"
	adapterkafka "mqtt-api-service/internal/adapters/kafka"
	mongo "mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/adapters/mqtt"
	appcommands "mqtt-api-service/internal/application/commands"
	lg_service "mqtt-api-service/internal/application/use_case/lg"
	infracache "mqtt-api-service/internal/infrastructure/cache"
	"mqtt-api-service/internal/infrastructure/config"
	"mqtt-api-service/internal/infrastructure/logger"

	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Configuración (100% variables de entorno, ver .env.example)
	cfg, err := config.LoadConfig()
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

	// Log de startup seguro (FASE LG-CMD-2G): permite confirmar en logs, sin
	// exponer ningún secreto, qué llegó realmente al proceso desde el env —
	// en particular LOG_LEVEL/LG_DEBUG_STATE_LOGS, que es justo lo que se
	// necesitaba verificar cuando ambos llegaban al contenedor (confirmado
	// por docker inspect) pero los logs de diagnóstico de FASE LG-CMD-2E no
	// aparecían.
	log.Info("mqtt-api-service config loaded",
		zap.String("appEnv", cfg.App.Environment),
		zap.String("logLevel", cfg.App.LogLevel),
		zap.Bool("debugStateLogs", cfg.LGCommands.DebugStateLogs),
		zap.Bool("commandsEnabled", cfg.LGCommands.Enabled),
		zap.Bool("kafkaBrokersConfigured", len(cfg.Kafka.Brokers) > 0),
		zap.Bool("trackingGrpcAddressConfigured", cfg.GRPC.Address != ""),
		zap.Bool("mongoConfigured", cfg.Mongo.URI != ""),
		zap.Bool("redisConfigured", cfg.Redis.Addr != ""),
	)

	mongoClient, err := mongo.NewMongoClient(ctx, cfg, log)
	if err != nil {
		log.Fatal("failed to connect to mongo", zap.Error(err))
	}

	rawMessageRepo := mongo.NewRawMessageService(mongoClient, cfg.Mongo.DBName, cfg.Mongo.CollectionName)

	redisClient, err := infracache.NewRedisClient(ctx, cfg.Redis.Addr, log)
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

	grpcCfg := grpcclient.Config{
		Address:           cfg.GRPC.Address,
		ConnectionTimeout: cfg.GRPC.ConnectionTimeout,
		RequestTimeout:    cfg.GRPC.RequestTimeout,
		MaxAttempts:       cfg.GRPC.MaxAttempts,
		InitialBackoff:    cfg.GRPC.RetryInitialBackoff,
		MaxBackoff:        cfg.GRPC.RetryMaxBackoff,
	}

	grpcTrackingClient, err := grpcclient.New(
		grpcCfg,
		log,
	)

	log.Info(
		"Starting tracking gRPC client",
		zap.String("tracking address", cfg.GRPC.Address),
	)

	if err != nil {
		log.Fatal(
			"failed creating grpc client",
			zap.Error(err),
		)
	}

	defer grpcTrackingClient.Close()

	lgService, err := lg_service.NewLGService(cfg, log, rawMessageRepo, deviceStateStore, grpcTrackingClient)
	if err != nil {
		log.Fatal("Error creando LGService", zap.Error(err))
	}

	err = lgService.Initialize(ctx)
	if err != nil {
		log.Fatal("Error inicializando LGService", zap.Error(err))
	}

	// Bridge de comandos LG por Kafka (FASE LG-CMD-1/2). Si
	// LG_COMMANDS_ENABLED=false, ni el consumer ni el sweep de confirmación
	// arrancan — el servicio se comporta igual que antes de esta fase.
	var commandConsumer *adapterkafka.CommandConsumer
	var commandStatusPublisher *adapterkafka.CommandStatusPublisher

	if cfg.LGCommands.Enabled {
		ackPublisher := appcommands.NewAckPublisher(grpcTrackingClient, log)

		commandStatusPublisher = adapterkafka.NewCommandStatusPublisher(
			cfg.Kafka.Brokers,
			cfg.Kafka.CommandSentTopic,
			cfg.Kafka.CommandPublishFailedTopic,
			log,
		)

		confirmationManager := appcommands.NewConfirmationManager(
			redisClient,
			ackPublisher,
			commandStatusPublisher,
			log,
			cfg.LGCommands.AckTimeout,
			cfg.LGCommands.DebugStateLogs,
		)
		lgService.SetConfirmationManager(confirmationManager)

		dispatcher := appcommands.NewCommandDispatcher(
			log,
			lgService,
			confirmationManager,
			ackPublisher,
			commandStatusPublisher,
			appcommands.ParseConfig{
				TemperatureMinC: cfg.LGCommands.TemperatureMinC,
				TemperatureMaxC: cfg.LGCommands.TemperatureMaxC,
			},
			cfg.LGCommands.SeenTTL,
			cfg.LGCommands.PostRefreshDelay,
		)

		commandConsumer = adapterkafka.NewCommandConsumer(
			cfg.Kafka.Brokers,
			cfg.Kafka.CommandTopic,
			cfg.Kafka.CommandConsumerGroup,
			dispatcher,
			log,
		)

		go commandConsumer.Run(ctx)
		confirmationManager.StartSweep(ctx, cfg.LGCommands.AckSweepInterval)

		log.Info("LG command bridge enabled",
			zap.String("topic", cfg.Kafka.CommandTopic),
			zap.String("group", cfg.Kafka.CommandConsumerGroup),
			zap.Strings("brokers", cfg.Kafka.Brokers),
		)
	} else {
		log.Info("LG command bridge disabled (LG_COMMANDS_ENABLED=false)")
	}

	// FASE LG-CMD-2H: ambos intervalos eran hardcodeados (30 min / 2 min).
	// El de estado en particular quedaba demasiado lejos de
	// LG_COMMAND_ACK_TIMEOUT_SECONDS — un comando podía vencer antes de que
	// corriera el siguiente ciclo de polling si LG no mandaba push
	// inmediato. Ahora ambos vienen de config (defaults: 30s / 30min).
	lgService.StartEventSubscriptionMonitor(ctx, cfg.LG.EventSubscriptionMonitorInterval)
	lgService.StartDeviceStateMonitor(ctx, cfg.LG.StatePollInterval)

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

	if err := client.Subscribe(ctx, inboxTopic, inboxHandler); err != nil {
		log.Error("Error suscribiendo a topic", zap.String("topic", inboxTopic), zap.Error(err))
	}

	// 9. Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	log.Info("Señal de shutdown recibida")
	cancel()

	if err := client.Disconnect(ctx); err != nil {
		log.Warn("error closing mqtt client", zap.Error(err))
	}

	if commandConsumer != nil {
		if err := commandConsumer.Close(); err != nil {
			log.Warn("error closing kafka command consumer", zap.Error(err))
		}
	}
	if commandStatusPublisher != nil {
		if err := commandStatusPublisher.Close(); err != nil {
			log.Warn("error closing kafka command status publisher", zap.Error(err))
		}
	}

	log.Info("mqtt-api-service detenido")
}
