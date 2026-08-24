package repository

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"mqtt-api-service/internal/infrastructure/config"
)

type RawMessageRepository interface {
	Save(ctx context.Context, msg RawMessage) error
}

type RawMessage struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Brand       string             `bson:"brand"`
	Topic       string             `bson:"topic,omitempty"`
	IMEI        string             `bson:"imei"`
	MessageType string             `bson:"messageType"`
	Endpoint    string             `bson:"endpoint,omitempty"`
	Payload     map[string]any     `bson:"payload"`
	PayloadRaw  string             `bson:"payloadRaw"`
	ReceivedAt  time.Time          `bson:"receivedAt"`
}

type RawMessageService struct {
	collection *mongo.Collection
}

var _ RawMessageRepository = (*RawMessageService)(nil)

const (
	// Timeout de cada intento individual de connect+ping.
	mongoConnectAttemptTimeout = 10 * time.Second
	// Tiempo máximo total de reintentos antes de fallar (fatal). Dentro del
	// rango recomendado de 60-120s para tolerar DNS/startup timing de Docker.
	mongoMaxElapsed = 90 * time.Second
	// Backoff: 1s, 2s, 4s, 8s, tope 10s.
	mongoInitialBackoff = 1 * time.Second
	mongoMaxBackoff     = 10 * time.Second
)

// NewMongoClient conecta y hace ping a Mongo con retry/backoff acotado, para
// tolerar que el nombre "mongo" todavía no resuelva por DNS o que el
// contenedor de Mongo no esté listo cuando este servicio arranca (ambos
// casos observados en pruebas reales dentro de saas-network). Cada intento
// fallido se loguea como warn; solo se hace fatal (error) cuando se agota
// mongoMaxElapsed.
func NewMongoClient(ctx context.Context, cfg *config.Config, log *zap.Logger) (*mongo.Client, error) {
	backoff := mongoInitialBackoff
	deadline := time.Now().Add(mongoMaxElapsed)

	var lastErr error
	attempt := 0

retryLoop:
	for {
		attempt++

		attemptCtx, cancel := context.WithTimeout(ctx, mongoConnectAttemptTimeout)
		client, err := connectAndPing(attemptCtx, cfg.Mongo.URI)
		cancel()

		if err == nil {
			log.Info("connected to mongo",
				zap.String("uri", redactMongoURI(cfg.Mongo.URI)),
				zap.String("dbName", cfg.Mongo.DBName),
				zap.String("collectionName", cfg.Mongo.CollectionName),
				zap.Int("attempt", attempt),
			)
			return client, nil
		}

		lastErr = err

		if time.Now().After(deadline) {
			break retryLoop
		}

		log.Warn("mongo connection retry",
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.String("uri", redactMongoURI(cfg.Mongo.URI)),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			lastErr = ctx.Err()
			break retryLoop
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > mongoMaxBackoff {
			backoff = mongoMaxBackoff
		}
	}

	log.Error("failed to connect to mongo after retries",
		zap.Int("attempts", attempt),
		zap.Duration("elapsed", mongoMaxElapsed),
		zap.Error(lastErr),
	)

	return nil, fmt.Errorf("failed to connect to mongo after retries: %w", lastErr)
}

func connectAndPing(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongo: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongo: %w", err)
	}

	return client, nil
}

// redactMongoURI quita usuario/password de un connection string de Mongo
// antes de loguearlo. Si la URI no se puede parsear, se devuelve un
// placeholder en vez de arriesgar imprimir credenciales.
func redactMongoURI(uri string) string {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "mongodb://<unparseable-uri-redacted>"
	}

	if parsed.User != nil {
		parsed.User = nil
	}

	return parsed.String()
}

func NewRawMessageService(client *mongo.Client, database, collection string) *RawMessageService {
	return &RawMessageService{
		collection: client.Database(database).Collection(collection),
	}
}

func (s *RawMessageService) Save(
	ctx context.Context,
	msg RawMessage,
) error {

	msg.ReceivedAt = time.Now().UTC()

	_, err := s.collection.InsertOne(ctx, msg)

	if err != nil {
		return fmt.Errorf("failed to save raw message: %w", err)
	}

	return nil
}
