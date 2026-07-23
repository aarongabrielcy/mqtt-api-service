package repository

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

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

const defaultMongoConnectTimeout = 10 * time.Second

func NewMongoClient(ctx context.Context, cfg *config.Config) (*mongo.Client, error) {
	connectCtx, cancel := context.WithTimeout(ctx, defaultMongoConnectTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(cfg.Mongo.URI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongo: %w", err)
	}

	if err := client.Ping(connectCtx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongo: %w", err)
	}

	return client, nil
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
