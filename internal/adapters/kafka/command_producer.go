package kafka

import (
	"context"
	"encoding/json"

	commandsdomain "mqtt-api-service/internal/domain/commands"

	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// CommandStatusPublisher escribe los eventos de resultado de entrega
// (device.command.sent / device.command.publish_failed). Un único writer se
// comparte entre ambos topics (el topic se fija por mensaje).
type CommandStatusPublisher struct {
	writer      *kafkago.Writer
	sentTopic   string
	failedTopic string
	log         *zap.Logger
}

func NewCommandStatusPublisher(brokers []string, sentTopic, failedTopic string, log *zap.Logger) *CommandStatusPublisher {
	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Balancer:               &kafkago.LeastBytes{},
		AllowAutoTopicCreation: false,
	}

	return &CommandStatusPublisher{
		writer:      writer,
		sentTopic:   sentTopic,
		failedTopic: failedTopic,
		log:         log,
	}
}

func (p *CommandStatusPublisher) PublishSent(ctx context.Context, evt commandsdomain.CommandSentEvent) error {
	value, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: p.sentTopic,
		Key:   []byte(evt.CommandID),
		Value: value,
	}); err != nil {
		return err
	}

	p.log.Info("device.command.sent published",
		zap.String("commandId", evt.CommandID),
		zap.String("imei", evt.IMEI),
	)
	return nil
}

func (p *CommandStatusPublisher) PublishFailed(ctx context.Context, evt commandsdomain.CommandPublishFailedEvent) error {
	value, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: p.failedTopic,
		Key:   []byte(evt.CommandID),
		Value: value,
	}); err != nil {
		return err
	}

	p.log.Info("device.command.publish_failed published",
		zap.String("commandId", evt.CommandID),
		zap.String("imei", evt.IMEI),
		zap.String("errorMessage", evt.ErrorMessage),
	)
	return nil
}

// Close libera los recursos del writer.
func (p *CommandStatusPublisher) Close() error {
	return p.writer.Close()
}
