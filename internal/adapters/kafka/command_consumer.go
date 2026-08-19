package kafka

import (
	"context"
	"encoding/json"

	commandsdomain "mqtt-api-service/internal/domain/commands"

	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// CommandHandler es lo que despacha un evento ya deserializado. Implementado
// por internal/application/commands.CommandDispatcher.
type CommandHandler interface {
	Dispatch(ctx context.Context, event commandsdomain.DeviceCommandEvent) error
}

// CommandConsumer consume device.command.requested. A diferencia del patrón
// de mqtt-adapter-service (auto-commit vía ReadMessage), este usa
// FetchMessage + CommitMessages para hacer commit manual solo después de
// que el dispatcher procesó el mensaje.
type CommandConsumer struct {
	reader  *kafkago.Reader
	handler CommandHandler
	log     *zap.Logger
}

func NewCommandConsumer(brokers []string, topic, groupID string, handler CommandHandler, log *zap.Logger) *CommandConsumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	return &CommandConsumer{
		reader:  reader,
		handler: handler,
		log:     log,
	}
}

// Run bloquea hasta que ctx se cancele. Debe llamarse en una goroutine.
// No loguea el payload completo del mensaje (puede contener datos de
// negocio no destinados a logs), solo identificadores.
func (c *CommandConsumer) Run(ctx context.Context) {
	c.log.Info("lg command consumer started",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group", c.reader.Config().GroupID),
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("lg command consumer stopped")
				return
			}
			c.log.Warn("lg command consumer read error", zap.Error(err))
			continue
		}

		var event commandsdomain.DeviceCommandEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.log.Warn("cannot parse command message, skipping",
				zap.Int64("offset", msg.Offset), zap.Error(err))
			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				c.log.Warn("failed to commit unparseable message", zap.Error(commitErr))
			}
			continue
		}

		if err := c.handler.Dispatch(ctx, event); err != nil {
			c.log.Error("dispatch failed",
				zap.String("commandId", event.CommandID),
				zap.String("imei", event.IMEI),
				zap.Error(err),
			)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Warn("failed to commit message",
				zap.String("commandId", event.CommandID), zap.Error(err))
		}
	}
}

// Close libera los recursos del reader.
func (c *CommandConsumer) Close() error {
	return c.reader.Close()
}
