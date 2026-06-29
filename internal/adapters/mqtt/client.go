package mqtt

import (
"context"
"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, topic string, payload []byte) error

type Client interface {
Connect(ctx context.Context) error
Subscribe(ctx context.Context, topic string, handler MessageHandler) error
Publish(ctx context.Context, topic string, payload []byte) error
Disconnect(ctx context.Context) error
IsConnected() bool
}

type client struct {
log *zap.Logger
}

func NewClient(config interface{}, log *zap.Logger) (Client, error) {
return &client{log: log}, nil
}

func (c *client) Connect(ctx context.Context) error {
c.log.Info("MQTT Connect - TODO: Implementar con paho")
return nil
}

func (c *client) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
c.log.Info("MQTT Subscribe - TODO: Implementar", zap.String("topic", topic))
return nil
}

func (c *client) Publish(ctx context.Context, topic string, payload []byte) error {
c.log.Info("MQTT Publish - TODO: Implementar")
return nil
}

func (c *client) Disconnect(ctx context.Context) error {
c.log.Info("MQTT Disconnect - TODO: Implementar")
return nil
}

func (c *client) IsConnected() bool {
return false
}
