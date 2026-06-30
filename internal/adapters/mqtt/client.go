package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"

	"mqtt-api-service/internal/infrastructure/config"
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
	log    *zap.Logger
	cfg    config.Config
	client mqtt.Client
	mu     sync.RWMutex
}

func NewClient(cfg config.Config, log *zap.Logger) (Client, error) {
	return &client{
		cfg: cfg,
		log: log,
	}, nil
}

func (c *client) buildTLS() (*tls.Config, error) {

	ca, err := os.ReadFile(c.cfg.MQTT.TLS.CAFile)
	if err != nil {
		return nil, fmt.Errorf("CA error: %w", err)
	}

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(ca)

	cert, err := tls.LoadX509KeyPair(
		c.cfg.MQTT.TLS.CertFile,
		c.cfg.MQTT.TLS.KeyFile,
	)
	if err != nil {
		return nil, fmt.Errorf("cert error: %w", err)
	}

	return &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func (c *client) Connect(ctx context.Context) error {

	tlsConfig, err := c.buildTLS()
	if err != nil {
		return err
	}

	opts := mqtt.NewClientOptions()

	opts.AddBroker(c.cfg.MQTT.Endpoint)
	opts.SetClientID(c.cfg.MQTT.ClientID)

	opts.SetTLSConfig(tlsConfig)

	opts.SetKeepAlive(time.Duration(c.cfg.MQTT.KeepAlive) * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(3 * time.Second)
	opts.SetCleanSession(true)

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		c.log.Info("MQTT conectado")
	})

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		c.log.Error("MQTT desconectado", zap.Error(err))
	})

	c.client = mqtt.NewClient(opts)

	token := c.client.Connect()
	token.Wait()

	if err := token.Error(); err != nil {
		return fmt.Errorf("MQTT connect error: %w", err)
	}

	return nil
}

func (c *client) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {

	token := c.client.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {

		_ = handler(ctx, msg.Topic(), msg.Payload())
	})

	token.Wait()
	return token.Error()
}

func (c *client) Publish(ctx context.Context, topic string, payload []byte) error {

	token := c.client.Publish(topic, 1, false, payload)
	token.Wait()

	return token.Error()
}

func (c *client) Disconnect(ctx context.Context) error {

	if c.client == nil {
		return nil
	}

	c.client.Disconnect(250)
	return nil
}

func (c *client) IsConnected() bool {
	if c.client == nil {
		return false
	}
	return c.client.IsConnected()
}
