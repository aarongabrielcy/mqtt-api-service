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

// subscription guarda lo necesario para volver a suscribirse a un topic
// cada vez que se (re)establece la conexión con el broker.
type subscription struct {
	topic   string
	handler MessageHandler
}

type client struct {
	log    *zap.Logger
	cfg    config.Config
	client mqtt.Client
	mu     sync.RWMutex

	subsMu sync.Mutex
	subs   []subscription
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

	// OnConnectHandler se dispara en la conexión inicial Y en cada
	// reconexión automática que haga paho. Como usamos CleanSession(true),
	// el broker NO recuerda las suscripciones entre reconexiones, así que
	// hay que re-suscribirse acá siempre, no solo una vez en main.go.
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		c.log.Info("MQTT conectado")
		// Pequeño delay antes de re-suscribir: suscribirse inmediatamente
		// tras el CONNACK parece hacer que el broker de LG corte la sesión
		// (ver EOF ~120ms después de cada conexión). Esto le da tiempo a
		// que la sesión quede asentada del lado del broker antes de
		// mandar los SUBSCRIBE.
		go func() {
			time.Sleep(1 * time.Second)
			c.resubscribeAll()
		}()
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

	// Guardamos la suscripción para poder reaplicarla automáticamente
	// cada vez que el cliente se reconecte (ver resubscribeAll).
	c.subsMu.Lock()
	c.subs = append(c.subs, subscription{topic: topic, handler: handler})
	c.subsMu.Unlock()

	return c.subscribeNow(ctx, topic, handler)
}

// subscribeNow hace la suscripción real contra el broker, sin tocar
// la lista de suscripciones registradas.
func (c *client) subscribeNow(ctx context.Context, topic string, handler MessageHandler) error {
	token := c.client.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		_ = handler(ctx, msg.Topic(), msg.Payload())
	})

	token.Wait()
	return token.Error()
}

// resubscribeAll reaplica todas las suscripciones conocidas. Se llama
// desde OnConnectHandler, tanto en la conexión inicial como en cada
// reconexión automática.
func (c *client) resubscribeAll() {
	c.subsMu.Lock()
	subs := make([]subscription, len(c.subs))
	copy(subs, c.subs)
	c.subsMu.Unlock()

	for _, s := range subs {
		if err := c.subscribeNow(context.Background(), s.topic, s.handler); err != nil {
			c.log.Error("Error re-suscribiendo a topic tras (re)conexión",
				zap.String("topic", s.topic), zap.Error(err))
			continue
		}
		c.log.Info("Suscrito a topic", zap.String("topic", s.topic))
	}
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
