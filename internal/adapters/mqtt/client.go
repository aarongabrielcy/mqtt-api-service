package mqtt

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"

	"mqtt-api-service/internal/infrastructure/config"
)

type MessageHandler func(ctx context.Context, topic string, payload []byte) error

// Subscription es una suscripción requerida del transporte push-MQTT LG. Se
// registran al construir el cliente y se (re)instalan en cada conexión
// exitosa (FR-04): con CleanSession=true el broker descarta las
// suscripciones en cada desconexión y paho no las reanuda (ResumeSubs=false).
type Subscription struct {
	Topic   string
	QoS     byte
	Handler MessageHandler
}

type Client interface {
	// Connect hace el primer intento de conexión (bloqueante) y arranca la
	// goroutine supervisora. Si ese intento falla no devuelve error: el
	// supervisor reintenta con ReconnectPolicy. Solo devuelve error si el
	// cliente no se puede preparar (TLS). Las suscripciones se instalan desde
	// el handler OnConnect en cada conexión exitosa.
	Connect(ctx context.Context) error
	Publish(ctx context.Context, topic string, payload []byte) error
	// Disconnect detiene el supervisor de reconexión y, si hay una conexión
	// activa, envía un DISCONNECT MQTT acotado. Es idempotente.
	Disconnect(ctx context.Context) error
	IsConnected() bool
}

const (
	// disconnectQuiesceMillis es cuánto espera paho a que termine el trabajo
	// en curso antes de cerrar (argumento de paho Disconnect).
	disconnectQuiesceMillis = 250
	// disconnectTimeout acota el Disconnect completo (FR-05).
	disconnectTimeout = 2 * time.Second
	// subscribeTimeout acota cada Subscribe dentro de OnConnect.
	subscribeTimeout = 10 * time.Second
	// connectTimeout acota cada intento de conexión (inicial y reconexión).
	connectTimeout = 30 * time.Second
)

// pahoClient es el subconjunto de mqtt.Client que usa este adapter; permite
// sustituir paho en tests sin contactar al broker LG.
type pahoClient interface {
	Connect() mqtt.Token
	Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token
	Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token
	Disconnect(quiesce uint)
	IsConnected() bool
}

type client struct {
	log    *zap.Logger
	cfg    config.Config
	subs   []Subscription
	policy ReconnectPolicy

	fingerprint string

	// Puntos de inyección para tests.
	buildTLS func() (*tls.Config, error)
	newPaho  func(opts *mqtt.ClientOptions) pahoClient
	after    func(d time.Duration) <-chan time.Time
	now      func() time.Time

	mu          sync.Mutex
	paho        pahoClient
	connectedAt time.Time
	started     bool
	stopped     bool

	lost chan error
	stop chan struct{}
	done chan struct{}

	ctx context.Context
}

func NewClient(cfg config.Config, log *zap.Logger, subs []Subscription) (Client, error) {
	c := newClient(cfg, log, subs, DefaultReconnectPolicy)
	c.buildTLS = c.buildTLSFromFiles
	return c, nil
}

func newClient(cfg config.Config, log *zap.Logger, subs []Subscription, policy ReconnectPolicy) *client {
	return &client{
		cfg:         cfg,
		log:         log,
		subs:        subs,
		policy:      policy,
		fingerprint: ClientIDFingerprint(cfg.MQTT.ClientID),
		newPaho: func(opts *mqtt.ClientOptions) pahoClient {
			return mqtt.NewClient(opts)
		},
		after: time.After,
		now:   time.Now,
		lost:  make(chan error, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		ctx:   context.Background(),
	}
}

// ClientIDFingerprint devuelve una huella corta no reversible del Client ID
// MQTT (primeros 12 hex de SHA-256), para diagnósticos y comparación entre
// entornos sin exponer el valor real (FR-07).
func ClientIDFingerprint(clientID string) string {
	sum := sha256.Sum256([]byte(clientID))
	return hex.EncodeToString(sum[:])[:12]
}

// RedactTopic reemplaza el segmento de client id de
// app/clients/<id>/<kind> por su huella, para poder loguear topics LG sin
// exponer el identificador real (que en runtime puede coincidir con
// LG_MQTT_CLIENT_ID).
func RedactTopic(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) >= 4 && parts[0] == "app" && parts[1] == "clients" {
		parts[2] = "fp:" + ClientIDFingerprint(parts[2])
		return strings.Join(parts, "/")
	}
	return topic
}

func (c *client) buildTLSFromFiles() (*tls.Config, error) {

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
		c.log.Error("LG push MQTT initial connect failed",
			zap.String("stage", "tls"),
			zap.String("clientIdFingerprint", c.fingerprint),
			zap.Error(err),
		)
		return err
	}

	opts := mqtt.NewClientOptions()

	opts.AddBroker(c.cfg.MQTT.Endpoint)
	opts.SetClientID(c.cfg.MQTT.ClientID)

	opts.SetTLSConfig(tlsConfig)

	opts.SetKeepAlive(time.Duration(c.cfg.MQTT.KeepAlive) * time.Second)
	opts.SetConnectTimeout(connectTimeout)
	opts.SetCleanSession(true)
	// El reconnect lo maneja la goroutine supervisora con ReconnectPolicy
	// (FR-06); el de paho se reinicia en cada conexión exitosa.
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)

	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		c.installSubscriptions()
	})

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.onConnectionLost(err)
	})

	c.mu.Lock()
	c.ctx = ctx
	c.paho = c.newPaho(opts)
	c.mu.Unlock()

	c.log.Info("LG push MQTT connecting",
		zap.String("clientIdFingerprint", c.fingerprint),
		zap.Int("subscriptions", len(c.subs)),
	)

	// Un fallo del primer intento (p. ej. EOF del broker) no es fatal: se
	// entrega a la misma goroutine supervisora, que reintenta con el mismo
	// backoff que usa para las pérdidas de conexión.
	initialErr := c.attemptConnect()
	if initialErr != nil {
		c.log.Error("LG push MQTT initial connect failed",
			zap.String("stage", "connect"),
			zap.String("clientIdFingerprint", c.fingerprint),
			zap.Error(initialErr),
		)
	} else {
		c.log.Info("LG push MQTT connected",
			zap.Bool("reconnect", false),
			zap.String("clientIdFingerprint", c.fingerprint),
		)
	}

	c.mu.Lock()
	c.started = true
	c.mu.Unlock()

	go c.supervise(initialErr)

	return nil
}

// attemptConnect hace un único intento de conexión acotado por
// connectTimeout y, si tiene éxito, marca el inicio de la conexión.
func (c *client) attemptConnect() error {
	token := c.paho.Connect()
	if !token.WaitTimeout(connectTimeout + 5*time.Second) {
		return fmt.Errorf("connect timed out after %s", connectTimeout)
	}
	if err := token.Error(); err != nil {
		return err
	}

	c.mu.Lock()
	c.connectedAt = c.now()
	c.mu.Unlock()

	return nil
}

// installSubscriptions (re)instala todas las suscripciones requeridas. Se
// llama solo desde OnConnect, así que corre una vez por conexión exitosa
// (inicial o reconexión) y nunca dos veces en el arranque.
func (c *client) installSubscriptions() {
	c.mu.Lock()
	p := c.paho
	ctx := c.ctx
	c.mu.Unlock()

	installed := 0

	for _, sub := range c.subs {
		handler := sub.Handler
		token := p.Subscribe(sub.Topic, sub.QoS, func(_ mqtt.Client, msg mqtt.Message) {
			_ = handler(ctx, msg.Topic(), msg.Payload())
		})

		var err error
		if !token.WaitTimeout(subscribeTimeout) {
			err = fmt.Errorf("subscribe timed out after %s", subscribeTimeout)
		} else {
			err = token.Error()
		}

		if err != nil {
			c.log.Error("LG push MQTT subscribe failed",
				zap.String("topic", RedactTopic(sub.Topic)),
				zap.Int("qos", int(sub.QoS)),
				zap.Error(err),
			)
			continue
		}

		installed++
	}

	c.log.Info("LG push MQTT subscriptions installed",
		zap.Int("installed", installed),
		zap.Int("required", len(c.subs)),
		zap.String("clientIdFingerprint", c.fingerprint),
	)
}

func (c *client) onConnectionLost(err error) {
	c.mu.Lock()
	stopped := c.stopped
	c.mu.Unlock()

	if stopped {
		return
	}

	select {
	case c.lost <- err:
	default:
	}
}

// supervise es la única goroutine que reconecta. Si el primer intento falló
// (initialErr != nil) arranca directamente en el ciclo de reintento; luego
// cada pérdida de conexión dispara el mismo ciclo acotado por
// ReconnectPolicy. Disconnect lo detiene en cualquier punto.
func (c *client) supervise(initialErr error) {
	defer close(c.done)

	backoff := newReconnectBackoff(c.policy)

	if initialErr != nil {
		if !c.retryUntilConnected(backoff, backoff.attemptFailed()) {
			return
		}
	}

	for {
		select {
		case <-c.stop:
			return
		case err := <-c.lost:
			if !c.reconnect(backoff, err) {
				return
			}
		}
	}
}

// reconnect maneja una pérdida de conexión hasta reconectar. Devuelve false
// si el cliente se detuvo mientras esperaba.
func (c *client) reconnect(backoff *reconnectBackoff, lostErr error) bool {
	c.mu.Lock()
	connectedFor := c.now().Sub(c.connectedAt)
	c.mu.Unlock()

	delay, shortLived, streak := backoff.connectionLost(connectedFor)

	c.log.Warn("LG push MQTT connection lost",
		zap.Duration("connectedFor", connectedFor),
		zap.Bool("shortLived", shortLived),
		zap.Int("shortLivedStreak", streak),
		zap.String("clientIdFingerprint", c.fingerprint),
		zap.Error(lostErr),
	)

	if backoff.unstable() {
		c.log.Warn("LG push MQTT rapid connection instability: possible duplicate LG_MQTT_CLIENT_ID in another active runtime (heuristic, not proven)",
			zap.Int("shortLivedStreak", streak),
			zap.Int("threshold", c.policy.InstabilityThreshold),
			zap.Duration("stableConnection", c.policy.StableConnection),
			zap.String("clientIdFingerprint", c.fingerprint),
		)
	}

	return c.retryUntilConnected(backoff, delay)
}

// retryUntilConnected es el único ciclo de reintento: espera delay, intenta
// conectar y, si falla, espera el siguiente retraso del backoff (x2 hasta
// MaxDelay). Devuelve false si Disconnect lo detuvo.
func (c *client) retryUntilConnected(backoff *reconnectBackoff, delay time.Duration) bool {
	for attempt := 1; ; attempt++ {
		c.log.Info("LG push MQTT reconnect scheduled",
			zap.Int("attempt", attempt),
			zap.Duration("delay", delay),
			zap.Duration("maxDelay", c.policy.MaxDelay),
		)

		select {
		case <-c.stop:
			return false
		case <-c.after(delay):
		}

		if err := c.attemptConnect(); err != nil {
			delay = backoff.attemptFailed()
			c.log.Warn("LG push MQTT reconnect attempt failed",
				zap.Int("attempt", attempt),
				zap.String("clientIdFingerprint", c.fingerprint),
				zap.Error(err),
			)
			continue
		}

		c.mu.Lock()
		stopped := c.stopped
		c.mu.Unlock()

		if stopped {
			// Disconnect llegó durante el intento: no dejar una sesión
			// abierta tras el shutdown.
			c.paho.Disconnect(disconnectQuiesceMillis)
			return false
		}

		c.log.Info("LG push MQTT connected",
			zap.Bool("reconnect", true),
			zap.Int("attempt", attempt),
			zap.String("clientIdFingerprint", c.fingerprint),
		)
		return true
	}
}

func (c *client) Publish(ctx context.Context, topic string, payload []byte) error {

	token := c.paho.Publish(topic, 1, false, payload)
	token.Wait()

	return token.Error()
}

func (c *client) Disconnect(ctx context.Context) error {

	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	p := c.paho
	started := c.started
	c.mu.Unlock()

	close(c.stop)

	if started {
		select {
		case <-c.done:
		case <-time.After(disconnectTimeout):
			c.log.Warn("LG push MQTT reconnect supervisor did not stop in time",
				zap.Duration("timeout", disconnectTimeout),
			)
		}
	}

	if p == nil || !p.IsConnected() {
		c.log.Info("LG push MQTT stopped (no active connection)")
		return nil
	}

	finished := make(chan struct{})
	go func() {
		p.Disconnect(disconnectQuiesceMillis)
		close(finished)
	}()

	select {
	case <-finished:
		c.log.Info("LG push MQTT disconnected gracefully",
			zap.String("clientIdFingerprint", c.fingerprint),
		)
		return nil
	case <-time.After(disconnectTimeout):
		c.log.Warn("LG push MQTT graceful disconnect timed out",
			zap.Duration("timeout", disconnectTimeout),
		)
		return fmt.Errorf("MQTT disconnect timed out after %s", disconnectTimeout)
	}
}

func (c *client) IsConnected() bool {
	c.mu.Lock()
	p := c.paho
	c.mu.Unlock()

	if p == nil {
		return false
	}
	return p.IsConnected()
}
