package mqtt

import "time"

// ReconnectPolicy define los límites explícitos y finitos del reconnect del
// transporte push-MQTT LG (MQTT-API-LG-PUSH-RECONNECT-RESILIENCE-1, FR-06).
//
// El auto-reconnect interno de paho se desactiva: su backoff se reinicia en
// cada conexión exitosa, así que un ciclo "conecta y el broker corta a los
// pocos segundos" (p. ej. otro runtime activo con el mismo
// LG_MQTT_CLIENT_ID) terminaba en ~490 reconexiones/hora. Con esta política
// una conexión que no llega a StableConnection NO reinicia el backoff: el
// retraso sigue duplicándose hasta MaxDelay.
type ReconnectPolicy struct {
	// InitialDelay es el retraso mínimo antes de cualquier intento de
	// reconexión (nunca se reintenta en un loop apretado).
	InitialDelay time.Duration

	// MaxDelay es el techo del backoff exponencial (x2 por intento).
	MaxDelay time.Duration

	// StableConnection es cuánto debe durar una conexión para considerarse
	// estable y reiniciar el backoff. Pérdidas antes de este umbral cuentan
	// como conexiones de vida corta.
	StableConnection time.Duration

	// InstabilityThreshold es cuántas conexiones de vida corta consecutivas
	// disparan el diagnóstico heurístico de posible Client ID duplicado.
	InstabilityThreshold int
}

// DefaultReconnectPolicy son los límites usados en runtime. Con churn
// persistente el ritmo converge a 1 intento cada 5 minutos (≈12/hora).
var DefaultReconnectPolicy = ReconnectPolicy{
	InitialDelay:         2 * time.Second,
	MaxDelay:             5 * time.Minute,
	StableConnection:     60 * time.Second,
	InstabilityThreshold: 3,
}

// maxBackoffExponent evita overflow al desplazar InitialDelay; 2s<<30 ya
// supera cualquier MaxDelay razonable.
const maxBackoffExponent = 30

// reconnectBackoff es el estado del backoff; no es concurrente (solo lo usa
// la goroutine supervisora del cliente).
type reconnectBackoff struct {
	policy           ReconnectPolicy
	exponent         int
	shortLivedStreak int
}

func newReconnectBackoff(policy ReconnectPolicy) *reconnectBackoff {
	return &reconnectBackoff{policy: policy}
}

// connectionLost registra una pérdida de conexión que duró connectedFor y
// devuelve el retraso antes del primer intento de reconexión, si la
// conexión fue de vida corta y la racha actual de conexiones cortas.
func (b *reconnectBackoff) connectionLost(connectedFor time.Duration) (delay time.Duration, shortLived bool, streak int) {
	shortLived = connectedFor < b.policy.StableConnection
	if shortLived {
		b.shortLivedStreak++
	} else {
		b.exponent = 0
		b.shortLivedStreak = 0
	}
	return b.next(), shortLived, b.shortLivedStreak
}

// attemptFailed devuelve el retraso antes del siguiente intento tras un
// intento de reconexión fallido.
func (b *reconnectBackoff) attemptFailed() time.Duration {
	return b.next()
}

// unstable indica si la racha actual alcanzó InstabilityThreshold.
func (b *reconnectBackoff) unstable() bool {
	return b.policy.InstabilityThreshold > 0 && b.shortLivedStreak >= b.policy.InstabilityThreshold
}

func (b *reconnectBackoff) next() time.Duration {
	d := b.policy.InitialDelay << b.exponent
	if d > b.policy.MaxDelay || d <= 0 {
		d = b.policy.MaxDelay
	}
	if d < b.policy.InitialDelay {
		d = b.policy.InitialDelay
	}
	if b.exponent < maxBackoffExponent {
		b.exponent++
	}
	return d
}
