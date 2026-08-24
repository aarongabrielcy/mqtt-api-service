package interfaces

import (
	"context"
	"time"
)

// IngestRawInput es lo que un adapter de salida (gRPC hacia
// tracking-platform) necesita para reenviar un mensaje ya normalizado.
// Payload debe ser JSON directo compatible con Payload Profiles: sin
// wrapper de topic/deviceID y sin el raw completo de LG (eso se conserva
// aparte en Mongo, ver internal/adapters/mongo).
type IngestRawInput struct {
	Topic      string
	Payload    []byte
	ReceivedAt time.Time
}

type TrackingClient interface {
	IngestRaw(ctx context.Context, input IngestRawInput) error
}
