package client

import (
	"context"
	"fmt"
	"time"

	trackingpb "mqtt-api-service/internal/adapters/grpc/proto/tracking"
	"mqtt-api-service/internal/domain/interfaces"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IngestRaw envía el mensaje ya normalizado a tracking-platform vía
// TrackingService.IngestRaw. input.Payload debe ser JSON directo (sin
// wrapper); el topic viaja en el campo dedicado del contrato, no dentro
// del payload.
//
// Cada intento usa un timeout explícito (c.requestTimeout, nunca
// context.Background sin deadline) y, ante errores transitorios
// (Unavailable/DeadlineExceeded — típicos de DNS Docker no resuelto todavía
// o ingestion-service arrancando), reintenta con backoff corto hasta
// c.maxAttempts. Errores no transitorios (payload inválido, etc.) no se
// reintentan.
func (c *Client) IngestRaw(
	ctx context.Context,
	input interfaces.IngestRawInput,
) error {

	req := &trackingpb.RawMessage{
		Topic:      input.Topic,
		Payload:    input.Payload,
		Qos:        -1,
		Retain:     false,
		ReceivedAt: input.ReceivedAt.Format(time.RFC3339),
	}

	backoff := c.initialBackoff
	var lastErr error

retryLoop:
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		rpcCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
		resp, err := c.tracking.IngestRaw(rpcCtx, req)
		cancel()

		if err == nil {
			c.log.Info(
				"tracking event ingested",
				zap.String("topic", input.Topic),
				zap.Bool("ok", resp.GetOk()),
				zap.Int("attempt", attempt),
			)
			return nil
		}

		lastErr = err

		if attempt == c.maxAttempts || !isTransientGRPCError(err) {
			break retryLoop
		}

		c.log.Warn(
			"tracking gRPC retry",
			zap.Int("attempt", attempt),
			zap.Int("maxAttempts", c.maxAttempts),
			zap.String("topic", input.Topic),
			zap.String("grpc_state", c.conn.GetState().String()),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			lastErr = ctx.Err()
			break retryLoop
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > c.maxBackoff {
			backoff = c.maxBackoff
		}
	}

	c.log.Error(
		"tracking publish failed after retries",
		zap.String("topic", input.Topic),
		zap.Int("maxAttempts", c.maxAttempts),
		zap.String("grpc_state", c.conn.GetState().String()),
		zap.Error(lastErr),
	)

	return fmt.Errorf(
		"failed to ingest tracking event after %d attempts: %w",
		c.maxAttempts,
		lastErr,
	)
}

// isTransientGRPCError determina si un error de IngestRaw amerita el
// reintento corto (DNS Docker todavía no resuelto, ingestion-service
// arrancando) o si es un error que no se resolvería reintentando.
func isTransientGRPCError(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
