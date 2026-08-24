package lg_service

import (
	"errors"
	"testing"

	"mqtt-api-service/internal/adapters/api/lg"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// newObservedLGService construye un LGService mínimo (solo con logger) para
// aislar logControlError sin necesitar el resto de las dependencias
// concretas del servicio (*lg.DeviceService, Mongo, Redis, etc.) — ver la
// nota en README.md sobre por qué refreshDeviceStates no tiene test de
// integración: logControlError es, en cambio, una pieza pura que sí se
// puede aislar sin ese refactor.
func newObservedLGService() (*LGService, *observer.ObservedLogs) {
	core, logs := observer.New(zap.WarnLevel)
	return &LGService{log: zap.New(core)}, logs
}

func TestLogControlError_DeviceDisconnected_LogsWarnNotError(t *testing.T) {
	s, logs := newObservedLGService()

	apiErr := &lg.APIError{StatusCode: 416, Code: "1222"}
	s.logControlError(apiErr, "failed to set device power", zap.String("deviceID", "dev-1"))

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
	}
	if entries[0].Level != zap.WarnLevel {
		t.Errorf("level = %v, want Warn", entries[0].Level)
	}
	if entries[0].Message != "device disconnected" {
		t.Errorf("message = %q, want %q", entries[0].Message, "device disconnected")
	}
}

func TestLogControlError_OtherAPIError_LogsError(t *testing.T) {
	s, logs := newObservedLGService()

	// Un lg.APIError que NO es "not connected" (ej. 500 real de la LG API)
	// nunca debe clasificarse como el warn de desconexión — antes del fix,
	// el fmt.Printf original imprimía "desconectado" para CUALQUIER
	// lg.APIError, sin importar el status/code real.
	apiErr := &lg.APIError{StatusCode: 500, Code: "9999"}
	s.logControlError(apiErr, "failed to set device power", zap.String("deviceID", "dev-2"))

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
	}
	if entries[0].Level != zap.ErrorLevel {
		t.Errorf("level = %v, want Error", entries[0].Level)
	}
	if entries[0].Message != "failed to set device power" {
		t.Errorf("message = %q, want the action string", entries[0].Message)
	}
}

func TestLogControlError_GenericError_LogsError(t *testing.T) {
	s, logs := newObservedLGService()

	s.logControlError(errors.New("boom"), "failed to set air flow", zap.String("deviceID", "dev-3"))

	entries := logs.All()
	if len(entries) != 1 || entries[0].Level != zap.ErrorLevel {
		t.Fatalf("expected exactly 1 Error-level entry, got %+v", entries)
	}
}
