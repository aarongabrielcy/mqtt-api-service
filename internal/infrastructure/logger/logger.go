package logger

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger construye el logger zap de producción (JSON, timestamps
// ISO8601) con el nivel resuelto desde level (App.LogLevel / LOG_LEVEL).
//
// FASE LG-CMD-2G: antes de este fix, el parámetro level se ignoraba por
// completo — zap.NewProductionConfig() siempre queda en su default (Info) a
// menos que se sobreescriba explícitamente su Level, y este código nunca lo
// hacía. Como consecuencia, LOG_LEVEL=debug nunca habilitaba ningún
// zap.Debug(...) en todo el servicio, incluida la instrumentación de
// diagnóstico agregada en FASE LG-CMD-2E ("LG raw state response", "LG
// state parsed", etc.) — confirmado como la causa exacta de que esos logs
// nunca aparecieran a pesar de que LOG_LEVEL=debug y LG_DEBUG_STATE_LOGS=true
// llegaban correctamente al contenedor.
func NewLogger(level string) *zap.Logger {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(parseLevel(level))

	logger, err := config.Build()
	if err != nil {
		// Fallback seguro: un LOG_LEVEL inválido nunca debe impedir el
		// arranque del servicio por un logger mal configurado.
		fallback, _ := zap.NewProduction()
		return fallback
	}
	return logger
}

// parseLevel resuelve level (case-insensitive, ver zapcore.ParseLevel) sin
// fallar nunca: un valor vacío o no reconocido cae a Info, igual que hacía
// zap.NewProductionConfig() por defecto antes de este fix.
func parseLevel(level string) zapcore.Level {
	parsed, err := zapcore.ParseLevel(strings.TrimSpace(level))
	if err != nil {
		return zapcore.InfoLevel
	}
	return parsed
}
