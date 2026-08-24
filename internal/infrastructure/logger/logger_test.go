package logger

import (
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestParseLevel_Debug(t *testing.T) {
	if got := parseLevel("debug"); got != zapcore.DebugLevel {
		t.Errorf("parseLevel(%q) = %v, want DebugLevel", "debug", got)
	}
}

func TestParseLevel_CaseInsensitive(t *testing.T) {
	for _, in := range []string{"DEBUG", "Debug", "  debug  "} {
		if got := parseLevel(in); got != zapcore.DebugLevel {
			t.Errorf("parseLevel(%q) = %v, want DebugLevel", in, got)
		}
	}
}

func TestParseLevel_Info(t *testing.T) {
	if got := parseLevel("info"); got != zapcore.InfoLevel {
		t.Errorf("parseLevel(%q) = %v, want InfoLevel", "info", got)
	}
}

func TestParseLevel_EmptyFallsBackToInfo(t *testing.T) {
	if got := parseLevel(""); got != zapcore.InfoLevel {
		t.Errorf("parseLevel(\"\") = %v, want InfoLevel", got)
	}
}

func TestParseLevel_InvalidFallsBackToInfo(t *testing.T) {
	if got := parseLevel("not-a-level"); got != zapcore.InfoLevel {
		t.Errorf("parseLevel(%q) = %v, want InfoLevel fallback", "not-a-level", got)
	}
}

// TestNewLogger_DebugLevel_ActuallyEnablesDebugLogging cubre la causa raíz
// de FASE LG-CMD-2G: antes de este fix, NewLogger ignoraba el parámetro
// level por completo y el logger quedaba fijo en Info sin importar
// LOG_LEVEL — este test falla si esa regresión vuelve a introducirse.
func TestNewLogger_DebugLevel_ActuallyEnablesDebugLogging(t *testing.T) {
	log := NewLogger("debug")
	defer log.Sync()

	if !log.Core().Enabled(zapcore.DebugLevel) {
		t.Error("logger built with level=debug should have DebugLevel enabled")
	}
}

func TestNewLogger_InfoLevel_DisablesDebugLogging(t *testing.T) {
	log := NewLogger("info")
	defer log.Sync()

	if log.Core().Enabled(zapcore.DebugLevel) {
		t.Error("logger built with level=info should NOT have DebugLevel enabled")
	}
	if !log.Core().Enabled(zapcore.InfoLevel) {
		t.Error("logger built with level=info should have InfoLevel enabled")
	}
}

func TestNewLogger_DefaultLevel_UnsetFallsBackToInfoNotDebug(t *testing.T) {
	log := NewLogger("")
	defer log.Sync()

	if log.Core().Enabled(zapcore.DebugLevel) {
		t.Error("logger built with an empty level should NOT have DebugLevel enabled (default is info)")
	}
}
