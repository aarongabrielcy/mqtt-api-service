package commands

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Payload es el valor ya validado y tipado para un commandKey LG. Solo el
// campo relevante para ese commandKey se completa; los demás quedan en su
// zero-value.
type Payload struct {
	Power       bool
	Temperature float64
	Mode        string
	Strength    string
	Enabled     bool
}

// ParseConfig trae los límites configurables necesarios para validar el
// payload (hoy solo el rango de temperatura, ver LG_COMMAND_TEMPERATURE_*).
type ParseConfig struct {
	TemperatureMinC float64
	TemperatureMaxC float64
}

var validModes = map[string]bool{
	"COOL":    true,
	"AUTO":    true,
	"FAN":     true,
	"AIR_DRY": true,
}

var validAirflowStrengths = map[string]bool{
	"LOW":  true,
	"MID":  true,
	"HIGH": true,
	"AUTO": true,
}

// ParseCommandPayload extrae y valida el valor necesario para commandKey a
// partir del payload crudo del evento Kafka. Acepta payload directo
// ({"power":true}), envuelto ({"commandPayload":{...}} o {"data":{...}}), o
// el formato legacy de tracking-platform ({"commands":{"<commandCode>":"1"}}).
func ParseCommandPayload(commandKey string, commandCode int, raw json.RawMessage, cfg ParseConfig) (Payload, error) {
	switch commandKey {
	case CommandKeyPower:
		v, err := requireField(raw, "power", commandCode)
		if err != nil {
			return Payload{}, err
		}
		b, err := coerceBool(v)
		if err != nil {
			return Payload{}, fmt.Errorf("%s: %w", commandKey, err)
		}
		return Payload{Power: b}, nil

	case CommandKeyTemperature:
		v, err := requireField(raw, "temperature", commandCode)
		if err != nil {
			return Payload{}, err
		}
		f, err := coerceFloat(v)
		if err != nil {
			return Payload{}, fmt.Errorf("%s: %w", commandKey, err)
		}
		if f < cfg.TemperatureMinC || f > cfg.TemperatureMaxC {
			return Payload{}, fmt.Errorf("%s: temperature %.1f out of range [%.1f,%.1f]", commandKey, f, cfg.TemperatureMinC, cfg.TemperatureMaxC)
		}
		return Payload{Temperature: f}, nil

	case CommandKeyMode:
		v, err := requireField(raw, "mode", commandCode)
		if err != nil {
			return Payload{}, err
		}
		s, err := coerceString(v)
		if err != nil {
			return Payload{}, fmt.Errorf("%s: %w", commandKey, err)
		}
		s = strings.ToUpper(strings.TrimSpace(s))
		if !validModes[s] {
			return Payload{}, fmt.Errorf("%s: invalid mode %q", commandKey, s)
		}
		return Payload{Mode: s}, nil

	case CommandKeyAirflow:
		v, err := requireField(raw, "strength", commandCode)
		if err != nil {
			return Payload{}, err
		}
		s, err := coerceString(v)
		if err != nil {
			return Payload{}, fmt.Errorf("%s: %w", commandKey, err)
		}
		s = strings.ToUpper(strings.TrimSpace(s))
		if !validAirflowStrengths[s] {
			return Payload{}, fmt.Errorf("%s: invalid strength %q", commandKey, s)
		}
		return Payload{Strength: s}, nil

	case CommandKeyOscillation, CommandKeyPowerSave:
		v, err := requireField(raw, "enabled", commandCode)
		if err != nil {
			return Payload{}, err
		}
		b, err := coerceBool(v)
		if err != nil {
			return Payload{}, fmt.Errorf("%s: %w", commandKey, err)
		}
		return Payload{Enabled: b}, nil

	default:
		return Payload{}, fmt.Errorf("unsupported command key %q", commandKey)
	}
}

func requireField(raw json.RawMessage, fieldName string, commandCode int) (any, error) {
	v, ok, err := extractFieldValue(raw, fieldName, commandCode)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("missing field %q in command payload", fieldName)
	}
	return v, nil
}

// extractFieldValue busca fieldName en el payload, en este orden:
// directo a nivel raíz, dentro de un wrapper ("commandPayload" o "data"), o
// como el valor legacy en commands[<commandCode>] (formato actual del
// endpoint manual de tracking-platform: {"commands":{"201":"1"}}).
func extractFieldValue(raw json.RawMessage, fieldName string, commandCode int) (any, bool, error) {
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, false, fmt.Errorf("invalid command payload: %w", err)
	}

	container := generic
	if wrapped, ok := generic["commandPayload"].(map[string]any); ok {
		container = wrapped
	} else if wrapped, ok := generic["data"].(map[string]any); ok {
		container = wrapped
	}

	if v, ok := container[fieldName]; ok {
		return v, true, nil
	}

	if commandsMap, ok := generic["commands"].(map[string]any); ok {
		if v, ok := commandsMap[strconv.Itoa(commandCode)]; ok {
			return v, true, nil
		}
	}

	return nil, false, nil
}

func coerceBool(v any) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case float64:
		if t == 1 {
			return true, nil
		}
		if t == 0 {
			return false, nil
		}
		return false, fmt.Errorf("invalid boolean numeric value %v", t)
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "on", "1":
			return true, nil
		case "false", "off", "0":
			return false, nil
		}
		return false, fmt.Errorf("invalid boolean string value %q", t)
	default:
		return false, fmt.Errorf("unsupported boolean type %T", v)
	}
}

func coerceFloat(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil {
			return 0, fmt.Errorf("invalid numeric value %q", t)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", v)
	}
}

func coerceString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("unsupported string type %T", v)
	}
	return s, nil
}
