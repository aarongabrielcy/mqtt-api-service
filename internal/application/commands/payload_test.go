package commands

import (
	"encoding/json"
	"testing"
)

var testParseCfg = ParseConfig{TemperatureMinC: 16, TemperatureMaxC: 30}

func TestParseCommandPayload_Power(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{"bool true", `{"power":true}`, true},
		{"bool false", `{"power":false}`, false},
		{"string true", `{"power":"true"}`, true},
		{"string on", `{"power":"on"}`, true},
		{"string off", `{"power":"off"}`, false},
		{"string 1", `{"power":"1"}`, true},
		{"string 0", `{"power":"0"}`, false},
		{"number 1", `{"power":1}`, true},
		{"number 0", `{"power":0}`, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := ParseCommandPayload(CommandKeyPower, 201, json.RawMessage(c.raw), testParseCfg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.Power != c.want {
				t.Errorf("Power = %v, want %v", p.Power, c.want)
			}
		})
	}
}

func TestParseCommandPayload_Power_LegacyCommandsMap(t *testing.T) {
	raw := json.RawMessage(`{"commandId":"cmd_1","source":"manual","commands":{"201":"1"}}`)

	p, err := ParseCommandPayload(CommandKeyPower, 201, raw, testParseCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Power {
		t.Error("Power = false, want true from legacy commands map")
	}
}

func TestParseCommandPayload_Power_WrapperFormats(t *testing.T) {
	cases := []string{
		`{"commandPayload":{"power":true}}`,
		`{"data":{"power":true}}`,
	}

	for _, raw := range cases {
		p, err := ParseCommandPayload(CommandKeyPower, 201, json.RawMessage(raw), testParseCfg)
		if err != nil {
			t.Fatalf("unexpected error for %s: %v", raw, err)
		}
		if !p.Power {
			t.Errorf("Power = false, want true for wrapper payload %s", raw)
		}
	}
}

func TestParseCommandPayload_Power_InvalidType(t *testing.T) {
	if _, err := ParseCommandPayload(CommandKeyPower, 201, json.RawMessage(`{"power":"maybe"}`), testParseCfg); err == nil {
		t.Error("expected error for invalid power value")
	}
}

func TestParseCommandPayload_Temperature_NumberAndString(t *testing.T) {
	p, err := ParseCommandPayload(CommandKeyTemperature, 202, json.RawMessage(`{"temperature":24}`), testParseCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Temperature != 24 {
		t.Errorf("Temperature = %v, want 24", p.Temperature)
	}

	p, err = ParseCommandPayload(CommandKeyTemperature, 202, json.RawMessage(`{"temperature":"22.5"}`), testParseCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Temperature != 22.5 {
		t.Errorf("Temperature = %v, want 22.5", p.Temperature)
	}
}

func TestParseCommandPayload_Temperature_OutOfRange(t *testing.T) {
	cases := []string{`{"temperature":15}`, `{"temperature":31}`}
	for _, raw := range cases {
		if _, err := ParseCommandPayload(CommandKeyTemperature, 202, json.RawMessage(raw), testParseCfg); err == nil {
			t.Errorf("expected out-of-range error for %s", raw)
		}
	}
}

func TestParseCommandPayload_Mode_ValidAndInvalid(t *testing.T) {
	for _, mode := range []string{"COOL", "AUTO", "FAN", "AIR_DRY", "cool"} {
		p, err := ParseCommandPayload(CommandKeyMode, 203, json.RawMessage(`{"mode":"`+mode+`"}`), testParseCfg)
		if err != nil {
			t.Fatalf("unexpected error for mode %q: %v", mode, err)
		}
		if p.Mode == "" {
			t.Errorf("Mode empty for input %q", mode)
		}
	}

	if _, err := ParseCommandPayload(CommandKeyMode, 203, json.RawMessage(`{"mode":"TURBO"}`), testParseCfg); err == nil {
		t.Error("expected error for invalid mode TURBO")
	}
}

func TestParseCommandPayload_Airflow_ValidAndInvalid(t *testing.T) {
	for _, strength := range []string{"LOW", "MID", "HIGH", "AUTO"} {
		p, err := ParseCommandPayload(CommandKeyAirflow, 204, json.RawMessage(`{"strength":"`+strength+`"}`), testParseCfg)
		if err != nil {
			t.Fatalf("unexpected error for strength %q: %v", strength, err)
		}
		if p.Strength != strength {
			t.Errorf("Strength = %q, want %q", p.Strength, strength)
		}
	}

	if _, err := ParseCommandPayload(CommandKeyAirflow, 204, json.RawMessage(`{"strength":"TURBO"}`), testParseCfg); err == nil {
		t.Error("expected error for invalid strength TURBO")
	}
}

func TestParseCommandPayload_Oscillation_BoolAndString(t *testing.T) {
	p, err := ParseCommandPayload(CommandKeyOscillation, 205, json.RawMessage(`{"enabled":true}`), testParseCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Enabled {
		t.Error("Enabled = false, want true")
	}

	p, err = ParseCommandPayload(CommandKeyOscillation, 205, json.RawMessage(`{"enabled":"off"}`), testParseCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Enabled {
		t.Error("Enabled = true, want false")
	}
}

func TestParseCommandPayload_PowerSave_BoolAndString(t *testing.T) {
	p, err := ParseCommandPayload(CommandKeyPowerSave, 206, json.RawMessage(`{"enabled":"1"}`), testParseCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Enabled {
		t.Error("Enabled = false, want true")
	}
}

func TestParseCommandPayload_MissingField(t *testing.T) {
	if _, err := ParseCommandPayload(CommandKeyPower, 201, json.RawMessage(`{}`), testParseCfg); err == nil {
		t.Error("expected error for missing power field")
	}
}

func TestParseCommandPayload_UnsupportedCommandKey(t *testing.T) {
	if _, err := ParseCommandPayload("lg.unknown", 0, json.RawMessage(`{}`), testParseCfg); err == nil {
		t.Error("expected error for unsupported command key")
	}
}
