package commands

import (
	"testing"

	commandsdomain "mqtt-api-service/internal/domain/commands"
)

func TestResolveCommandKey_FromMetadata(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{
		Metadata: &commandsdomain.DeviceCommandMetadata{CommandKey: CommandKeyPower},
	}

	key, ok := ResolveCommandKey(event)
	if !ok || key != CommandKeyPower {
		t.Fatalf("ResolveCommandKey = (%q, %v), want (%q, true)", key, ok, CommandKeyPower)
	}
}

func TestResolveCommandKey_FromTopLevelCommandKey(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{CommandKey: CommandKeyOscillation}

	key, ok := ResolveCommandKey(event)
	if !ok || key != CommandKeyOscillation {
		t.Fatalf("ResolveCommandKey = (%q, %v), want (%q, true)", key, ok, CommandKeyOscillation)
	}
}

func TestResolveCommandKey_FromCommandType(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{CommandType: CommandKeyMode}

	key, ok := ResolveCommandKey(event)
	if !ok || key != CommandKeyMode {
		t.Fatalf("ResolveCommandKey = (%q, %v), want (%q, true)", key, ok, CommandKeyMode)
	}
}

func TestResolveCommandKey_FromCommandCodeFallback(t *testing.T) {
	cases := []struct {
		code int
		want string
	}{
		{201, CommandKeyPower},
		{202, CommandKeyTemperature},
		{203, CommandKeyMode},
		{204, CommandKeyAirflow},
		{205, CommandKeyOscillation},
		{206, CommandKeyPowerSave},
	}

	for _, c := range cases {
		event := commandsdomain.DeviceCommandEvent{CommandCode: c.code}
		key, ok := ResolveCommandKey(event)
		if !ok || key != c.want {
			t.Errorf("commandCode %d: ResolveCommandKey = (%q, %v), want (%q, true)", c.code, key, ok, c.want)
		}
	}
}

func TestResolveCommandKey_MetadataTakesPriorityOverCommandCode(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{
		CommandCode: 201, // would resolve to lg.power via fallback
		Metadata:    &commandsdomain.DeviceCommandMetadata{CommandKey: CommandKeyMode},
	}

	key, ok := ResolveCommandKey(event)
	if !ok || key != CommandKeyMode {
		t.Fatalf("ResolveCommandKey = (%q, %v), want metadata to win with (%q, true)", key, ok, CommandKeyMode)
	}
}

func TestResolveCommandKey_Unsupported(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{CommandCode: 999, CommandType: "SOMETHING_ELSE"}

	if _, ok := ResolveCommandKey(event); ok {
		t.Fatal("ResolveCommandKey should fail to resolve an unsupported command")
	}
}

func TestLooksLikeLGCommand_ESP32LegacyIsNotLG(t *testing.T) {
	// Evidencia real (FASE LG-CMD-E2E-DIAG): comandos ESP32 legacy
	// (commandCode 101-106, sin commandKey/metadata) llegan al mismo topic
	// device.command.requested y no deben clasificarse como LG.
	for code := 101; code <= 106; code++ {
		event := commandsdomain.DeviceCommandEvent{
			CommandCode: code,
			CommandType: "OUTPUT_1",
		}
		if LooksLikeLGCommand(event) {
			t.Errorf("commandCode %d (ESP32 legacy) should not look like an LG command", code)
		}
	}
}

func TestLooksLikeLGCommand_MetadataCommandKeyPrefix(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{
		Metadata: &commandsdomain.DeviceCommandMetadata{CommandKey: "lg.bogus"},
	}
	if !LooksLikeLGCommand(event) {
		t.Error("a metadata.commandKey with the lg. prefix should look like an LG command, even if unresolvable")
	}
}

func TestLooksLikeLGCommand_TopLevelCommandKeyPrefix(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{CommandKey: "lg.unknown_future_key"}
	if !LooksLikeLGCommand(event) {
		t.Error("a top-level commandKey with the lg. prefix should look like an LG command")
	}
}

func TestLooksLikeLGCommand_CommandTypePrefix(t *testing.T) {
	event := commandsdomain.DeviceCommandEvent{CommandType: "LG_SOMETHING_NEW"}
	if !LooksLikeLGCommand(event) {
		t.Error("a commandType with the LG_ prefix should look like an LG command")
	}
}

func TestLooksLikeLGCommand_CommandCodeInLGRange(t *testing.T) {
	for _, code := range []int{200, 201, 206, 250, 299} {
		event := commandsdomain.DeviceCommandEvent{CommandCode: code}
		if !LooksLikeLGCommand(event) {
			t.Errorf("commandCode %d (in the 200-299 LG range) should look like an LG command", code)
		}
	}
}

func TestLooksLikeLGCommand_CommandCodeOutsideRange(t *testing.T) {
	for _, code := range []int{0, 100, 106, 199, 300, 999} {
		event := commandsdomain.DeviceCommandEvent{CommandCode: code}
		if LooksLikeLGCommand(event) {
			t.Errorf("commandCode %d (outside the LG range, no other LG signal) should not look like an LG command", code)
		}
	}
}
