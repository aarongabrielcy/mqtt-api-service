package commands

import "testing"

func TestBuildExpectedState(t *testing.T) {
	cases := []struct {
		commandKey string
		payload    Payload
		wantPath   string
	}{
		{CommandKeyPower, Payload{Power: true}, "state.power"},
		{CommandKeyTemperature, Payload{Temperature: 24}, "climate.temperature.target"},
		{CommandKeyMode, Payload{Mode: "COOL"}, "state.mode"},
		{CommandKeyAirflow, Payload{Strength: "HIGH"}, "state.airflow"},
		{CommandKeyOscillation, Payload{Enabled: true}, "state.oscillation"},
		{CommandKeyPowerSave, Payload{Enabled: true}, "state.powersave"},
	}

	for _, c := range cases {
		got := buildExpectedState(c.commandKey, c.payload)
		if got.Path != c.wantPath {
			t.Errorf("%s: path = %q, want %q", c.commandKey, got.Path, c.wantPath)
		}
	}
}

func TestMatchesExpected_Power(t *testing.T) {
	expected := ExpectedState{Path: "state.power", Value: true}

	if !matchesExpected(expected, CurrentState{Power: true}) {
		t.Error("expected match when current.Power == true")
	}
	if matchesExpected(expected, CurrentState{Power: false}) {
		t.Error("expected no match when current.Power == false")
	}
}

func TestMatchesExpected_Mode_CaseInsensitive(t *testing.T) {
	expected := ExpectedState{Path: "state.mode", Value: "COOL"}

	if !matchesExpected(expected, CurrentState{Mode: "cool"}) {
		t.Error("expected case-insensitive match for mode")
	}
	if matchesExpected(expected, CurrentState{Mode: "FAN"}) {
		t.Error("expected no match for a different mode")
	}
}

func TestMatchesExpected_TemperatureWithTolerance(t *testing.T) {
	expected := ExpectedState{Path: "climate.temperature.target", Value: 24.0}

	if !matchesExpected(expected, CurrentState{TemperatureTarget: 24.05}) {
		t.Error("expected match within 0.1 tolerance")
	}
	if matchesExpected(expected, CurrentState{TemperatureTarget: 24.5}) {
		t.Error("expected no match outside tolerance")
	}
}

func TestMatchesExpected_Airflow(t *testing.T) {
	expected := ExpectedState{Path: "state.airflow", Value: "HIGH"}

	if !matchesExpected(expected, CurrentState{Airflow: "high"}) {
		t.Error("expected case-insensitive match for airflow")
	}
	if matchesExpected(expected, CurrentState{Airflow: "LOW"}) {
		t.Error("expected no match for a different airflow strength")
	}
}

func TestMatchesExpected_OscillationAndPowerSave(t *testing.T) {
	if !matchesExpected(ExpectedState{Path: "state.oscillation", Value: true}, CurrentState{Oscillation: true}) {
		t.Error("expected oscillation match")
	}
	if !matchesExpected(ExpectedState{Path: "state.powersave", Value: false}, CurrentState{PowerSave: false}) {
		t.Error("expected powersave match")
	}
}

func TestMatchesExpected_UnknownPathNeverMatches(t *testing.T) {
	if matchesExpected(ExpectedState{Path: "state.unknown", Value: true}, CurrentState{}) {
		t.Error("unknown path should never match")
	}
}

func TestMatchesExpected_WrongValueTypeNeverMatches(t *testing.T) {
	// expected.Value tras un round-trip JSON de Redis siempre llega con el
	// tipo correcto, pero si por alguna razón no coincide, no debe pánico
	// ni asumir match.
	if matchesExpected(ExpectedState{Path: "state.power", Value: "true"}, CurrentState{Power: true}) {
		t.Error("string value for a boolean path should not match")
	}
}
