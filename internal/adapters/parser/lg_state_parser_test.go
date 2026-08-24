package parser

import (
	"encoding/json"
	"testing"
)

func TestInspectOscillationField_PresentTrue(t *testing.T) {
	raw := json.RawMessage(`{"windDirection":{"rotateUpDown":true}}`)

	diag := InspectOscillationField(raw)

	if !diag.Present {
		t.Error("Present = false, want true when windDirection.rotateUpDown exists")
	}
	if diag.Raw != true {
		t.Errorf("Raw = %v, want true", diag.Raw)
	}
	if diag.Source != "windDirection.rotateUpDown" {
		t.Errorf("Source = %q, want windDirection.rotateUpDown", diag.Source)
	}
}

func TestInspectOscillationField_PresentFalse(t *testing.T) {
	raw := json.RawMessage(`{"windDirection":{"rotateUpDown":false}}`)

	diag := InspectOscillationField(raw)

	if !diag.Present {
		t.Error("Present = false, want true when windDirection.rotateUpDown exists (even if its value is false)")
	}
	if diag.Raw != false {
		t.Errorf("Raw = %v, want false", diag.Raw)
	}
}

func TestInspectOscillationField_AbsentWindDirectionObject(t *testing.T) {
	raw := json.RawMessage(`{"operation":{"airConOperationMode":"POWER_ON"}}`)

	diag := InspectOscillationField(raw)

	if diag.Present {
		t.Error("Present = true, want false when windDirection is entirely absent from the raw JSON")
	}
	if diag.Raw != nil {
		t.Errorf("Raw = %v, want nil", diag.Raw)
	}
}

func TestInspectOscillationField_WindDirectionPresentButRotateUpDownMissing(t *testing.T) {
	raw := json.RawMessage(`{"windDirection":{"rotateLeftRight":true}}`)

	diag := InspectOscillationField(raw)

	if diag.Present {
		t.Error("Present = true, want false when windDirection exists but rotateUpDown key does not")
	}
}

func TestInspectOscillationField_EmptyRaw(t *testing.T) {
	diag := InspectOscillationField(nil)
	if diag.Present {
		t.Error("Present = true, want false for nil raw")
	}

	diag = InspectOscillationField(json.RawMessage{})
	if diag.Present {
		t.Error("Present = true, want false for empty raw")
	}
}

func TestInspectOscillationField_InvalidJSON_DoesNotPanic(t *testing.T) {
	diag := InspectOscillationField(json.RawMessage(`not json`))
	if diag.Present {
		t.Error("Present = true, want false for invalid JSON")
	}
}

func TestInspectOscillationField_UnexpectedType_DoesNotPanic(t *testing.T) {
	// windDirection no es un objeto — no debe entrar en pánico al hacer el
	// type assertion a map[string]any.
	raw := json.RawMessage(`{"windDirection":"unexpected-string-shape"}`)

	diag := InspectOscillationField(raw)
	if diag.Present {
		t.Error("Present = true, want false when windDirection is not a JSON object")
	}
}
