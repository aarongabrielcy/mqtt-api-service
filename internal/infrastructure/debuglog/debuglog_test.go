package debuglog

import "testing"

func TestTruncate_ShorterThanMax_ReturnsUnchanged(t *testing.T) {
	data := []byte(`{"a":1}`)
	out, truncated := Truncate(data, 8192)

	if truncated {
		t.Error("truncated = true, want false for data shorter than max")
	}
	if string(out) != string(data) {
		t.Errorf("out = %q, want %q", out, data)
	}
}

func TestTruncate_LongerThanMax_TruncatesAndFlags(t *testing.T) {
	data := make([]byte, 100)
	for i := range data {
		data[i] = 'a'
	}

	out, truncated := Truncate(data, 10)

	if !truncated {
		t.Error("truncated = false, want true for data longer than max")
	}
	if len(out) != 10 {
		t.Errorf("len(out) = %d, want 10", len(out))
	}
}

func TestTruncate_EqualToMax_NotTruncated(t *testing.T) {
	data := []byte("0123456789")
	out, truncated := Truncate(data, 10)

	if truncated {
		t.Error("truncated = true, want false when data length equals max")
	}
	if len(out) != 10 {
		t.Errorf("len(out) = %d, want 10", len(out))
	}
}

func TestTruncate_ZeroOrNegativeMax_NeverPanicsAndReturnsUnchanged(t *testing.T) {
	data := []byte(`{"json":"body"}`)

	for _, max := range []int{0, -1, -100} {
		out, truncated := Truncate(data, max)
		if truncated {
			t.Errorf("max=%d: truncated = true, want false (a non-positive max disables truncation)", max)
		}
		if string(out) != string(data) {
			t.Errorf("max=%d: out = %q, want unchanged %q", max, out, data)
		}
	}
}

func TestTruncate_NilOrEmptyData_NeverPanics(t *testing.T) {
	if out, truncated := Truncate(nil, 8192); truncated || out != nil {
		t.Errorf("nil data: out=%v truncated=%v, want nil/false", out, truncated)
	}
	if out, truncated := Truncate([]byte{}, 8192); truncated || len(out) != 0 {
		t.Errorf("empty data: out=%v truncated=%v, want empty/false", out, truncated)
	}
}

func TestTruncate_StillValidLoggableString_DoesNotBreakOnTruncatedJSON(t *testing.T) {
	// El resultado truncado no tiene por qué ser JSON válido (es
	// intencionalmente un corte crudo), pero debe seguir siendo una cadena
	// de bytes segura de loguear (no debe entrar en pánico ni devolver
	// bytes fuera de rango).
	data := []byte(`{"state":{"oscillation":true,"airflow":"HIGH","powersave":false}}`)
	out, truncated := Truncate(data, 20)

	if !truncated {
		t.Fatal("expected truncation for a body longer than max")
	}
	if len(out) != 20 {
		t.Fatalf("len(out) = %d, want 20", len(out))
	}
	if string(out) != string(data[:20]) {
		t.Errorf("out = %q, want first 20 bytes of original data", out)
	}
}
