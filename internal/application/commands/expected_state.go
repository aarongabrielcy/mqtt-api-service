package commands

import "strings"

// temperatureConfirmTolerance es la tolerancia usada al comparar
// climate.temperature.target contra el valor esperado: el estado LG puede
// reportar el setpoint con decimales que no coincidan exactamente con lo
// enviado en el comando.
const temperatureConfirmTolerance = 0.1

// ExpectedState describe qué campo del estado LG debe cambiar, y a qué
// valor, para considerar un comando confirmado. Se guarda serializado junto
// a PendingConfirmation.
type ExpectedState struct {
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// CurrentState es el subconjunto del estado LG relevante para confirmar
// comandos por estado, tomado directamente del polling/push más reciente.
type CurrentState struct {
	Power             bool
	Mode              string
	TemperatureTarget float64
	Airflow           string
	Oscillation       bool
	PowerSave         bool
}

// buildExpectedState deriva el estado esperado tras ejecutar un comando LG,
// a partir del payload ya validado.
func buildExpectedState(commandKey string, payload Payload) ExpectedState {
	switch commandKey {
	case CommandKeyPower:
		return ExpectedState{Path: "state.power", Value: payload.Power}
	case CommandKeyTemperature:
		return ExpectedState{Path: "climate.temperature.target", Value: payload.Temperature}
	case CommandKeyMode:
		return ExpectedState{Path: "state.mode", Value: payload.Mode}
	case CommandKeyAirflow:
		return ExpectedState{Path: "state.airflow", Value: payload.Strength}
	case CommandKeyOscillation:
		return ExpectedState{Path: "state.oscillation", Value: payload.Enabled}
	case CommandKeyPowerSave:
		return ExpectedState{Path: "state.powersave", Value: payload.Enabled}
	default:
		return ExpectedState{}
	}
}

// matchesExpected compara el estado LG más reciente contra lo esperado por
// una confirmación pendiente. Devuelve false tanto si no coincide como si
// expected.Path es desconocido o tiene un tipo inesperado (nunca asume un
// match que no pudo verificar).
func matchesExpected(expected ExpectedState, current CurrentState) bool {
	switch expected.Path {
	case "state.power":
		want, ok := expected.Value.(bool)
		return ok && want == current.Power
	case "state.mode":
		want, ok := expected.Value.(string)
		return ok && strings.EqualFold(want, current.Mode)
	case "climate.temperature.target":
		want, ok := toFloat(expected.Value)
		return ok && absFloat(want-current.TemperatureTarget) <= temperatureConfirmTolerance
	case "state.airflow":
		want, ok := expected.Value.(string)
		return ok && strings.EqualFold(want, current.Airflow)
	case "state.oscillation":
		want, ok := expected.Value.(bool)
		return ok && want == current.Oscillation
	case "state.powersave":
		want, ok := expected.Value.(bool)
		return ok && want == current.PowerSave
	default:
		return false
	}
}

// extractActualByPath devuelve el valor actual de CurrentState en
// expected.Path, para diagnóstico (LG_DEBUG_STATE_LOGS, FASE LG-CMD-2E) —
// espeja el switch de matchesExpected pero devuelve el valor crudo en vez de
// un bool de match, así un log puede mostrar expected=true junto a
// actual=false en vez de solo "no coincide".
func extractActualByPath(path string, current CurrentState) any {
	switch path {
	case "state.power":
		return current.Power
	case "state.mode":
		return current.Mode
	case "climate.temperature.target":
		return current.TemperatureTarget
	case "state.airflow":
		return current.Airflow
	case "state.oscillation":
		return current.Oscillation
	case "state.powersave":
		return current.PowerSave
	default:
		return nil
	}
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	default:
		return 0, false
	}
}

func absFloat(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
