package parser

import (
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
)

type AirConditionerState struct {
	RunState struct {
		CurrentState string `json:"currentState"`
	} `json:"runState"`

	AirConJobMode struct {
		CurrentJobMode string `json:"currentJobMode"`
	} `json:"airConJobMode"`

	PowerSave struct {
		PowerSaveEnabled bool `json:"powerSaveEnabled"`
	} `json:"powerSave"`

	Temperature struct {
		CurrentTemperature float64 `json:"currentTemperature"`
		TargetTemperature  float64 `json:"targetTemperature"`
		Unit               string  `json:"unit"`
	} `json:"temperature"`

	TemperatureInUnits []struct {
		CurrentTemperature float64 `json:"currentTemperature"`
		TargetTemperature  float64 `json:"targetTemperature"`
		Unit               string  `json:"unit"`
	} `json:"temperatureInUnits"`

	AirFlow struct {
		WindStrength       string `json:"windStrength"`
		WindStrengthDetail string `json:"windStrengthDetail"`
	} `json:"airFlow"`

	WindDirection struct {
		RotateUpDown bool `json:"rotateUpDown"`
	} `json:"windDirection"`

	Operation struct {
		AirConOperationMode string `json:"airConOperationMode"`
	} `json:"operation"`

	Timer struct {
		RelativeStartTimer string `json:"relativeStartTimer"`
		RelativeStopTimer  string `json:"relativeStopTimer"`
	} `json:"timer"`

	SleepTimer struct {
		RelativeStopTimer string `json:"relativeStopTimer"`
	} `json:"sleepTimer"`
}

type LGStateParser struct {
	log *zap.Logger
}

func NewLGStateParser(log *zap.Logger) *LGStateParser {
	return &LGStateParser{log: log}
}

func (p *LGStateParser) ParseAirConditionerState(deviceID string, raw json.RawMessage) (*AirConditionerState, error) {
	var state AirConditionerState

	if err := json.Unmarshal(raw, &state); err != nil {
		p.log.Warn("error parseando estado de aire acondicionado",
			zap.String("deviceId", deviceID),
			zap.Error(err),
		)
		return nil, fmt.Errorf("invalid air conditioner state format: %w", err)
	}

	return &state, nil
}

// oscillationFieldSource documenta el path JSON que AirConditionerState usa
// para Oscillation — mismo valor que InspectOscillationField reporta como
// Source, para que un log de diagnóstico no tenga que adivinarlo.
const oscillationFieldSource = "windDirection.rotateUpDown"

// OscillationDiagnostic describe lo que realmente había en el JSON crudo de
// LG para el campo de oscilación (FASE LG-CMD-2E) — AirConditionerState.
// WindDirection.RotateUpDown es un bool no-pointer, así que un campo
// ausente en la respuesta de LG y un campo presente con valor false son
// indistinguibles después de json.Unmarshal (ambos dan RotateUpDown=false).
// Este diagnóstico inspecciona el mapa crudo directamente, antes de
// cualquier default de Go, para poder distinguirlos.
type OscillationDiagnostic struct {
	// Present indica si windDirection.rotateUpDown existía explícitamente
	// en el JSON crudo (no si su valor era true).
	Present bool
	// Source es el path JSON inspeccionado, para que un log no tenga que
	// adivinar qué campo se está diagnosticando.
	Source string
	// Raw es el valor tal cual venía en el JSON (normalmente bool, pero se
	// deja como any para no ocultar un tipo inesperado de LG, ej. un
	// string "true" en vez de un bool).
	Raw any
}

// InspectOscillationField inspecciona raw (el JSON tal cual lo devolvió LG,
// antes de parsear a AirConditionerState) para determinar si
// windDirection.rotateUpDown estaba realmente presente, y con qué valor
// crudo — independiente de cómo AirConditionerState lo haya parseado. No
// falla nunca: un raw vacío, inválido, o sin el path esperado simplemente
// reporta Present=false, Raw=nil.
func InspectOscillationField(raw json.RawMessage) OscillationDiagnostic {
	diag := OscillationDiagnostic{Source: oscillationFieldSource}

	if len(raw) == 0 {
		return diag
	}

	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return diag
	}

	windDirection, ok := generic["windDirection"].(map[string]any)
	if !ok {
		return diag
	}

	value, present := windDirection["rotateUpDown"]
	diag.Present = present
	diag.Raw = value

	return diag
}

type PushOperation struct {
	AirConOperationMode string `json:"airConOperationMode"`
}

type PushTemperature struct {
	CurrentTemperature float64 `json:"currentTemperature"`
	TargetTemperature  float64 `json:"targetTemperature"`
	Unit               string  `json:"unit"`
}

type PushTemperatureInUnit struct {
	CurrentTemperature float64 `json:"currentTemperature"`
	TargetTemperature  float64 `json:"targetTemperature"`
	Unit               string  `json:"unit"`
}

type PushReport struct {
	RunState *struct {
		CurrentState string `json:"currentState"`
	} `json:"runState,omitempty"`

	AirConJobMode *struct {
		CurrentJobMode string `json:"currentJobMode"`
	} `json:"airConJobMode,omitempty"`

	PowerSave *struct {
		PowerSaveEnabled bool `json:"powerSaveEnabled"`
	} `json:"powerSave,omitempty"`

	Temperature *PushTemperature `json:"temperature,omitempty"`

	TemperatureInUnits []PushTemperatureInUnit `json:"temperatureInUnits,omitempty"`

	AirFlow *struct {
		WindStrength       string `json:"windStrength"`
		WindStrengthDetail string `json:"windStrengthDetail"`
	} `json:"airFlow,omitempty"`

	WindDirection *struct {
		RotateUpDown bool `json:"rotateUpDown"`
	} `json:"windDirection,omitempty"`

	Operation *PushOperation `json:"operation,omitempty"`

	Timer *struct {
		RelativeStartTimer string `json:"relativeStartTimer"`
		RelativeStopTimer  string `json:"relativeStopTimer"`
	} `json:"timer,omitempty"`

	SleepTimer *struct {
		RelativeStopTimer string `json:"relativeStopTimer"`
	} `json:"sleepTimer,omitempty"`
}

type AirConditionerPush struct {
	PushType   string     `json:"pushType"`
	ServiceID  string     `json:"serviceId"`
	DeviceID   string     `json:"deviceId"`
	UserList   []string   `json:"userList"`
	DeviceType string     `json:"deviceType"`
	Report     PushReport `json:"report"`
}

func (p *LGStateParser) ParseAirConditionerPush(
	raw json.RawMessage,
) (*AirConditionerPush, error) {

	var push AirConditionerPush

	if err := json.Unmarshal(raw, &push); err != nil {
		p.log.Warn("error parseando push de aire acondicionado",
			zap.Error(err),
		)
		return nil, fmt.Errorf("invalid air conditioner push format: %w", err)
	}

	return &push, nil
}
