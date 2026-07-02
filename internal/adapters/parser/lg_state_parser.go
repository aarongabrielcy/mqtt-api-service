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
