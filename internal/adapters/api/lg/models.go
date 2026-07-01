package lg

type APIResponse[T any] struct {
	MessageID string `json:"messageId"`
	Response  T      `json:"response"`
	Timestamp string `json:"timestamp"`
}

type Device struct {
	DeviceID   string     `json:"deviceId"`
	DeviceInfo DeviceInfo `json:"deviceInfo"`
}

type DeviceInfo struct {
	Alias      string `json:"alias"`
	DeviceType string `json:"deviceType"`
	ModelName  string `json:"modelName"`
	Reportable bool   `json:"reportable"`
}

type PushDevice struct {
	DeviceID string `json:"deviceId"`
}

type Event struct {
	DeviceID string `json:"deviceId"`
	TTL      int64  `json:"ttl"`
}

type EventSubscribeRequest struct {
	Expire EventExpire `json:"expire"`
}

type EventExpire struct {
	Unit  string `json:"unit"`
	Timer int    `json:"timer"`
}

type AirConditionerState struct {
	RunState           RunState      `json:"runState"`
	AirConJobMode      AirConJobMode `json:"airConJobMode"`
	PowerSave          PowerSave     `json:"powerSave"`
	Temperature        Temperature   `json:"temperature"`
	TemperatureInUnits []Temperature `json:"temperatureInUnits"`
	AirFlow            AirFlow       `json:"airFlow"`
	WindDirection      WindDirection `json:"windDirection"`
	Operation          Operation     `json:"operation"`
	Timer              Timer         `json:"timer"`
	SleepTimer         SleepTimer    `json:"sleepTimer"`
}

type RunState struct {
	CurrentState string `json:"currentState"`
}

type AirConJobMode struct {
	CurrentJobMode string `json:"currentJobMode"`
}

type PowerSave struct {
	PowerSaveEnabled bool `json:"powerSaveEnabled"`
}

type Temperature struct {
	CurrentTemperature float64 `json:"currentTemperature"`
	TargetTemperature  float64 `json:"targetTemperature"`
	Unit               string  `json:"unit"`
}

type AirFlow struct {
	WindStrength       string `json:"windStrength"`
	WindStrengthDetail string `json:"windStrengthDetail"`
}

type WindDirection struct {
	RotateUpDown bool `json:"rotateUpDown"`
}

type Operation struct {
	AirConOperationMode string `json:"airConOperationMode"`
}

type Timer struct {
	RelativeStartTimer string `json:"relativeStartTimer"`
	RelativeStopTimer  string `json:"relativeStopTimer"`
}

type SleepTimer struct {
	RelativeStopTimer string `json:"relativeStopTimer"`
}
