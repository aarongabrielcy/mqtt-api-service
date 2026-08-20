package lg_service

import (
	"context"
	"encoding/json"
	"fmt"
	repository "mqtt-api-service/internal/adapters/mongo"
	"mqtt-api-service/internal/adapters/parser"
	"mqtt-api-service/internal/application/commands"
	"mqtt-api-service/internal/application/normalizers"

	"go.uber.org/zap"
)

func (s *LGService) HandlePushMessage(ctx context.Context, topic string, rawPayload []byte) error {
	msg, ok, err := s.parsePushMessage(topic, rawPayload)
	if err != nil || !ok {
		return err
	}

	previousState, hasPreviousState := s.getPreviousState(ctx, msg.DeviceID)

	mergedState, newState, err := s.mergeDeviceState(ctx, msg.DeviceID, msg.Report)
	if err != nil {
		return err
	}

	s.logParsedStateIfEnabled(msg.DeviceID, mergedState, &newState)

	if s.confirmationManager != nil {
		s.confirmationManager.TryConfirm(ctx, msg.DeviceID, commands.CurrentState{
			Power:             newState.Operation.AirConOperationMode == "POWER_ON",
			Mode:              newState.AirConJobMode.CurrentJobMode,
			TemperatureTarget: newState.Temperature.TargetTemperature,
			Airflow:           newState.AirFlow.WindStrength,
			Oscillation:       newState.WindDirection.RotateUpDown,
			PowerSave:         newState.PowerSave.PowerSaveEnabled,
		})
	}

	eventCode, shouldEmit, err := classifyPushEvent(msg.Report, previousState, newState, hasPreviousState)
	if err != nil {
		s.log.Error("failed to classify push event",
			zap.String("deviceID", msg.DeviceID),
			zap.Error(err),
		)
		return err
	}

	if !shouldEmit {
		s.log.Debug("push message does not represent a trackable event, state updated in redis only",
			zap.String("deviceID", msg.DeviceID),
		)
		return nil
	}

	return s.emitPushEvent(ctx, msg.DeviceID, msg.DeviceType, topic, mergedState, &newState, eventCode)
}

func (s *LGService) parsePushMessage(topic string, rawPayload []byte) (msg *parser.LGPushMessage, ok bool, err error) {
	msg, err = s.pushParser.Parse(topic, rawPayload)
	if err != nil {
		s.log.Error("failed to parse push message",
			zap.String("topic", topic),
			zap.Error(err),
		)
		return nil, false, err
	}

	if len(msg.Report) == 0 {
		s.log.Debug("push message without report, skipping",
			zap.String("deviceID", msg.DeviceID),
			zap.String("pushType", msg.PushType),
		)
		return nil, false, nil
	}

	return msg, true, nil
}

func (s *LGService) getPreviousState(ctx context.Context, deviceID string) (state parser.AirConditionerState, hasPreviousState bool) {
	prev, err := s.deviceStateStore.GetState(ctx, deviceID)
	if err != nil {
		return state, false
	}

	if err := json.Unmarshal(prev, &state); err != nil {
		s.log.Warn("failed to unmarshal previous state, treating as unknown",
			zap.String("deviceID", deviceID),
			zap.Error(err),
		)
		return state, false
	}

	return state, true
}

func (s *LGService) mergeDeviceState(ctx context.Context, deviceID string, report json.RawMessage) (mergedRaw []byte, state parser.AirConditionerState, err error) {
	mergedRaw, err = s.deviceStateStore.MergePartial(ctx, deviceID, report)
	if err != nil {
		s.log.Error("failed to merge device state in redis", zap.String("deviceID", deviceID), zap.Error(err))
		return nil, state, err
	}

	if err := json.Unmarshal(mergedRaw, &state); err != nil {
		s.log.Error("failed to unmarshal merged state", zap.String("deviceID", deviceID), zap.Error(err))
		return nil, state, err
	}

	return mergedRaw, state, nil
}

func (s *LGService) emitPushEvent(
	ctx context.Context,
	deviceID string,
	deviceType string,
	topic string,
	mergedState []byte,
	state *parser.AirConditionerState,
	eventCode normalizers.EventCode,
) error {

	if err := s.deviceStateStore.SetSnapshot(
		ctx,
		deviceID,
		mergedState,
	); err != nil {
		s.log.Error(
			"failed to update device state in redis",
			zap.String("deviceID", deviceID),
			zap.Error(err),
		)
	}

	var p map[string]any
	json.Unmarshal(mergedState, &p)

	if err := s.repository.Save(
		ctx,
		repository.RawMessage{
			IMEI:        deviceID,
			Brand:       "LG",
			MessageType: "push",
			Topic:       topic,
			Payload:     p,
			PayloadRaw:  string(mergedState),
		},
	); err != nil {
		s.log.Error(
			"failed to save push message",
			zap.String("deviceID", deviceID),
			zap.Error(err),
		)
	}

	return s.publishTracking(
		ctx,
		deviceID,
		deviceType,
		eventCode,
		state,
	)
}

func classifyPushEvent(
	report json.RawMessage,
	previousState parser.AirConditionerState,
	newState parser.AirConditionerState,
	hasPreviousState bool,
) (eventCode normalizers.EventCode, shouldEmit bool, err error) {

	var reportMap map[string]json.RawMessage

	if err := json.Unmarshal(report, &reportMap); err != nil {
		return 0, false, fmt.Errorf(
			"failed to inspect report fields: %w",
			err,
		)
	}

	if operationRaw, ok := reportMap["operation"]; ok && len(reportMap) == 1 {

		var operation struct {
			AirConOperationMode string `json:"airConOperationMode"`
		}

		if err := json.Unmarshal(operationRaw, &operation); err != nil {
			return 0, false, fmt.Errorf(
				"failed to parse operation field: %w",
				err,
			)
		}

		switch operation.AirConOperationMode {
		case "POWER_ON":
			return normalizers.EventCodePowerOn, true, nil

		case "POWER_OFF":
			return normalizers.EventCodePowerOff, true, nil

		default:
			return normalizers.EventCodeTracking, true, nil
		}
	}

	if _, ok := reportMap["airConJobMode"]; ok &&
		hasPreviousState &&
		previousState.AirConJobMode.CurrentJobMode !=
			newState.AirConJobMode.CurrentJobMode {

		return normalizers.EventCodeOperationModeChange, true, nil
	}

	if _, ok := reportMap["airFlow"]; ok &&
		hasPreviousState &&
		(previousState.AirFlow.WindStrength !=
			newState.AirFlow.WindStrength ||
			previousState.AirFlow.WindStrengthDetail !=
				newState.AirFlow.WindStrengthDetail) {

		return normalizers.EventCodeAirFlowChange, true, nil
	}

	if _, ok := reportMap["windDirection"]; ok &&
		hasPreviousState &&
		previousState.WindDirection.RotateUpDown !=
			newState.WindDirection.RotateUpDown {

		return normalizers.EventCodeOscillationChange, true, nil
	}

	if _, ok := reportMap["powerSave"]; ok &&
		hasPreviousState &&
		previousState.PowerSave.PowerSaveEnabled !=
			newState.PowerSave.PowerSaveEnabled {

		return normalizers.EventCodePowerSaveChange, true, nil
	}

	allowedTemperatureKeys := map[string]bool{
		"temperature":        true,
		"temperatureInUnits": true,
	}

	isTemperatureOnlyReport := len(reportMap) > 0

	for key := range reportMap {
		if !allowedTemperatureKeys[key] {
			isTemperatureOnlyReport = false
			break
		}
	}

	if isTemperatureOnlyReport &&
		hasPreviousState &&
		previousState.Temperature.TargetTemperature !=
			newState.Temperature.TargetTemperature {

		return normalizers.EventCodeTemperatureChange, true, nil
	}

	return normalizers.EventCodeTracking, false, nil
}
