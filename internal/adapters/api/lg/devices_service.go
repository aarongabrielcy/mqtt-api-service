package lg

import (
	"context"
	"encoding/json"
)

type DeviceService struct {
	client *LGAPIClient
}

func NewDeviceService(client *LGAPIClient) *DeviceService {
	return &DeviceService{
		client: client,
	}
}

func (s *DeviceService) List(ctx context.Context) ([]Device, error) {
	var resp APIResponse[[]Device]

	err := s.client.doRequest(
		ctx,
		"GET",
		"/devices",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return nil, err
	}

	return resp.Response, nil
}

func (s *DeviceService) GetState(ctx context.Context, deviceID string) (json.RawMessage, error) {
	var resp APIResponse[json.RawMessage]

	rawBody, statusCode, err := s.client.doRequestCapture(
		ctx,
		"GET",
		"/devices/"+deviceID+"/state",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return nil, err
	}

	// FASE LG-CMD-2E: bajo LG_DEBUG_STATE_LOGS=true, loguea el JSON crudo
	// devuelto por LG API para este device — es la única forma de saber si
	// windDirection.rotateUpDown (Oscillation) realmente vino en la
	// respuesta o si el parser lo está defaulteando a false.
	s.client.logRawStateResponseIfEnabled(deviceID, statusCode, rawBody)

	return resp.Response, nil
}

func (s *DeviceService) ControlState(ctx context.Context, deviceID string, state json.RawMessage) error {
	var resp APIResponse[any]

	err := s.client.doRequest(
		ctx,
		"POST",
		"/devices/"+deviceID+"/control",
		state,
		nil,
		&resp,
	)
	if err != nil {
		return err
	}

	return nil
}
