package lg

import (
	"context"
)

type PushService struct {
	client *LGAPIClient
}

func NewPushService(client *LGAPIClient) *PushService {
	return &PushService{
		client: client,
	}
}

func (s *PushService) List(ctx context.Context) ([]PushDevice, error) {
	var resp APIResponse[[]PushDevice]

	err := s.client.doRequest(
		ctx,
		"GET",
		"/push",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return nil, err
	}

	return resp.Response, nil
}

func (s *PushService) Subscribe(ctx context.Context, deviceID string) error {
	var resp APIResponse[any]

	err := s.client.doRequest(
		ctx,
		"POST",
		"/push/"+deviceID+"/subscribe",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *PushService) Unsubscribe(ctx context.Context, deviceID string) error {
	var resp APIResponse[any]

	err := s.client.doRequest(
		ctx,
		"DELETE",
		"/push/"+deviceID+"/unsubscribe",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return err
	}

	return nil
}
