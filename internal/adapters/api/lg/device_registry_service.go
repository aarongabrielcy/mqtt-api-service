package lg

import "context"

type DeviceRegistryService struct {
	client *LGAPIClient
}

func NewDeviceRegistryService(client *LGAPIClient) *DeviceRegistryService {
	return &DeviceRegistryService{
		client: client,
	}
}

func (s *DeviceRegistryService) List(ctx context.Context) ([]string, error) {
	var resp APIResponse[[]string]

	err := s.client.doRequest(
		ctx,
		"GET",
		"/push/devices",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return nil, err
	}

	return resp.Response, nil
}

func (s *DeviceRegistryService) Subscribe(ctx context.Context) error {
	var resp APIResponse[any]

	err := s.client.doRequest(
		ctx,
		"POST",
		"/push/devices",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *DeviceRegistryService) Unsubscribe(ctx context.Context) error {
	var resp APIResponse[any]

	err := s.client.doRequest(
		ctx,
		"DELETE",
		"/push/devices",
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return err
	}

	return nil
}
