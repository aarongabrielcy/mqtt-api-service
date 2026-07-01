package lg

import "context"

type EventService struct {
	client *LGAPIClient
}

func NewEventService(client *LGAPIClient) *EventService {
	return &EventService{
		client: client,
	}
}

func (s *EventService) List(ctx context.Context) ([]Event, error) {
	var resp APIResponse[[]Event]

	err := s.client.doRequest(
		ctx,
		"GET",
		"/event",
		nil,
		nil,
		&resp,
	)

	if err != nil {
		return nil, err
	}

	return resp.Response, nil
}

func (s *EventService) Subscribe(ctx context.Context, deviceID string, timer int) error {
	req := EventSubscribeRequest{
		Expire: EventExpire{
			Unit:  "HOUR",
			Timer: timer,
		},
	}

	var resp APIResponse[any]

	err := s.client.doRequest(
		ctx,
		"POST",
		"/event/"+deviceID+"/subscribe",
		&req,
		nil,
		&resp,
	)

	if err != nil {
		return err
	}

	return nil
}

func (s *EventService) Unsubscribe(ctx context.Context, deviceID string) error {
	var resp APIResponse[any]

	err := s.client.doRequest(
		ctx,
		"DELETE",
		"/event/"+deviceID+"/unsubscribe",
		nil,
		nil,
		&resp,
	)

	if err != nil {
		return err
	}

	return nil
}
