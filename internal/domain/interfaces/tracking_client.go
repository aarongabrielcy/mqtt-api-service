package interfaces

import (
	"context"
)

type TrackingClient interface {
	PublishEvent(
		ctx context.Context,
		payload []byte,
	) error
}
