package interfaces

import (
	"context"
)

type TrackingClient interface {
	IngestRaw(
		ctx context.Context,
		payload []byte,
	) error
}
