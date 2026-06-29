package api

import (
"context"
"go.uber.org/zap"
)

type LGAPIClient struct {
log *zap.Logger
}

func NewLGAPIClient(config interface{}, log *zap.Logger) *LGAPIClient {
return &LGAPIClient{log: log}
}

func (c *LGAPIClient) GetDeviceState(ctx context.Context, deviceID string) (interface{}, error) {
return nil, nil
}

func (c *LGAPIClient) SendCommand(ctx context.Context, deviceID string, action string, params map[string]interface{}) (string, error) {
return "", nil
}
