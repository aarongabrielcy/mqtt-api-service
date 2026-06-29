package mongo

import (
"context"
"mqtt-api-service/internal/domain"
"go.uber.org/zap"
)

type Repository interface {
SaveRawMessage(ctx context.Context, msg *domain.RawMessage) error
SaveDeadLetterMessage(ctx context.Context, msg *domain.DeadLetterMessage) error
Close(ctx context.Context) error
}

type mongoRepository struct {
log *zap.Logger
}

func NewRepository(config interface{}, log *zap.Logger) (Repository, error) {
return &mongoRepository{log: log}, nil
}

func (r *mongoRepository) SaveRawMessage(ctx context.Context, msg *domain.RawMessage) error {
return nil
}

func (r *mongoRepository) SaveDeadLetterMessage(ctx context.Context, msg *domain.DeadLetterMessage) error {
return nil
}

func (r *mongoRepository) Close(ctx context.Context) error {
return nil
}
