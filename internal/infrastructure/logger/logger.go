package logger

import "go.uber.org/zap"

func NewLogger(level string) *zap.Logger {
config := zap.NewProductionConfig()
logger, _ := config.Build()
return logger
}
