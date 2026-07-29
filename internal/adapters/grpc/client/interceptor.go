package client

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func UnaryLoggingInterceptor(log *zap.Logger) grpc.UnaryClientInterceptor {

	return func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		start := time.Now()

		err := invoker(
			ctx,
			method,
			req,
			reply,
			cc,
			opts...,
		)

		log.Info("grpc call", zap.String("method", method), zap.Duration("duration", time.Since(start)), zap.Error(err))

		return err
	}
}
