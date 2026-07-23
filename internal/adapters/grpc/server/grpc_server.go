package server

import (
	"net"

	devicecontrolpb "mqtt-api-service/internal/adapters/grpc/proto/devicecontrol"

	"google.golang.org/grpc"
)

func Start(
	address string,
	deviceControlServer *DeviceControlServer,
) error {

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	devicecontrolpb.RegisterDeviceControlServiceServer(
		grpcServer,
		deviceControlServer,
	)

	return grpcServer.Serve(lis)
}
