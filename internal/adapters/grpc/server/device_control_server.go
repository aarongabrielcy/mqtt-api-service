package server

import (
	"context"
	"log"
	devicecontrolpb "mqtt-api-service/internal/adapters/grpc/proto/devicecontrol"
	lg_service "mqtt-api-service/internal/application/use_case/lg"
)

type DeviceControlServer struct {
	devicecontrolpb.UnimplementedDeviceControlServiceServer

	lgService *lg_service.LGService
}

func NewDeviceControlServer(
	lgService *lg_service.LGService,
) *DeviceControlServer {
	return &DeviceControlServer{
		lgService: lgService,
	}
}

func (s *DeviceControlServer) SetPower(
	ctx context.Context,
	req *devicecontrolpb.SetPowerRequest,
) (*devicecontrolpb.CommandResponse, error) {

	log.Printf(
		">>> SetPower called: device=%s power=%v",
		req.DeviceId,
		req.Power,
	)

	err := s.lgService.SetDevicePower(
		ctx,
		req.DeviceId,
		req.Power,
	)

	if err != nil {
		return &devicecontrolpb.CommandResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &devicecontrolpb.CommandResponse{
		Success: true,
		Message: "power updated",
	}, nil
}

func (s *DeviceControlServer) SetTemperature(
	ctx context.Context,
	req *devicecontrolpb.SetTemperatureRequest,
) (*devicecontrolpb.CommandResponse, error) {
	log.Printf(
		">>> SetTemperature called: device=%s temp=%v",
		req.DeviceId,
		req.Temperature,
	)

	err := s.lgService.SetDeviceTemperature(
		ctx,
		req.DeviceId,
		req.Temperature,
	)

	if err != nil {
		return &devicecontrolpb.CommandResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &devicecontrolpb.CommandResponse{
		Success: true,
		Message: "temperature updated",
	}, nil
}
