package server

import (
	"context"
	"log"

	devicecontrolpb "mqtt-api-service/internal/adapters/grpc/proto/devicecontrol"
	lg_service "mqtt-api-service/internal/application/use_case/lg"
)

// DeviceControlServer expone comandos LG (SetPower, SetTemperature, etc.)
// como un gRPC server invocado directamente. Esto NO es el flujo final de
// comandos de la plataforma, solo un mecanismo de desarrollo/pruebas.
//
// TODO: la integración final de comandos LG debe consumir Kafka
// device.command.requested y publicar device.command.sent /
// device.command.publish_failed, equivalente al patrón implementado en
// mqtt-adapter-service (internal/adapters/kafka + internal/application
// command_dispatcher.go de ese repo). No se implementa el Kafka command
// consumer en esta fase (FASE LG-1 / LG-1A). No conectar este servidor a
// producción.
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

func (s *DeviceControlServer) SetAirFlow(
	ctx context.Context,
	req *devicecontrolpb.SetAirFlowRequest,
) (*devicecontrolpb.CommandResponse, error) {

	log.Printf(
		">>> SetAirFlow called: device=%s strength=%s",
		req.DeviceId,
		req.Strength,
	)

	err := s.lgService.SetAirFlow(
		ctx,
		req.DeviceId,
		req.Strength,
	)

	if err != nil {
		return &devicecontrolpb.CommandResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &devicecontrolpb.CommandResponse{
		Success: true,
		Message: "air flow updated",
	}, nil
}

func (s *DeviceControlServer) SetOperationMode(
	ctx context.Context,
	req *devicecontrolpb.SetOperationModeRequest,
) (*devicecontrolpb.CommandResponse, error) {

	log.Printf(
		">>> SetOperationMode called: device=%s mode=%s",
		req.DeviceId,
		req.Mode,
	)

	err := s.lgService.SetOperationMode(
		ctx,
		req.DeviceId,
		req.Mode,
	)

	if err != nil {
		return &devicecontrolpb.CommandResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &devicecontrolpb.CommandResponse{
		Success: true,
		Message: "operation mode updated",
	}, nil
}

func (s *DeviceControlServer) SetOscillation(
	ctx context.Context,
	req *devicecontrolpb.SetOscillationRequest,
) (*devicecontrolpb.CommandResponse, error) {

	log.Printf(
		">>> SetOscillation called: device=%s enabled=%v",
		req.DeviceId,
		req.Enabled,
	)

	err := s.lgService.SetOscillation(
		ctx,
		req.DeviceId,
		req.Enabled,
	)

	if err != nil {
		return &devicecontrolpb.CommandResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &devicecontrolpb.CommandResponse{
		Success: true,
		Message: "oscillation updated",
	}, nil
}

func (s *DeviceControlServer) SetPowerSave(
	ctx context.Context,
	req *devicecontrolpb.SetPowerSaveRequest,
) (*devicecontrolpb.CommandResponse, error) {

	log.Printf(
		">>> SetPowerSave called: device=%s enabled=%v",
		req.DeviceId,
		req.Enabled,
	)

	err := s.lgService.SetPowerSave(
		ctx,
		req.DeviceId,
		req.Enabled,
	)

	if err != nil {
		return &devicecontrolpb.CommandResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &devicecontrolpb.CommandResponse{
		Success: true,
		Message: "power save updated",
	}, nil
}
