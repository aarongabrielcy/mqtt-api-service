package lg

import (
	"context"
	"fmt"
)

type EnergyService struct {
	client *LGAPIClient
}

func NewEnergyService(client *LGAPIClient) *EnergyService {
	return &EnergyService{
		client: client,
	}
}

type EnergyUsageRequest struct {
	DeviceID  string
	Period    string
	StartDate string
	EndDate   string
}

type EnergyUsageResponse struct {
	DataList []EnergyUsage `json:"dataList"`
	Property []string      `json:"property"`
}

type EnergyUsage struct {
	UsedDate    string  `json:"usedDate"`
	EnergyUsage float64 `json:"energyUsage"`
}

func (s *EnergyService) GetEnergyUsage(
	ctx context.Context,
	req EnergyUsageRequest,
) (*EnergyUsageResponse, error) {
	if req.DeviceID == "" {
		return nil, fmt.Errorf("device ID is required")
	}

	if req.Period == "" {
		return nil, fmt.Errorf("period is required")
	}

	if req.StartDate == "" {
		return nil, fmt.Errorf("start date is required")
	}

	if req.EndDate == "" {
		return nil, fmt.Errorf("end date is required")
	}

	path := fmt.Sprintf(
		"/devices/energy/%s/usage?period=%s&startDate=%s&endDate=%s",
		req.DeviceID,
		req.Period,
		req.StartDate,
		req.EndDate,
	)

	var resp APIResponse[EnergyUsageResponse]

	err := s.client.doRequest(
		ctx,
		"GET",
		path,
		nil,
		nil,
		&resp,
	)
	if err != nil {
		return nil, err
	}

	return &resp.Response, nil
}
