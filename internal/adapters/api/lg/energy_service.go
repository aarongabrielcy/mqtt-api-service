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
) (resp *EnergyUsageResponse, rawBody []byte, err error) {
	if req.DeviceID == "" {
		return nil, nil, fmt.Errorf("device ID is required")
	}

	if req.Period == "" {
		return nil, nil, fmt.Errorf("period is required")
	}

	if req.StartDate == "" {
		return nil, nil, fmt.Errorf("start date is required")
	}

	if req.EndDate == "" {
		return nil, nil, fmt.Errorf("end date is required")
	}

	path := fmt.Sprintf(
		"/devices/energy/%s/usage?period=%s&startDate=%s&endDate=%s",
		req.DeviceID,
		req.Period,
		req.StartDate,
		req.EndDate,
	)

	var apiResp APIResponse[EnergyUsageResponse]

	rawBody, _, err = s.client.doRequestCapture(
		ctx,
		"GET",
		path,
		nil,
		nil,
		&apiResp,
	)
	if err != nil {
		return nil, nil, err
	}

	return &apiResp.Response, rawBody, nil
}
