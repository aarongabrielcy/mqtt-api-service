package client

import (
	"context"
)

func (c *Client) PublishState(
	ctx context.Context,
	deviceID string,
	state any,
) error {

	// Aquí eventualmente irá:
	//
	// c.deviceService.PublishState(...)

	return nil
}

func (c *Client) SendCommand(
	ctx context.Context,
	deviceID string,
	command any,
) error {

	// Aquí eventualmente irá:
	//
	// c.deviceService.SendCommand(...)

	return nil
}
