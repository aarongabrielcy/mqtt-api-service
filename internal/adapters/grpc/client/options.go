package client

import "time"

type Config struct {
	Address           string
	ConnectionTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		ConnectionTimeout: 5 * time.Second,
	}
}
