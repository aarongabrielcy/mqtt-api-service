package lg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"mqtt-api-service/internal/infrastructure/config"
)

type LGAPIClient struct {
	httpClient *http.Client
	cfg        *config.Config
	log        *zap.Logger
}

type APIError struct {
	StatusCode int
	Body       string
}

func NewLGAPIClient(cfg *config.Config, log *zap.Logger) (*LGAPIClient, error) {

	timeout, err := time.ParseDuration(cfg.LGApi.Timeout)
	if err != nil {
		return nil, err
	}

	return &LGAPIClient{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		cfg: cfg,
		log: log,
	}, nil
}

func (c *LGAPIClient) buildURL(path string) string {
	return strings.TrimRight(c.cfg.LGApi.BaseURL, "/") +
		"/" +
		strings.TrimLeft(path, "/")
}

func (c *LGAPIClient) addHeaders(req *http.Request, extra map[string]string) {
	req.Header.Set("Authorization", "Bearer "+c.cfg.LGApi.AccessToken)
	req.Header.Set("x-message-id", uuid.NewString())
	req.Header.Set("x-country", c.cfg.LGApi.CountryCode)
	req.Header.Set("x-client-id", c.cfg.LGApi.ClientID)
	req.Header.Set("x-api-key", c.cfg.LGApi.APIKey)

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	for k, v := range extra {
		req.Header.Set(k, v)
	}
}

func (c *LGAPIClient) doRequest(
	ctx context.Context,
	method string,
	path string,
	body any,
	headers map[string]string,
	out any,
) error {

	url := c.buildURL(path)

	var bodyReader io.Reader

	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}

		bodyReader = bytes.NewBuffer(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req, headers)
	start := time.Now()

	c.log.Debug("Sending LG API request",
		zap.String("method", method),
		zap.String("url", url),
	)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		if len(respBody) > 500 {
			respBody = respBody[:500]
		}

		return &APIError{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
		}
	}

	if out == nil {
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	c.log.Debug("LG API response received",
		zap.String("path", path),
		zap.Duration("duration", time.Since(start)),
	)

	return nil
}

func (e *APIError) Error() string {
	return fmt.Sprintf("LG API error status=%d body=%s", e.StatusCode, e.Body)
}
