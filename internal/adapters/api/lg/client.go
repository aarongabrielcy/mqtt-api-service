package lg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	Code       string
	Message    string
}

// lgNotConnectedHTTPStatus / lgNotConnectedErrorCode identifican la
// respuesta esperada de la LG API cuando un dispositivo está desconectado
// (status=416, error.code=1222, message="Not connected device"). Esta es
// una condición operativa normal, no un error crítico del servicio.
const (
	lgNotConnectedHTTPStatus = 416
	lgNotConnectedErrorCode  = "1222"

	// lgDeviceTimeoutHTTPStatus / lgDeviceTimeoutErrorCode identifican la
	// respuesta de la LG API cuando el comando se envió pero LG Cloud no
	// recibió confirmación del dispositivo a tiempo (status=400,
	// error.code=2211, message="Device Timeout"). A diferencia de "Not
	// connected device" (1222), esto NO significa que el dispositivo esté
	// offline ni que el comando haya fallado: el equipo puede ejecutar el
	// cambio segundos después. Ver FASE LG-CMD-2D: el dispatcher no marca
	// failure inmediato para este código, sino que registra la confirmación
	// pendiente igual que en el camino exitoso.
	lgDeviceTimeoutHTTPStatus = 400
	lgDeviceTimeoutErrorCode  = "2211"
)

// IsDeviceNotConnected identifica la respuesta esperada de la LG API cuando
// el dispositivo está desconectado (offline en la app LG ThinQ), para poder
// tratarla como una condición operativa normal en vez de un error crítico.
func (e *APIError) IsDeviceNotConnected() bool {
	return e.StatusCode == lgNotConnectedHTTPStatus && e.Code == lgNotConnectedErrorCode
}

// IsDeviceTimeout identifica el "Device Timeout" ambiguo de la LG API
// (status=400, error.code=2211): el comando pudo haberse aplicado igual, así
// que se debe esperar confirmación por estado en vez de fallar de inmediato.
func (e *APIError) IsDeviceTimeout() bool {
	return e.StatusCode == lgDeviceTimeoutHTTPStatus && e.Code == lgDeviceTimeoutErrorCode
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

		code, message := parseLGErrorBody(respBody)

		return &APIError{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
			Code:       code,
			Message:    message,
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

// parseLGErrorBody extrae code/message del body de error de la LG API,
// tolerando tanto {"error":{"code":...,"message":...}} como
// {"code":...,"message":...} a nivel raíz, y tanto código numérico como
// string (LG no documenta el shape exacto de forma consistente).
func parseLGErrorBody(body []byte) (code, message string) {
	var generic map[string]interface{}
	if err := json.Unmarshal(body, &generic); err != nil {
		return "", ""
	}

	if errObj, ok := generic["error"].(map[string]interface{}); ok {
		code = stringifyAny(errObj["code"])
		message = stringifyAny(errObj["message"])
		if code != "" || message != "" {
			return code, message
		}
	}

	return stringifyAny(generic["code"]), stringifyAny(generic["message"])
}

func stringifyAny(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return ""
	}
}
