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
	"mqtt-api-service/internal/infrastructure/debuglog"
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

// doRequest es el wrapper delgado usado por la mayoría de llamadas, que no
// necesitan el body de respuesta crudo además de lo ya decodificado en out.
func (c *LGAPIClient) doRequest(
	ctx context.Context,
	method string,
	path string,
	body any,
	headers map[string]string,
	out any,
) error {
	_, _, err := c.doRequestCapture(ctx, method, path, body, headers, out)
	return err
}

// doRequestCapture es igual a doRequest, pero además devuelve el body crudo
// de una respuesta exitosa y su status code — usado por GetState (FASE
// LG-CMD-2E) para poder loguear el JSON raw de estado LG bajo
// LG_DEBUG_STATE_LOGS sin que cada llamador tenga que reimplementar el
// manejo HTTP. Nunca decodifica desde un stream: lee el body completo
// primero (estas respuestas son pequeñas — estado/lista de un solo
// dispositivo) para poder tanto decodificarlo como loguearlo.
func (c *LGAPIClient) doRequestCapture(
	ctx context.Context,
	method string,
	path string,
	body any,
	headers map[string]string,
	out any,
) (respBody []byte, statusCode int, err error) {

	url := c.buildURL(path)

	var bodyReader io.Reader

	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to marshal request body: %w", err)
		}

		bodyReader = bytes.NewBuffer(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	c.addHeaders(req, headers)
	start := time.Now()

	c.log.Debug("Sending LG API request",
		zap.String("method", method),
		zap.String("url", url),
	)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(resp.Body)
		if len(errBody) > 500 {
			errBody = errBody[:500]
		}

		code, message := parseLGErrorBody(errBody)

		return nil, resp.StatusCode, &APIError{
			StatusCode: resp.StatusCode,
			Body:       string(errBody),
			Code:       code,
			Message:    message,
		}
	}

	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read response body: %w", err)
	}

	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return respBody, resp.StatusCode, fmt.Errorf("failed to decode response: %w", err)
		}
	}

	c.log.Debug("LG API response received",
		zap.String("path", path),
		zap.Duration("duration", time.Since(start)),
	)

	return respBody, resp.StatusCode, nil
}

// logRawStateResponseIfEnabled loguea el JSON raw de una respuesta de estado
// LG (FASE LG-CMD-2E), solo si LG_DEBUG_STATE_LOGS=true. Nunca incluye
// headers/tokens — únicamente el body ya leído para decodificar, truncado a
// un tamaño razonable.
func (c *LGAPIClient) logRawStateResponseIfEnabled(deviceID string, statusCode int, body []byte) {
	if c.cfg == nil || !c.cfg.LGCommands.DebugStateLogs {
		return
	}

	truncatedBody, wasTruncated := debuglog.Truncate(body, debuglog.DefaultMaxBodyLogLength)

	c.log.Debug("LG raw state response",
		zap.String("deviceID", deviceID),
		zap.Int("status", statusCode),
		zap.Bool("bodyTruncated", wasTruncated),
		zap.Int("bodyLength", len(body)),
		zap.ByteString("body", truncatedBody),
	)
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
