package lg

import "testing"

func TestAPIError_IsDeviceNotConnected(t *testing.T) {
	cases := []struct {
		name string
		err  *APIError
		want bool
	}{
		{"416 + 1222 is disconnected", &APIError{StatusCode: 416, Code: "1222"}, true},
		{"416 but different code is not disconnected", &APIError{StatusCode: 416, Code: "9999"}, false},
		{"1222 but different status is not disconnected", &APIError{StatusCode: 500, Code: "1222"}, false},
		{"unrelated error", &APIError{StatusCode: 500, Code: "0001"}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.err.IsDeviceNotConnected(); got != c.want {
				t.Errorf("IsDeviceNotConnected() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestParseLGErrorBody_NestedErrorObject(t *testing.T) {
	body := []byte(`{"error":{"code":1222,"message":"Not connected device"}}`)

	code, message := parseLGErrorBody(body)
	if code != "1222" {
		t.Errorf("code = %q, want 1222", code)
	}
	if message != "Not connected device" {
		t.Errorf("message = %q, want %q", message, "Not connected device")
	}
}

func TestParseLGErrorBody_FlatFields(t *testing.T) {
	body := []byte(`{"code":"1222","message":"Not connected device"}`)

	code, message := parseLGErrorBody(body)
	if code != "1222" {
		t.Errorf("code = %q, want 1222", code)
	}
	if message != "Not connected device" {
		t.Errorf("message = %q, want %q", message, "Not connected device")
	}
}

func TestParseLGErrorBody_InvalidJSON(t *testing.T) {
	code, message := parseLGErrorBody([]byte("not json"))
	if code != "" || message != "" {
		t.Errorf("expected empty code/message for invalid JSON, got code=%q message=%q", code, message)
	}
}

func TestParseLGErrorBody_EndToEnd_ClassifiesAsDisconnected(t *testing.T) {
	code, message := parseLGErrorBody([]byte(`{"error":{"code":1222,"message":"Not connected device"}}`))
	apiErr := &APIError{StatusCode: 416, Code: code, Message: message}

	if !apiErr.IsDeviceNotConnected() {
		t.Error("expected the parsed 416/1222 body to classify as IsDeviceNotConnected")
	}
}
