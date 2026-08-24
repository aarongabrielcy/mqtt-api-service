package cache

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStatusKey_Format(t *testing.T) {
	got := statusKey("device-123")
	want := "lg:device:device-123:status"

	if got != want {
		t.Errorf("statusKey(%q) = %q, want %q", "device-123", got, want)
	}
}

func TestDeviceStatus_JSONFieldNames(t *testing.T) {
	status := DeviceStatus{
		Status:        "offline",
		LastErrorCode: "1222",
		UpdatedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, field := range []string{"status", "lastSeenAt", "lastErrorCode", "updatedAt"} {
		if _, ok := got[field]; !ok {
			t.Errorf("expected JSON field %q to be present, got %v", field, got)
		}
	}
}
