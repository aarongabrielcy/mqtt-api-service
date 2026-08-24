package repository

import (
	"strings"
	"testing"
)

func TestRedactMongoURI_StripsCredentials(t *testing.T) {
	got := redactMongoURI("mongodb://appuser:s3cr3t@mongo:27017/mqtt-api-service")

	if got == "mongodb://appuser:s3cr3t@mongo:27017/mqtt-api-service" {
		t.Fatal("redactMongoURI no debe devolver la URI con credenciales intactas")
	}
	if strings.Contains(got, "appuser") || strings.Contains(got, "s3cr3t") {
		t.Errorf("redactMongoURI no debe filtrar credenciales, got %q", got)
	}
	if !strings.Contains(got, "mongo:27017") {
		t.Errorf("redactMongoURI debe conservar host:puerto, got %q", got)
	}
}

func TestRedactMongoURI_NoCredentialsUnchanged(t *testing.T) {
	const uri = "mongodb://mongo:27017"

	got := redactMongoURI(uri)
	if got != uri {
		t.Errorf("redactMongoURI(%q) = %q, want unchanged (sin credenciales que quitar)", uri, got)
	}
}

func TestRedactMongoURI_UnparseableNeverLeaksInput(t *testing.T) {
	const malformed = "mongodb://appuser:s3cr3t@mongo:27017/db?ssl=true%"

	got := redactMongoURI(malformed)
	if got == malformed {
		t.Fatal("una URI no parseable no debe devolverse tal cual (podría contener credenciales)")
	}
	if strings.Contains(got, "s3cr3t") {
		t.Errorf("el placeholder de fallback no debe contener credenciales, got %q", got)
	}
}
