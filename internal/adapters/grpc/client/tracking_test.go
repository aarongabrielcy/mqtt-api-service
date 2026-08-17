package client

import (
	"context"
	"errors"
	"testing"
	"time"

	trackingpb "mqtt-api-service/internal/adapters/grpc/proto/tracking"
	"mqtt-api-service/internal/domain/interfaces"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestIsTransientGRPCError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"unavailable", status.Error(codes.Unavailable, "name resolver error: produced zero addresses"), true},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), true},
		{"invalid argument", status.Error(codes.InvalidArgument, "bad payload"), false},
		{"not found", status.Error(codes.NotFound, "not found"), false},
		{"plain non-grpc error", errors.New("boom"), false},
		{"nil error", nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isTransientGRPCError(c.err); got != c.want {
				t.Errorf("isTransientGRPCError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// fakeTrackingClient implementa trackingpb.TrackingServiceClient devolviendo
// una secuencia fija de resultados, uno por llamada (el último se repite si
// hay más llamadas que resultados).
type fakeTrackingClient struct {
	calls   int
	results []error
}

func (f *fakeTrackingClient) IngestRaw(_ context.Context, _ *trackingpb.RawMessage, _ ...grpc.CallOption) (*trackingpb.Ack, error) {
	idx := f.calls
	if idx >= len(f.results) {
		idx = len(f.results) - 1
	}
	f.calls++

	if err := f.results[idx]; err != nil {
		return nil, err
	}
	return &trackingpb.Ack{Ok: true}, nil
}

// newTestClient arma un Client con un tracking fake y un *grpc.ClientConn
// real pero no conectado (grpc.NewClient es lazy — no dispara I/O), solo
// para que conn.GetState() funcione en los logs de IngestRaw.
func newTestClient(t *testing.T, fake *fakeTrackingClient) *Client {
	t.Helper()

	conn, err := grpc.NewClient("localhost:0", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to create test conn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &Client{
		conn:           conn,
		tracking:       fake,
		log:            zap.NewNop(),
		requestTimeout: 200 * time.Millisecond,
		maxAttempts:    3,
		initialBackoff: 1 * time.Millisecond,
		maxBackoff:     2 * time.Millisecond,
	}
}

func testInput() interfaces.IngestRawInput {
	return interfaces.IngestRawInput{
		Topic:      "devices/device-abc/telemetry",
		Payload:    []byte(`{"vendor":"lg"}`),
		ReceivedAt: time.Now(),
	}
}

func TestIngestRaw_RetriesOnTransientError(t *testing.T) {
	fake := &fakeTrackingClient{results: []error{
		status.Error(codes.Unavailable, "name resolver error: produced zero addresses"),
		status.Error(codes.Unavailable, "name resolver error: produced zero addresses"),
		nil,
	}}
	c := newTestClient(t, fake)

	if err := c.IngestRaw(context.Background(), testInput()); err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if fake.calls != 3 {
		t.Errorf("calls = %d, want 3", fake.calls)
	}
}

func TestIngestRaw_NoRetryOnNonTransientError(t *testing.T) {
	fake := &fakeTrackingClient{results: []error{
		status.Error(codes.InvalidArgument, "bad payload"),
	}}
	c := newTestClient(t, fake)

	if err := c.IngestRaw(context.Background(), testInput()); err == nil {
		t.Fatal("expected error, got nil")
	}
	if fake.calls != 1 {
		t.Errorf("calls = %d, want 1 (no debe reintentar errores no transitorios)", fake.calls)
	}
}

func TestIngestRaw_FailsAfterMaxAttempts(t *testing.T) {
	fake := &fakeTrackingClient{results: []error{
		status.Error(codes.Unavailable, "name resolver error: produced zero addresses"),
	}}
	c := newTestClient(t, fake)

	if err := c.IngestRaw(context.Background(), testInput()); err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if fake.calls != c.maxAttempts {
		t.Errorf("calls = %d, want %d (maxAttempts)", fake.calls, c.maxAttempts)
	}
}
