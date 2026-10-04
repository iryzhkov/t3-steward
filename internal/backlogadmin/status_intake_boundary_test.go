package backlogadmin

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
)

func TestIntakeStatusTransportBoundaryFailuresDoNotRetry(t *testing.T) {
	var calls atomic.Int32
	service := &statusTransportService{query: func(context.Context, Query) (Response, error) {
		calls.Add(1)
		return Response{}, nil
	}}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid())+1, service)
	defer stopLocalTransport(t, cancel, done)
	if _, err := client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus}); ClassOf(err) != ClassAuthentication || calls.Load() != 0 {
		t.Fatalf("peer auth failure masked: %v calls=%d", err, calls.Load())
	}
	client.Path = "relative"
	if _, err := client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus}); ClassOf(err) != ClassClientConfiguration || calls.Load() != 0 {
		t.Fatalf("client configuration failure masked: %v calls=%d", err, calls.Load())
	}
}
