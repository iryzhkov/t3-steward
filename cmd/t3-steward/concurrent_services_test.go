package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRunConcurrentServicesCancelsPeerAndReportsFailure(t *testing.T) {
	failure := errors.New("coordinator failed")
	peerStopped := make(chan struct{})
	err := runConcurrentServices(context.Background(),
		func(context.Context) error { return failure },
		func(ctx context.Context) error {
			<-ctx.Done()
			close(peerStopped)
			return nil
		},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}
	select {
	case <-peerStopped:
	default:
		t.Fatal("peer service was not stopped")
	}
}

func TestRunConcurrentServicesRejectsUnexpectedCleanExit(t *testing.T) {
	err := runConcurrentServices(context.Background(),
		func(context.Context) error { return nil },
		func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "coordinator stopped unexpectedly") {
		t.Fatalf("error = %v", err)
	}
}
