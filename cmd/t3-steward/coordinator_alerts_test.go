package main

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestCoordinatorAlertsDisabledWithoutBind(t *testing.T) {
	t.Parallel()
	cfg := config.Default()
	cfg.StatePath = ":memory:"
	listener, err := startCoordinatorAlerts(context.Background(), cfg, nil)
	if err != nil || listener != nil {
		t.Fatalf("disabled listener: %v %v", listener, err)
	}
	cfg.BacklogV2.Coordinator.Alerts.Bind = "127.0.0.1:0"
	if _, err := startCoordinatorAlerts(context.Background(), cfg, nil); err == nil {
		t.Fatal("enabled listener needs state directory")
	}
}
