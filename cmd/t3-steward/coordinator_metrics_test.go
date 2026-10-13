package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/config"
	"testing"
)

func TestCoordinatorMetricsDefaultDoesNotReadStoreOrListen(t *testing.T) {
	listener, err := startCoordinatorMetrics(context.Background(), config.Config{})
	if err != nil || listener != nil {
		t.Fatal(listener, err)
	}
}
