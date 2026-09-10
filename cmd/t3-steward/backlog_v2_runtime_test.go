package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestRunBacklogV2DisabledHasNoStartupEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	cfg := config.Default()
	cfg.StatePath = path
	handled, err := runBacklogV2(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("disabled runtime created state: %v", err)
	}
}

func TestRunBacklogV2CoordinatorStartsClosedAndAdvancesEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	cfg := config.Default()
	cfg.StatePath = path
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	handled, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.CoordinatorEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if epoch <= 1 {
		t.Fatalf("coordinator epoch did not advance: %d", epoch)
	}
	admissions, err := store.LoadQuotaAdmissions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(admissions) != 0 {
		t.Fatalf("startup unexpectedly opened admission: %+v", admissions)
	}
}
