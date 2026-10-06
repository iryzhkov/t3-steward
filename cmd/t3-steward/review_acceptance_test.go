package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReviewAcceptanceRefusesForgedClassification(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	snapshot := domain.WorkerSnapshot{WorkerID: "w", WorkerEpoch: "epoch", CoordinatorEpoch: 1, Sequence: 1, ObservedAt: now, ValidUntil: now.Add(time.Hour), Connected: true,
		Inventory: domain.WorkerInventory{ID: "w", Epoch: "epoch", Health: domain.WorkerHealthReady, Projects: []domain.WorkerProjectInventory{{Name: "scratch", Available: true}}, Providers: []domain.WorkerProviderInventory{{InstanceID: "a", Models: []string{"full"}, Available: true}, {InstanceID: "b", Models: []string{"full"}, Available: true}}}}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	service.SetRuntimeInfo(backlogadmin.RuntimeInfo{Epoch: 1, MaxWorkerSnapshotAge: time.Hour})
	service.SetViability(backlogadmin.ViabilitySettings{Projects: []backlog.ProjectDefinition{{Name: "scratch", Type: "fresh"}}, ReviewRoutes: map[string]config.ReviewRouteMetadata{"a/full": {ProviderFamily: "claude", Tier: "executor"}, "b/full": {ProviderFamily: "openai", Tier: "executor"}}})
	a, err := parseReviewArgs([]string{"--project", "scratch", "--reviewer", "a/full", "--reviewer", "b/full", "--no-notify"})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := buildReviewCampaign(a, reviewCatalog(), now)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	bundle, err := campaign.Load(dir, campaign.DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Manifest.Review.Reviewers[0].Tier = "critical"
	err = (coordinatorPermanentValidator{admin: service}).ValidatePermanent(ctx, bundle.Manifest)
	if err == nil || !strings.Contains(err.Error(), "authoritative catalog metadata") {
		t.Fatalf("forged tier accepted: %v", err)
	}
	bundle.Manifest.Review.Reviewers[0].Tier = "executor"
	bundle.Manifest.Review.Reviewers[0].ProviderFamily = "forged"
	err = (coordinatorPermanentValidator{admin: service}).ValidatePermanent(ctx, bundle.Manifest)
	if err == nil || !strings.Contains(err.Error(), "authoritative catalog metadata") {
		t.Fatalf("forged metadata accepted: %v", err)
	}
}
