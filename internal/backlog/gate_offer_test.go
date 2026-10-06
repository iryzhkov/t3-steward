package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestGateOfferRequiresAdvertisedCapability(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, a := packageBuilderFixture(now)
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].Gate = &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute}
		}
	}
	b := packageBuilder(t, records)
	b.GateCacheAge = 24 * time.Hour
	b.WorkerCapabilities = map[string][]string{a.WorkerID: {"internet"}}
	if _, err := b.BuildAssignmentOffer(context.Background(), a, now.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityWorkerOwnedGate) {
		t.Fatalf("old worker accepted: %v", err)
	}
	b.WorkerCapabilities[a.WorkerID] = []string{workerproto.PackageCapabilityWorkerOwnedGate}
	offer, err := b.BuildAssignmentOffer(context.Background(), a, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if offer.Package.Package.Gate == nil || offer.Package.Package.Limits.GateCacheAge != 24*time.Hour {
		t.Fatalf("gate lost %+v", offer.Package.Package)
	}
	mt := ManifestTask{Gate: &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute}}
	if !slices.Contains(placementCapabilities(Manifest{}, mt), workerproto.PackageCapabilityWorkerOwnedGate) {
		t.Fatal("gate placement not fenced")
	}
}
