package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestFailedCommitConsumerRequiresNewWorkerCapabilityAtOffer(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := commitBundleFixture(now, "normandy")
	for i := range records.Tasks {
		if records.Tasks[i].Name == "consumer" {
			RequireFailedCommitCapability(&records.Tasks[i])
		}
	}
	old := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}})
	if _, err := old.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityFailedCommit) {
		t.Fatalf("old worker not refused by name: %v", err)
	}
	current := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle, workerproto.PackageCapabilityFailedCommit}})
	offer, err := current.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(offer.Package.Package.RequiredCapabilities, workerproto.PackageCapabilityFailedCommit) {
		t.Fatal("offer lost failed-commit capability")
	}
}
