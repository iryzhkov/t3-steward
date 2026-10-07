package backlog

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A review-declared task asks its worker for the workspace HEAD at collection,
// and is never offered to a worker that cannot report it. Other tasks are
// built exactly as before.
func TestReviewDeclaredPackageRequiresWorkspaceHead(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)

	records, assignment := packageBuilderFixture(now)
	offer, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(offer.Package.Package.RequiredCapabilities, workerproto.PackageCapabilityWorkspaceHead) {
		t.Fatalf("undeclared task requires workspace head: %v", offer.Package.Package.RequiredCapabilities)
	}

	records, assignment = packageBuilderFixture(now)
	records.Tasks[1].ReviewRequirements = &domain.TaskReviewRequirements{Version: 1}
	builder := packageBuilder(t, records)
	builder.WorkerCapabilities = map[string][]string{"normandy": {workerproto.PackageCapabilityWorkspaceHead}}
	offer, err = builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(offer.Package.Package.RequiredCapabilities, workerproto.PackageCapabilityWorkspaceHead) || !offer.Package.Package.RequiresWorkspaceHead() {
		t.Fatalf("review-declared package does not require workspace head: %v", offer.Package.Package.RequiredCapabilities)
	}

	builder.WorkerCapabilities = map[string][]string{"normandy": {}}
	if _, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityWorkspaceHead) {
		t.Fatalf("older worker was offered a review-declared task: %v", err)
	}
}
