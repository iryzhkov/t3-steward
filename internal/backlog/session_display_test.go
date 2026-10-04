package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"reflect"
	"strings"
	"testing"
	"time"
)

type unavailableDisplayInventory struct{ packageRecordStore }

func (s unavailableDisplayInventory) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return nil, errors.New("inventory unavailable")
}
func TestOfferSessionDisplayNegotiation(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	records.Workflows[0].Name = "Campaign\n café"
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].Name = "review build"
			records.Tasks[i].ReviewJudge = false
		}
	}
	baseline, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		caps map[string][]string
		want bool
	}{
		{"unknown", nil, false}, {"older", map[string][]string{assignment.WorkerID: {"internet"}}, false},
		{"other worker", map[string][]string{"other": {workerproto.PackageCapabilitySessionDisplay}}, false},
		{"new", map[string][]string{assignment.WorkerID: {workerproto.PackageCapabilitySessionDisplay}}, true},
		{"mixed", map[string][]string{assignment.WorkerID: {"internet", workerproto.PackageCapabilitySessionDisplay, "future-optional"}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := packageBuilder(t, records)
			builder.WorkerCapabilities = test.caps
			first, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			second, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(2*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(first.Package, second.Package) {
				t.Fatal("replay changed frozen display")
			}
			pkg := first.Package.Package
			if !reflect.DeepEqual(pkg.Identity, baseline.Package.Package.Identity) {
				t.Fatal("naming changed identity")
			}
			if !test.want {
				if !reflect.DeepEqual(first.Package, baseline.Package) {
					t.Fatal("legacy content address changed")
				}
				data, _ := json.Marshal(pkg)
				if strings.Contains(string(data), "\"display\"") {
					t.Fatal("legacy field present")
				}
				if !strings.Contains(workerproto.InitialSessionTitle(pkg), "task-consumer") {
					t.Fatal("legacy fallback missing")
				}
			} else {
				if pkg.Display == nil || pkg.Display.WorkflowName != "Campaign café" || pkg.Display.TaskName != "review build" || pkg.Display.ReviewJudge {
					t.Fatalf("display %+v", pkg.Display)
				}
				if err := workerproto.ValidateExecutionPackageManifest(first.Package, 1<<20); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	// A recorded judge also requires existing project-context support.
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].ReviewJudge = true
		}
	}
	judgeBuilder := packageBuilder(t, records)
	judgeBuilder.WorkerCapabilities = map[string][]string{assignment.WorkerID: {workerproto.PackageCapabilitySessionDisplay, workerproto.PackageCapabilityProjectContext}}
	judged, err := judgeBuilder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil || judged.Package.Package.Display == nil || !judged.Package.Package.Display.ReviewJudge {
		t.Fatalf("recorded judge missing: %v", err)
	}
	for i := range records.Tasks {
		records.Tasks[i].ReviewJudge = false
	}
	builder := packageBuilder(t, records)
	builder.Store = unavailableDisplayInventory{builder.Store.(packageRecordStore)}
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil || offer.Package.Package.Display != nil {
		t.Fatalf("optional inventory denied ordinary work: %v", err)
	}
}
func TestDisplaySupportDoesNotBypassMandatoryPreflight(t *testing.T) {
	builder, assignment, expiry := preflightOfferFixture(t, PackagePreflightSteps([]ManifestPreflightStep{{ID: "build", Kind: PreflightKindCheck, Command: []string{"go", "build"}}}))
	builder.WorkerCapabilities = map[string][]string{assignment.WorkerID: {workerproto.PackageCapabilitySessionDisplay}}
	if _, err := builder.BuildAssignmentOffer(context.Background(), assignment, expiry); err == nil || !strings.Contains(err.Error(), workerproto.PackageCapabilityPreflight) {
		t.Fatalf("mandatory gate weakened: %v", err)
	}
	builder.WorkerCapabilities[assignment.WorkerID] = append(builder.WorkerCapabilities[assignment.WorkerID], workerproto.PackageCapabilityPreflight)
	if _, err := builder.BuildAssignmentOffer(context.Background(), assignment, expiry); err != nil {
		t.Fatal(err)
	}
}

// Regression derived from the pinned Astra producer repro. Retain the same
// unavailable -> advertised inventory transition and assert omission stays frozen.
func TestReviewInventoryRecoveryChangesOrdinaryReplay(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	builder := packageBuilder(t, records)
	source := builder.Store.(packageRecordStore)
	builder.Store = unavailableDisplayInventory{source}
	first, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	source.snapshots = []domain.WorkerSnapshot{
		{WorkerID: assignment.WorkerID, Inventory: domain.WorkerInventory{Capabilities: []string{workerproto.PackageCapabilitySessionDisplay}}},
	}
	builder.Store = source
	second, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.Package.Package.Display != nil || second.Package.Package.Display != nil {
		t.Fatal("omission not frozen")
	}
	if !reflect.DeepEqual(first.Package, second.Package) {
		t.Fatalf("optional inventory recovery changed same-assignment manifest: %s -> %s", first.Package.SHA256, second.Package.SHA256)
	}
}

func TestSessionDisplayInventoryTransitionsAndReopen(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	for _, initiallySupported := range []bool{false, true} {
		t.Run(fmt.Sprint(initiallySupported), func(t *testing.T) {
			records, assignment := packageBuilderFixture(now)
			builder := packageBuilder(t, records)
			source := builder.Store.(packageRecordStore)
			if initiallySupported {
				builder.WorkerCapabilities = map[string][]string{assignment.WorkerID: {workerproto.PackageCapabilitySessionDisplay}}
			} else {
				builder.Store = unavailableDisplayInventory{source}
			}
			first, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if (first.Package.Package.Display != nil) != initiallySupported {
				t.Fatal("invalid first decision")
			}
			if initiallySupported {
				builder.WorkerCapabilities = nil
				builder.Store = unavailableDisplayInventory{source}
			} else {
				builder.Store = source
				builder.WorkerCapabilities = map[string][]string{assignment.WorkerID: {workerproto.PackageCapabilitySessionDisplay}}
			}
			replay, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
			if err != nil || !reflect.DeepEqual(first.Package, replay.Package) {
				t.Fatalf("inventory transition changed package: %v", err)
			}
			path := source.decisionsPath
			if err := source.decisions.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := sqlite.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			source.decisions = reopened
			if initiallySupported {
				builder.Store = unavailableDisplayInventory{source}
			} else {
				builder.Store = source
			}
			replay, err = builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
			if err != nil || !reflect.DeepEqual(first.Package, replay.Package) {
				t.Fatalf("reopen changed frozen package: %v", err)
			}
			// Actual coordinator restart changes only its epoch in the package.
			if _, err := reopened.AcquireCoordinator(ctx, "coordinator"); err != nil {
				t.Fatal(err)
			}
			builder.CoordinatorEpoch = 2
			replay, err = builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			replay.Package.Package.CoordinatorEpoch = first.Package.Package.CoordinatorEpoch
			normalized, err := workerproto.BuildExecutionPackageManifest(replay.Package.Package)
			if err != nil || !reflect.DeepEqual(first.Package, normalized) {
				t.Fatalf("coordinator restart changed descriptive decision: %v", err)
			}
			if initiallySupported {
				builder.WorkerCapabilities = map[string][]string{assignment.WorkerID: {}}
				if _, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "no longer supports") {
					t.Fatalf("known capability loss not refused: %v", err)
				}
			}
		})
	}
}

func TestSessionDisplayConcurrentFirstDecision(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	base := packageBuilder(t, records)
	source := base.Store.(packageRecordStore)
	secondDB, err := sqlite.Open(source.decisionsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer secondDB.Close()
	other := source
	other.decisions = secondDB
	builders := []CoordinatorOfferBuilder{base, base}
	builders[0].Store = unavailableDisplayInventory{source}
	builders[1].Store = other
	builders[1].WorkerCapabilities = map[string][]string{assignment.WorkerID: {workerproto.PackageCapabilitySessionDisplay}}
	start := make(chan struct{})
	results := make(chan workerproto.AssignmentOffer, 2)
	errs := make(chan error, 2)
	for _, builder := range builders {
		go func(b CoordinatorOfferBuilder) {
			<-start
			offer, err := b.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
			results <- offer
			errs <- err
		}(builder)
	}
	close(start)
	first, second := <-results, <-results
	for range builders {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(first.Package, second.Package) {
		t.Fatal("concurrent connections emitted different first decisions")
	}
}
