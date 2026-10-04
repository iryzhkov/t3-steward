package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
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
	builder.Store = unavailableDisplayInventory{packageRecordStore{records: records}}
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
