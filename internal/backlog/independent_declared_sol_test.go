package backlog

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Mutate CURRENT custody after ResolveDeclared and before the owning freeze.
// The real SQLite writer must repeat the resolver's assignment predicates.
type independentDeclaredInterleave struct {
	*sqlite.Store
	mutate func()
}

func (s independentDeclaredInterleave) FreezeDeclaredReviewAuthority(ctx context.Context, f review.FrozenAuthority) (review.FrozenAuthority, error) {
	s.mutate()
	return s.Store.FreezeDeclaredReviewAuthority(ctx, f)
}
func TestIndependentDeclaredCurrentAssignment(t *testing.T) {
	for _, kind := range []string{"healthy", "project", "activation", "released", "epoch"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			f := newDeclaredAdmissionFixture(t)
			s := f.store.Store
			f.service.Store = independentDeclaredInterleave{s, func() {
				as := f.records.Assignments[0]
				switch kind {
				case "project":
					as.Project = "foreign"
				case "activation":
					as.ActivationID = "foreign-activation"
				case "released":
					as.State = domain.AssignmentReleased
				case "epoch":
					as.Epoch++
				}
				if err := s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{as}}); err != nil {
					t.Fatal(err)
				}
			}}
			got, err := f.service.FreezeDeclared(ctx, declaredRequest(f))
			_, found, loadErr := s.GetFrozenReviewAuthority(ctx, f.request.RunID, f.request.TaskID)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if kind == "healthy" {
				if err != nil || !found {
					t.Fatalf("healthy: %+v %v", got, err)
				}
			} else {
				if err == nil || found {
					t.Errorf("CURRENT %s drift admitted original authority: err=%v stored=%v", kind, err, found)
				}
				t.Logf("stale owning writer result for %s: %v", kind, err)
				// Epoch advancement is coherent new custody and can resolve afresh;
				// it must still reject the ORIGINAL pre-change authority.
				control := newDeclaredAdmissionFixture(t)
				current := control.records.Assignments[0]
				switch kind {
				case "project":
					current.Project = "foreign"
				case "activation":
					current.ActivationID = "foreign-activation"
				case "released":
					current.State = domain.AssignmentReleased
				case "epoch":
					current.Epoch++
				}
				if e := control.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{current}}); e != nil {
					t.Fatal(e)
				}
				resolved, e := control.service.ResolveDeclared(ctx, declaredRequest(control))
				if kind == "epoch" {
					if e != nil || resolved.Authority.Parent.AssignmentEpoch != current.Epoch {
						t.Fatalf("fresh epoch retry failed: %v", e)
					}
				} else if e == nil {
					t.Error("fresh drift unexpectedly resolved")
				}
			}
		})
	}
}
func TestIndependentDeclaredReceiptAndCopies(t *testing.T) {
	ctx := context.Background()
	f := newDeclaredAdmissionFixture(t)
	service := SubmissionService{Permanent: declarationValidator{f.catalog}}
	receipt, err := service.validatePermanent(ctx, f.source)
	if err != nil {
		t.Fatal(err)
	}
	m, err := LoadManifest(f.source)
	if err != nil {
		t.Fatal(err)
	}
	if err = receipt.ValidatePermanent(ctx, m); err != nil {
		t.Fatal(err)
	}
	task := m.Tasks["inspect"]
	// Find actual key without assuming fixture's task name.
	for name, v := range m.Tasks {
		if v.ReviewRequirements != nil {
			task = v
			delete(m.Tasks, name)
			v.ReviewRequirements.RoundLimit = 1
			m.Tasks[name] = v
			break
		}
	}
	_ = task
	if err = receipt.ValidatePermanent(ctx, m); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("reused receipt: %v", err)
	}
	// Ingestion must refuse the stale receipt before publishing another workflow.
	rewriteBundleManifest(t, f.source, strings.Replace(declaredManifestYAML(), "risk: routine", "risk: routine\n      round_limit: 1", 1))
	before, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: receipt}).Ingest(ctx, f.source); err == nil {
		t.Fatal("changed declaration ingested with original receipt")
	}
	after, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("receipt refusal wrote metadata")
	}
	// Domain selectors return detached declarations for template and graph paths.
	original := f.records.Tasks[0]
	for _, graph := range []bool{false, true} {
		run := f.records.WorkflowRuns[0]
		if graph {
			run.Graph = &domain.GraphDefinition{Tasks: []domain.Task{original}}
		}
		selected := domain.TasksForRun(run, []domain.Task{original})
		selected[0].ReviewRequirements.Members[0].ID = "forged"
		selected[0].ReviewRequirements.Criteria.Name = "forged"
		if domain.TaskDigest(original) != domain.TaskDigest(f.records.Tasks[0]) || original.ReviewRequirements.Members[0].ID == "forged" {
			t.Fatal("selector declaration aliases")
		}
	}
}
