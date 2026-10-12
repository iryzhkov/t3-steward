package backlog

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Use a terminal source graph, not prepopulated artifact metadata.
func terminalCarriedCommitFixture(now time.Time, worker string) (sqlite.CoordinatorRecords, domain.Assignment) {
	records, assignment := carriedCommitFixture(now, worker)
	source := domain.Task{ID: "task-source", WorkflowID: "workflow-source", Name: "implement",
		Outputs: []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}}
	records.Workflows = append(records.Workflows, domain.Workflow{ID: "workflow-source", Version: 2, Name: "source", TaskIDs: []string{source.ID}, CreatedAt: now})
	records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{
		ID: "run-source", WorkflowID: source.WorkflowID, Progress: domain.ProgressSucceeded,
		Sink: &domain.SinkTask{Progress: domain.ProgressSucceeded}, Revision: 1, CreatedAt: now, UpdatedAt: now,
		Graph: &domain.GraphDefinition{Tasks: []domain.Task{source}},
	})
	newer := records.Artifacts[len(records.Artifacts)-1]
	newer.ID, newer.AttemptID = "bundle-newer", "attempt-newer"
	records.Artifacts = append(records.Artifacts, newer)
	return records, assignment
}

func TestTerminalCarriedCommitOfferLoadsPinnedBundle(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name, worker               string
		missing, budget, incapable bool
	}{
		{name: "cross-worker", worker: "omarchy-pc"},
		{name: "same-worker", worker: "normandy"},
		{name: "missing", worker: "omarchy-pc", missing: true},
		{name: "budget", worker: "omarchy-pc", budget: true},
		{name: "incapable", worker: "omarchy-pc", incapable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			records, assignment := terminalCarriedCommitFixture(now, test.worker)
			if test.missing {
				records.Artifacts = slices.DeleteFunc(records.Artifacts, func(a domain.Artifact) bool { return a.ID == "bundle-source" })
			}
			if test.budget {
				for i := range records.Artifacts {
					if records.Artifacts[i].ID == "bundle-source" {
						records.Artifacts[i].Size = 2 << 20
					}
				}
			}
			builder := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}})
			store := builder.Store.(packageRecordStore).decisions
			builder.Store = store
			if test.incapable {
				builder.WorkerCapabilities["normandy"] = nil
			}
			ctx, scope := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
			defer scope.Finish()
			before, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			absent := func(r sqlite.CoordinatorRecords) bool {
				return !slices.ContainsFunc(r.Artifacts, func(a domain.Artifact) bool { return a.WorkflowRunID == "run-source" })
			}
			if !absent(before) {
				t.Fatal("terminal source artifacts present in tick snapshot")
			}
			offer, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
			if test.incapable {
				if err == nil {
					t.Fatal("incapable worker accepted")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				bundles := offer.Package.Package.CommitBundles
				switch {
				case test.missing || test.worker == "normandy":
					if len(bundles) != 0 {
						t.Fatalf("unexpected bundles: %+v", bundles)
					}
				case test.budget:
					if len(bundles) != 1 || bundles[0].Bundle != nil || bundles[0].Omitted == "" {
						t.Fatalf("omission: %+v", bundles)
					}
				default:
					if len(bundles) != 1 || bundles[0].WorkflowRunID != "run-source" || bundles[0].TaskID != "task-source" ||
						bundles[0].Bundle == nil || bundles[0].Bundle.ID != "bundle-source" || bundles[0].Bundle.SHA256 != strings.Repeat("c", 64) {
						t.Fatalf("pinned source bundle: %+v", bundles)
					}
				}
			}
			after, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !absent(before) || !absent(after) {
				t.Fatal("offer mutated cached tick artifacts")
			}
		})
	}
}

type closeAfterSnapshotStore struct {
	packageRecordStore
	store *sqlite.Store
}

func (s closeAfterSnapshotStore) LoadCoordinatorRecords(ctx context.Context) (sqlite.CoordinatorRecords, error) {
	records, err := s.store.LoadCoordinatorRecords(ctx)
	if err == nil {
		err = s.store.Close()
	}
	return records, err
}

func TestOrdinaryCarriedOfferDoesNotLoadSourceHistory(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := terminalCarriedCommitFixture(now, "omarchy-pc")
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].CarriedInputs[0].Name = "notes"
		} else {
			records.Tasks[i].CarriedInputs = []domain.CarriedInput{{SourceRunID: "run-source", ProducerTaskID: "task-source", Name: "repair"}}
		}
	}
	for i := range records.Artifacts {
		if records.Artifacts[i].ID == "reference-1" {
			records.Artifacts[i].Name = "notes"
		}
	}
	records.WorkflowRuns[1].Graph.Tasks[0].Outputs = append(records.WorkflowRuns[1].Graph.Tasks[0].Outputs, domain.ArtifactDeclaration{Name: "notes"})
	builder := packageBuilder(t, records)
	original := builder.Store.(packageRecordStore)
	healthy := packageBuilder(t, records).Store.(packageRecordStore)
	builder.Store = closeAfterSnapshotStore{packageRecordStore: healthy, store: original.decisions}
	ctx, scope := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
	defer scope.Finish()
	if _, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute)); err != nil {
		t.Fatalf("ordinary carried file tried to load source history: %v", err)
	}
}

func TestExternalCommitRetentionThenTerminalOffer(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := terminalCarriedCommitFixture(now, "omarchy-pc")
	records.Artifacts = slices.DeleteFunc(records.Artifacts, func(a domain.Artifact) bool { return a.ID == "reference-1" })
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].RunID = "run-1"
			records.Tasks[i].CarriedInputs = nil
			records.Tasks[i].DependencyInputs = map[string][]string{"run-source/implement": {"repair"}}
		}
	}
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}})
	store := builder.Store.(packageRecordStore).decisions
	ingester := BundleIngester{Store: store, NewTypedID: func(string) string { return "externally-retained" }}
	manifest := Manifest{Tasks: map[string]ManifestTask{"consumer": {InputsFrom: map[string][]string{"run-source/implement": {"repair"}}}}}
	if err := ingester.retainExternalInputs(context.Background(), manifest, &records, "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	builder.Store = store
	ctx, scope := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
	defer scope.Finish()
	offer, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	bundles := offer.Package.Package.CommitBundles
	if len(bundles) != 1 || bundles[0].Bundle == nil || bundles[0].Bundle.ID != "bundle-source" || bundles[0].Bundle.SHA256 != strings.Repeat("c", 64) {
		t.Fatalf("external bundle = %+v", bundles)
	}
}

func TestTerminalCarriedCommitOfferPropagatesLoaderError(t *testing.T) {
	now := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	records, assignment := terminalCarriedCommitFixture(now, "omarchy-pc")
	builder := commitBundleBuilder(t, records, map[string][]string{"normandy": {workerproto.PackageCapabilityCommitBundle}})
	original := builder.Store.(packageRecordStore)
	healthy := packageBuilder(t, records).Store.(packageRecordStore)
	builder.Store = closeAfterSnapshotStore{packageRecordStore: healthy, store: original.decisions}
	ctx, scope := sqlite.WithCoordinatorRecordsSnapshot(context.Background())
	defer scope.Finish()
	_, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
	if err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("loader error = %v, want closed database", err)
	}
}
