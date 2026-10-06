package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// gateAttestationFixture stores rounds gated tasks for worker-a, round 1 in
// run-0 and the rest in run-1, and returns a finalizer for round n.
func gateAttestationFixture(t *testing.T, rounds int) (context.Context, *sqlite.Store, CoordinatorResultImporter, func(int, []string) (GateReport, []byte, []byte)) {
	ctx := context.Background()
	now := coordinatorTestTime
	dir := h2GateRepository(t)
	storage := t.TempDir()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	records := sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-0", WorkflowID: "workflow"}, {ID: "run-1", WorkflowID: "workflow"}}}
	for n := 1; n <= rounds; n++ {
		task := testTask(fmt.Sprintf("round-%d", n))
		task.Gate = &domain.TaskGate{Commands: []string{"printf gated"}, Timeout: time.Second}
		run := "run-1"
		if n == 1 {
			run = "run-0"
		}
		attempt := domain.Attempt{ID: fmt.Sprintf("attempt-%d", n), WorkflowRunID: run, TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: fmt.Sprintf("assignment-%d", n), UpdatedAt: now}
		records.Tasks = append(records.Tasks, task)
		records.Attempts = append(records.Attempts, attempt)
		records.Assignments = append(records.Assignments, domain.Assignment{ID: attempt.AssignmentID, AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: fmt.Sprintf("lease-%d", n), DispatchToken: fmt.Sprintf("dispatch-%d", n), CreatedAt: now, UpdatedAt: now})
	}
	if err = store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	finalize := func(n int, origins []string) (GateReport, []byte, []byte) {
		req := h2GateRequest(dir, fmt.Sprintf("attempt-%d", n))
		req.Task, req.Attempt.TaskID, req.WorkerID = records.Tasks[n-1], records.Tasks[n-1].ID, "worker-a"
		result, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour, GateCacheOrigins: origins}).Finalize(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		cleanupImmutable(t, result.StorageDir)
		var gate, log []byte
		for _, artifact := range result.Artifacts {
			switch artifact.Name {
			case "gate":
				gate = readStoredArtifact(t, storage, artifact)
			case "gate/log.txt":
				log = readStoredArtifact(t, storage, artifact)
			}
		}
		return h2ReadGate(t, storage, result), gate, log
	}
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 65536, MaxTotalBytes: 262144, Now: func() time.Time { return now.Add(time.Minute) }}
	return ctx, store, importer, finalize
}

func gateAttestationSucceeded(t *testing.T, label string, result ResultImportReport, err error) {
	t.Helper()
	if err != nil || len(result.Transition) != 1 || result.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
		t.Fatalf("%s: transitions=%d err=%v", label, len(result.Transition), err)
	}
}

// gateAttestationOriginalPath is attempt-1's retained gate report object.
func gateAttestationOriginalPath(t *testing.T, ctx context.Context, store *sqlite.Store, importer CoordinatorResultImporter) string {
	t.Helper()
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range loaded.Artifacts {
		if a.AttemptID == "attempt-1" && a.Name == "gate" {
			return filepath.Join(importer.Artifacts.Root, filepath.FromSlash(a.StoragePath))
		}
	}
	t.Fatal("no original gate report")
	return ""
}

// An exact replay of an already-imported cached result is not corroborated
// again, so retention pruning the original does not reject it.
func TestCachedGateReplaySurvivesPrunedOriginal(t *testing.T) {
	ctx, _, importer, finalize := gateAttestationFixture(t, 2)
	_, gate, log := finalize(1, nil)
	result, err := gateAttestationImport(t, importer, 1, gate, log)
	gateAttestationSucceeded(t, "attempt-1", result, err)
	replayed, gate, log := finalize(2, []string{"attempt-1"})
	if !replayed.Cached {
		t.Fatal("attempt-2 not cached")
	}
	result, err = gateAttestationImport(t, importer, 2, gate, log)
	gateAttestationSucceeded(t, "attempt-2", result, err)
	if _, _, err := importer.Artifacts.Prune(ctx, coordinatorTestTime.Add(time.Hour), []string{"run-1"}); err != nil {
		t.Fatal(err)
	}
	result, err = gateAttestationImport(t, importer, 2, gate, log)
	if err != nil || len(result.Transition) != 0 {
		t.Fatalf("exact replay after original pruned: transitions=%d err=%v", len(result.Transition), err)
	}
}

// A storage failure reading the original is retryable: the result is neither
// rejected nor settled, and the same result imports once storage recovers.
func TestCachedGateOriginalReadFailureIsRetryable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission bits do not bind root")
	}
	ctx, store, importer, finalize := gateAttestationFixture(t, 2)
	_, gate, log := finalize(1, nil)
	result, err := gateAttestationImport(t, importer, 1, gate, log)
	gateAttestationSucceeded(t, "attempt-1", result, err)
	_, gate, log = finalize(2, []string{"attempt-1"})
	path := gateAttestationOriginalPath(t, ctx, store, importer)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	result, err = gateAttestationImport(t, importer, 2, gate, log)
	if err == nil || errors.Is(err, ErrResultImportRejected) || len(result.Transition) != 0 {
		t.Fatalf("storage failure settled the attempt: transitions=%d err=%v", len(result.Transition), err)
	}
	if err = os.Chmod(path, info.Mode()); err != nil {
		t.Fatal(err)
	}
	result, err = gateAttestationImport(t, importer, 2, gate, log)
	gateAttestationSucceeded(t, "attempt-2 after storage recovered", result, err)
}
