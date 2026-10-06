package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// forgeGateCache writes the passing record an uncontained agent could write
// for a gate that actually fails: the cache directory is in its own account.
func forgeGateCache(t *testing.T, storage string, failing GateReport, original string) {
	t.Helper()
	forged := failing
	forged.Passed, forged.Failure, forged.OriginalAttempt = true, nil, original
	forged.Commands = append([]GateCommandResult(nil), failing.Commands...)
	forged.Commands[0].ExitCode, forged.Commands[0].Error = 0, ""
	raw, err := json.Marshal(gateCacheRecord{Report: forged, Log: []byte("fabricated pass\n")})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(storage, "gate-cache", failing.CacheKey+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// The review's reproduction: a fabricated pass for a failing gate's key was
// reused without running the gate. The worker now reuses a record only when
// the coordinator attests its origin, so the gate runs and fails.
func TestGateCacheIgnoresUnattestedForgedRecord(t *testing.T) {
	for name, origins := range map[string][]string{"no attestation": nil, "other attempt attested": {"real-pass"}} {
		t.Run(name, func(t *testing.T) {
			dir := h2GateRepository(t)
			storage := t.TempDir()
			req := h2GateRequest(dir, "forged")
			req.Task.Gate.Commands = []string{"exit 23"}
			f := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}
			failing, _, err := f.runGate(context.Background(), req)
			if err != nil || failing.Passed {
				t.Fatalf("control gate: passed=%v err=%v", failing.Passed, err)
			}
			forgeGateCache(t, storage, failing, "invented-success")
			runner := &directRunner{}
			f.Processes, f.GateCacheOrigins = runner, origins
			result, err := f.Finalize(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, result.StorageDir)
			got := h2ReadGate(t, storage, result)
			if got.Cached || got.Passed || len(runner.calls) != 2 {
				t.Fatalf("forged success reused: cached=%v passed=%v original=%s calls=%d", got.Cached, got.Passed, got.OriginalAttempt, len(runner.calls))
			}
		})
	}
}

// gateAttestationImport imports one attempt's result carrying the given gate
// report and log, as worker-a, for its own task.
func gateAttestationImport(t *testing.T, importer CoordinatorResultImporter, n int, gate, log []byte) (ResultImportReport, error) {
	t.Helper()
	now := coordinatorTestTime
	attempt := fmt.Sprintf("attempt-%d", n)
	assignment := domain.Assignment{ID: fmt.Sprintf("assignment-%d", n), AttemptID: attempt, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	thread := []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	data := resultUploadOpener{"final-message-" + attempt: []byte("finished"), "thread-archive-" + attempt: thread, "gate-" + attempt: gate, "gate-log-" + attempt: log}
	objects := []workerproto.ArtifactObject{
		resultObject("final-message-"+attempt, "results/final-message.md", string(domain.ArtifactSummary), "text/markdown", data["final-message-"+attempt]),
		resultObject("thread-archive-"+attempt, "results/thread.json", string(domain.ArtifactLog), "application/json", thread),
		resultObject("gate-"+attempt, "results/gate/report.json", string(domain.ArtifactGate), "application/json", gate),
		resultObject("gate-log-"+attempt, "results/gate/log.txt", string(domain.ArtifactGate), "text/plain", log),
	}
	manifest := resultManifest(now, assignment, objects)
	manifest.ID = "upload-" + assignment.ID + "-result"
	return importer.Import(context.Background(), workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}, data)
}

func TestCoordinatorCorroboratesCachedGateAgainstRecordedOriginal(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	dir := h2GateRepository(t)
	storage := t.TempDir()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const rounds = 6
	records := sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow"}}}
	for n := 1; n <= rounds; n++ {
		task := testTask(fmt.Sprintf("round-%d", n))
		task.Gate = &domain.TaskGate{Commands: []string{"printf gated"}, Timeout: time.Second}
		attempt := domain.Attempt{ID: fmt.Sprintf("attempt-%d", n), WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: fmt.Sprintf("assignment-%d", n), UpdatedAt: now}
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
	succeeded := func(n int, result ResultImportReport, err error) {
		t.Helper()
		if err != nil || len(result.Transition) != 1 || result.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
			t.Fatalf("attempt-%d: result=%+v err=%v", n, result, err)
		}
	}

	original, gate, log := finalize(1, nil)
	if original.Cached {
		t.Fatal("first gate reported cached")
	}
	result, err := gateAttestationImport(t, importer, 1, gate, log)
	succeeded(1, result, err)

	// An authentic replay of attempt-1's pass is corroborated.
	replayed, gate, log := finalize(2, []string{"attempt-1"})
	if !replayed.Cached || replayed.OriginalAttempt != "attempt-1" {
		t.Fatalf("replay not cached: %+v", replayed)
	}
	result, err = gateAttestationImport(t, importer, 2, gate, log)
	succeeded(2, result, err)

	// Forged cached reports are rejected even though each passes the
	// stand-alone evidence contract.
	for n, forge := range map[int]func(*GateReport){
		3: func(r *GateReport) { r.OriginalAttempt = "invented-success" },
		4: func(r *GateReport) { r.CacheKey = strings.Repeat("c", 64) },
		5: func(r *GateReport) { r.CompletedAt = r.CompletedAt.Add(time.Millisecond) },
		6: func(r *GateReport) { r.OriginalAttempt = "attempt-2" },
	} {
		forged := replayed
		forge(&forged)
		raw, err := json.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
		if err = validateGateReport(records.Tasks[n-1], forged); err != nil {
			t.Fatalf("attempt-%d forgery is not well-formed: %v", n, err)
		}
		result, err := gateAttestationImport(t, importer, n, raw, log)
		if !errors.Is(err, ErrResultImportRejected) || len(result.Transition) != 1 || result.Transition[0].Attempt.Progress != domain.ProgressFailed {
			t.Fatalf("attempt-%d forged cached gate accepted: result=%+v err=%v", n, result, err)
		}
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range loaded.Artifacts {
		if artifact.Kind == domain.ArtifactGate && artifact.AttemptID != "attempt-1" && artifact.AttemptID != "attempt-2" {
			t.Fatalf("forged evidence published: %+v", artifact)
		}
	}
}

func TestGateOfferCarriesAttestedCacheOrigins(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, a := packageBuilderFixture(now)
	for i := range records.Tasks {
		if records.Tasks[i].ID == "task-consumer" {
			records.Tasks[i].Gate = &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute}
		}
	}
	previous := domain.Attempt{ID: "earlier-round", WorkflowRunID: "other-run", TaskID: "other-task", Number: 1, Progress: domain.ProgressSucceeded}
	records.Attempts = append(records.Attempts, previous)
	records.Artifacts = append(records.Artifacts, domain.Artifact{ID: "gate-earlier-round", WorkflowRunID: "other-run", TaskID: "other-task", AttemptID: previous.ID,
		Kind: domain.ArtifactGate, Name: "gate", Producer: "worker:" + a.WorkerID, CreatedAt: a.CreatedAt.Add(-time.Hour)})
	b := packageBuilder(t, records)
	b.GateCacheAge = 24 * time.Hour
	b.WorkerCapabilities = map[string][]string{a.WorkerID: {workerproto.PackageCapabilityWorkerOwnedGate}}
	offer, err := b.BuildAssignmentOffer(context.Background(), a, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got := offer.Package.Package.GateCacheOrigins; len(got) != 1 || got[0] != previous.ID {
		t.Fatalf("gate cache origins=%v", got)
	}
}

func TestGateCacheOriginsAttestOnlyRecordedPassesFromWorker(t *testing.T) {
	now := coordinatorTestTime
	gate := &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Second}
	attempt := func(id string, progress domain.ProgressState) domain.Attempt {
		return domain.Attempt{ID: id, Progress: progress}
	}
	report := func(attempt, worker string, age time.Duration) domain.Artifact {
		return domain.Artifact{ID: "gate-" + attempt, AttemptID: attempt, Kind: domain.ArtifactGate, Name: "gate", Producer: "worker:" + worker, CreatedAt: now.Add(-age)}
	}
	activation := attempt("activation", domain.ProgressSucceeded)
	activation.SupervisionActivationID = "activation-1"
	records := sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt("old", domain.ProgressSucceeded), attempt("new", domain.ProgressSucceeded), attempt("failed", domain.ProgressFailed),
			attempt("elsewhere", domain.ProgressSucceeded), attempt("stale", domain.ProgressSucceeded), attempt("future", domain.ProgressSucceeded), activation},
		Artifacts: []domain.Artifact{report("old", "worker-a", 2*time.Minute), report("new", "worker-a", time.Minute), report("failed", "worker-a", time.Minute),
			report("elsewhere", "worker-b", time.Minute), report("stale", "worker-a", 2*time.Hour), report("future", "worker-a", -time.Minute), report("activation", "worker-a", time.Minute),
			{ID: "log-new", AttemptID: "new", Kind: domain.ArtifactGate, Name: "gate/log.txt", Producer: "worker:worker-a", CreatedAt: now}},
	}
	if got := gateCacheOrigins(gate, records, "worker-a", now, time.Hour); strings.Join(got, ",") != "new,old" {
		t.Fatalf("origins=%v", got)
	}
	if got := gateCacheOrigins(nil, records, "worker-a", now, time.Hour); got != nil {
		t.Fatalf("ungated origins=%v", got)
	}
	if got := gateCacheOrigins(gate, records, "worker-a", now, 0); got != nil {
		t.Fatalf("disabled cache origins=%v", got)
	}
}
