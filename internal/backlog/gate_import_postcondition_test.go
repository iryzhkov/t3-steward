package backlog

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// importFinalizedGate uploads a finalized attempt's artifacts, behind the
// fixture's final message and thread archive, through the coordinator's full
// result import into a migrated store. It returns the import, the importer and
// the upload so that a test can replay it.
func importFinalizedGate(t *testing.T, task domain.Task, storage string, finalized FinalizedAttempt) (ResultImportReport, *sqlite.Store, CoordinatorResultImporter, workerproto.ArtifactUploadResponse, resultUploadOpener) {
	t.Helper()
	ctx := context.Background()
	_, fixture, fixturePayloads, _ := gateImportFixture(t)
	artifacts, payloads := fixture[:2], fixturePayloads[:2]
	for _, artifact := range finalized.Artifacts {
		artifacts = append(artifacts, artifact)
		payloads = append(payloads, readStoredArtifact(t, storage, artifact))
	}
	now := coordinatorTestTime
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	data := resultUploadOpener{}
	var objects []workerproto.ArtifactObject
	for index, artifact := range artifacts {
		id, name, media := artifact.ID, artifact.Name, artifact.MediaType
		switch index {
		case 0:
			id, media = "final-message-attempt-1", "text/markdown"
		case 1:
			id, media = "thread-archive-attempt-1", "application/json"
		}
		if name == "gate" {
			name = "gate/report.json"
		}
		data[id] = payloads[index]
		objects = append(objects, resultObject(id, "results/"+name, string(artifact.Kind), media, payloads[index]))
	}
	manifest := resultManifest(now, assignment, objects)
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 2 << 20, MaxTotalBytes: 4 << 20, Now: func() time.Time { return now.Add(time.Minute) }}
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	result, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatalf("result import: %v", err)
	}
	return result, store, importer, response, data
}

// requireFailedWithRetainedGateLog checks that an import failed the attempt for
// the given reason and kept the gate log, and that its replay changes nothing.
func requireFailedWithRetainedGateLog(t *testing.T, result ResultImportReport, store *sqlite.Store, importer CoordinatorResultImporter, response workerproto.ArtifactUploadResponse, data resultUploadOpener, want string) {
	t.Helper()
	ctx := context.Background()
	if len(result.Transition) != 1 || result.Transition[0].Attempt.Progress != domain.ProgressFailed {
		t.Fatalf("import did not fail the attempt: %+v", result.Transition)
	}
	if !strings.Contains(result.Transition[0].Attempt.Failure, want) {
		t.Fatalf("failure=%q, want it to contain %q", result.Transition[0].Attempt.Failure, want)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	logs := 0
	for _, artifact := range records.Artifacts {
		if artifact.Kind == domain.ArtifactGate && artifact.Name == "gate/log.txt" {
			logs++
		}
	}
	if logs != 1 {
		t.Fatalf("retained gate logs=%d, want 1", logs)
	}
	replay, err := importer.Import(ctx, response, data)
	if err != nil || len(replay.Transition) != 0 {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

// A gate command moved the declared revision. The worker refused publication
// with a git rev-parse postcondition failure, and the coordinator rejected that
// valid evidence as malformed instead of retaining it.
func TestMovedRevisionGateFailureRetainsEvidenceThroughImport(t *testing.T) {
	task, _, _, _ := gateImportFixture(t)
	storage := t.TempDir()
	req := h2GateRequest(h2GateRepository(t), "attempt-1")
	base := gateBindingGit(t, req.WorkspaceDir, "rev-parse", "HEAD")
	writeTestFile(t, req.WorkspaceDir, "source.txt", "bad")
	h2Commit(t, req.WorkspaceDir)
	bad := gateBindingGit(t, req.WorkspaceDir, "rev-parse", "HEAD")
	gateBindingGit(t, req.WorkspaceDir, "checkout", "-q", "--detach", base)
	gateBindingGit(t, req.WorkspaceDir, "branch", "work", base)
	task.Gate.Commands = []string{fmt.Sprintf("grep -qx source source.txt && git update-ref refs/heads/work %s", bad)}
	task.Outputs = []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{Revision: "work"}}}
	req.Repository, req.BaseCommit = req.WorkspaceDir, base
	req.Task = task
	req.Attempt.TaskID = task.ID
	req.WorkerID = "worker-a"
	finalized, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, CampaignRefs: CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if finalized.Completion.VerificationPassed {
		t.Fatal("worker passed a gate whose declared revision moved")
	}
	result, store, importer, response, data := importFinalizedGate(t, task, storage, finalized)
	requireFailedWithRetainedGateLog(t, result, store, importer, response, data, "gate command failed (1): git rev-parse: HEAD or a declared commit revision moved during the gate")
}

// A child left by a passing gate command rewrote a tracked declared output
// before capture. The worker failed the attempt, but the coordinator decided
// from the uploaded passing gate report and succeeded it with the corrupted
// output.
func TestChangedOutputAfterGateFailsThroughImport(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	done := filepath.Join(t.TempDir(), "done")
	req := h2GateRequest(dir, "attempt-1")
	req.WorkerID = "worker-a"
	rewrite := `i=0; while [ $i -lt 400 ]; do echo bad > .s.tmp && mv .s.tmp source.txt; i=$((i+1)); done`
	req.Task.Gate.Commands = []string{gateBackgroundChild(filepath.Join(storage, "gate-cache"), rewrite, done)}
	req.Task.Gate.Timeout = 5 * time.Second
	req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "source.txt"}}
	finalized, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, done)
	cleanupImmutable(t, finalized.StorageDir)
	if finalized.Completion.VerificationPassed {
		t.Skip("the child lost the race to rewrite source.txt before capture")
	}
	result, store, importer, response, data := importFinalizedGate(t, req.Task, storage, finalized)
	requireFailedWithRetainedGateLog(t, result, store, importer, response, data, `gate command failed (1): git hash-object: declared output "source.txt" changed after the gate`)
}

// finalizeWithBackgroundRewrite passes a gate whose leftover child runs rewrite
// once the gate has finished, and requires both the worker and the
// coordinator to fail an attempt whose captured output is no longer "source".
func finalizeWithBackgroundRewrite(t *testing.T, dir, output, rewrite string) {
	t.Helper()
	storage := t.TempDir()
	done := filepath.Join(t.TempDir(), "done")
	req := h2GateRequest(dir, "attempt-1")
	req.WorkerID = "worker-a"
	req.Task.Gate.Commands = []string{gateBackgroundChild(filepath.Join(storage, "gate-cache"), rewrite, done)}
	req.Task.Gate.Timeout = 5 * time.Second
	req.Task.Outputs = []domain.ArtifactDeclaration{{Name: output}}
	finalized, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, done)
	cleanupImmutable(t, finalized.StorageDir)
	for _, artifact := range finalized.Artifacts {
		if artifact.Name != output {
			continue
		}
		if got := strings.TrimSpace(string(readStoredArtifact(t, storage, artifact))); got == "source" {
			t.Skip("the child lost the race to rewrite the output before capture")
		}
		if finalized.Completion.VerificationPassed {
			t.Fatalf("worker passed with a captured %s that the gate never saw", output)
		}
		result, store, importer, response, data := importFinalizedGate(t, req.Task, storage, finalized)
		requireFailedWithRetainedGateLog(t, result, store, importer, response, data, fmt.Sprintf("gate command failed (1): git hash-object: declared output %q changed after the gate", output))
		return
	}
	t.Fatalf("no output captured: %+v", finalized.Completion)
}

// A leftover child replaced a tracked output with a symlink to another
// tracked, unchanged file. The capture check followed the symlink and
// compared the other file with its own blob, so the swap passed.
func TestGateSymlinkSwapOfTrackedOutputFails(t *testing.T) {
	dir := h2GateRepository(t)
	writeTestFile(t, dir, "other.txt", "bad")
	gateBindingGit(t, dir, "add", "other.txt")
	gateBindingGit(t, dir, "commit", "-qm", "other")
	finalizeWithBackgroundRewrite(t, dir, "source.txt",
		`i=0; while [ $i -lt 400 ]; do ln -sfn other.txt .l && mv -T .l source.txt; i=$((i+1)); done`)
}

// A leftover child rewrote a declared output that Git ignores. Only tracked
// outputs were compared with the gated commit, so the rewrite passed.
func TestGateRewriteOfIgnoredOutputFails(t *testing.T) {
	dir := h2GateRepository(t)
	writeTestFile(t, dir, ".gitignore", "out.txt\n")
	gateBindingGit(t, dir, "add", ".gitignore")
	gateBindingGit(t, dir, "commit", "-qm", "ignore")
	writeTestFile(t, dir, "out.txt", "source")
	finalizeWithBackgroundRewrite(t, dir, "out.txt",
		`i=0; while [ $i -lt 400 ]; do echo bad > .o.tmp && mv .o.tmp out.txt; i=$((i+1)); done`)
}

// The amended report must remain valid evidence in both of its shapes: a fresh
// report, and a cached one whose original attempt passed elsewhere.
func TestAmendedGateReportIsValidEvidence(t *testing.T) {
	task, _, payloads, _ := gateImportFixture(t)
	var report GateReport
	if err := json.Unmarshal(payloads[2], &report); err != nil {
		t.Fatal(err)
	}
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) {
			source := report
			if cached {
				source.Cached, source.OriginalAttempt = true, "attempt-0"
			}
			raw, err := amendGateForChangedOutputs(source, "attempt-1", []string{`declared output "source.txt" changed after the gate`})
			if err != nil {
				t.Fatal(err)
			}
			amended, err := decodeGateEvidence(raw)
			if err != nil {
				t.Fatal(err)
			}
			if amended.Passed || amended.Cached != cached || len(amended.Commands) != 1 {
				t.Fatalf("amended=%+v", amended)
			}
			if err := validateGateReport(task, amended); err != nil {
				t.Fatalf("amended report rejected: %v", err)
			}
			// A tree or revision postcondition is checked by the gate itself,
			// which a cached report did not run here.
			for _, command := range []string{"git status", "git rev-parse"} {
				other := amended
				other.Failure = &GateFailure{Command: command, ExitCode: 1, Reason: "moved"}
				if err := validateGateReport(task, other); (err == nil) == cached {
					t.Fatalf("%s postcondition with cached=%v: err=%v", command, cached, err)
				}
			}
		})
	}
	// A reason or report beyond the coordinator's limits still yields
	// importable evidence.
	big := report
	big.Cached, big.OriginalAttempt = true, "attempt-0"
	big.ToolVersions = map[string]string{}
	for index := range 63 {
		big.ToolVersions[fmt.Sprintf("tool-%02d", index)] = strings.Repeat("v", 4000)
	}
	raw, err := amendGateForChangedOutputs(big, "attempt-1", []string{strings.Repeat("x", 9), strings.Repeat("é", 20000)})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > GateEvidenceMaxBytes {
		t.Fatalf("amended report is %d bytes", len(raw))
	}
	amended, err := decodeGateEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateGateReport(task, amended); err != nil {
		t.Fatalf("oversized amendment rejected: %v", err)
	}
}
