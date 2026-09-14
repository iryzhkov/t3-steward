package backlog

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A whole result carrying preflight evidence has to import, not merely pass the
// identity check in isolation. Each layer of this contract refused preflight in
// a different way, and each refusal cost a release to discover, so this exercises
// the real importer end to end with the shape a preflight-declaring task
// actually produces.
func TestCoordinatorResultImporterAcceptsAResultCarryingPreflightEvidence(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	task := testTask("task")
	task.Outputs = []domain.ArtifactDeclaration{{Name: "plan.md", MediaType: "text/markdown"}}
	task.Verification = []string{"test -s plan.md"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}

	data := resultUploadOpener{
		"output-plan":                verificationBytes(t, "ignored", 0, now),
		"verification-1":             verificationBytes(t, "test -s plan.md", 0, now),
		"final-message-attempt-1":    []byte("BACKLOG STATUS: done\n"),
		"thread-archive-attempt-1":   []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`),
		"preflight-1111111111111111": []byte("go version go1.25.0\n"),
		"preflight-2222222222222222": []byte("HEAD 0123456\n"),
	}
	objects := []workerproto.ArtifactObject{
		resultObject("output-plan", "results/plan.md", "output", "text/markdown", data["output-plan"]),
		resultObject("verification-1", "results/verification/001.json", "verification", "application/json", data["verification-1"]),
		resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
		resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"]),
		resultObject("preflight-1111111111111111", "results/preflight/toolchain.log", "log", "text/plain; charset=utf-8", data["preflight-1111111111111111"]),
		resultObject("preflight-2222222222222222", "results/preflight/repository_state.log", "log", "text/plain; charset=utf-8", data["preflight-2222222222222222"]),
	}
	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 4096, MaxTotalBytes: 16384, Now: func() time.Time { return now.Add(time.Minute) }}

	report, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatalf("a result carrying preflight evidence was refused: %v", err)
	}
	if len(report.Artifacts) != len(objects) {
		t.Fatalf("imported %d artifacts, want %d", len(report.Artifacts), len(objects))
	}
	var preflight int
	for _, artifact := range report.Artifacts {
		if strings.HasPrefix(artifact.ID, "preflight-") {
			preflight++
		}
	}
	if preflight != 2 {
		t.Fatalf("preflight artifacts imported = %d, want 2", preflight)
	}
	if len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
		t.Fatalf("transition = %#v, want a succeeded attempt", report.Transition)
	}
}

// Discarding an unimportable result is not enough on its own. The attempt has
// already reached verifying, so with its result thrown away it can neither
// settle nor be retried — retry is invalid from verifying — and an operator's
// only remaining move is to cancel it by hand. A live campaign left eight
// attempts stranded exactly that way.
func TestCoordinatorResultImporterSettlesARejectedResult(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	task := testTask("task")
	task.Verification = nil
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}

	// A log naming an identity that is neither the thread archive nor preflight
	// evidence can never be imported, whatever is retried.
	data := resultUploadOpener{"artifact-unknown": []byte("whatever\n")}
	objects := []workerproto.ArtifactObject{
		resultObject("artifact-unknown", "results/mystery.log", "log", "text/plain; charset=utf-8", data["artifact-unknown"]),
	}
	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 4096, MaxTotalBytes: 16384, Now: func() time.Time { return now.Add(time.Minute) }}

	report, err := importer.Import(ctx, response, data)
	if !errors.Is(err, ErrResultImportRejected) {
		t.Fatalf("error = %v, want a rejection", err)
	}
	if len(report.Transition) != 1 {
		t.Fatalf("a rejected result left the attempt unsettled: %#v", report.Transition)
	}
	settled := report.Transition[0].Attempt
	if !settled.Progress.Terminal() {
		t.Fatalf("attempt progress = %q, want a terminal state so the run can proceed", settled.Progress)
	}
	// The reason has to survive as durable state, not only as a log line.
	if !strings.Contains(settled.Failure, "result import rejected") {
		t.Fatalf("attempt failure = %q, want the rejection recorded", settled.Failure)
	}
}

// A result whose objects violate the contract can never import, so it must be
// rejected in a way the caller can recognise and discard. The coordinator
// reconciles a worker in one pass, so an ordinary error aborts that pass and
// every other result the worker holds goes with it: one malformed result stalled
// an entire host until it was cancelled by hand.
func TestImportResultRejectsAnUnimportableObjectDistinguishably(t *testing.T) {
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1"}
	task := domain.Task{ID: "task-1"}
	manifest := workerproto.ArtifactTransferManifest{WorkerID: "homelab"}
	object := workerproto.ArtifactObject{
		ID: "artifact-generic", Path: "results/preflight/state.log", Kind: string(domain.ArtifactLog),
		MediaType: "text/plain; charset=utf-8", Size: 12, SHA256: strings.Repeat("a", 64),
	}
	_, err := resultArtifact(object, manifest, attempt, task, time.Now())
	if err == nil {
		t.Fatal("an artifact with no recognised log identity was accepted")
	}
	wrapped := fmt.Errorf("%w: %w", ErrResultImportRejected, err)
	if !errors.Is(wrapped, ErrResultImportRejected) {
		t.Fatal("a rejected result is not recognisable as rejected")
	}
	if errors.Is(wrapped, ErrResultImportSuperseded) {
		t.Fatal("a rejected result must not look superseded; the two are discarded for different reasons")
	}
}

// A log artifact is either the thread archive or preflight evidence. Preflight
// runs before the provider session exists, so it can never be the thread
// archive, and a contract that recognised only the archive rejected every
// preflight log. A live batch stalled on exactly that: the worker produced the
// evidence and the coordinator refused the whole result in a retry loop.
func TestResultArtifactAcceptsPreflightEvidenceAndTheThreadArchive(t *testing.T) {
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1"}
	task := domain.Task{ID: "task-1"}
	manifest := workerproto.ArtifactTransferManifest{WorkerID: "homelab"}
	now := time.Date(2026, 9, 14, 6, 0, 0, 0, time.UTC)
	digest := strings.Repeat("a", 64)

	accepted := map[string]workerproto.ArtifactObject{
		"thread archive": {
			ID: "thread-archive-attempt-1", Path: "results/thread.json", Kind: string(domain.ArtifactLog),
			MediaType: "application/json", Size: 12, SHA256: digest,
		},
		"preflight log": {
			ID: "preflight-0123456789abcdef", Path: "results/preflight/baseline_build.log",
			Kind: string(domain.ArtifactLog), MediaType: "text/plain; charset=utf-8", Size: 12, SHA256: digest,
		},
	}
	for name, object := range accepted {
		t.Run(name, func(t *testing.T) {
			artifact, err := resultArtifact(object, manifest, attempt, task, now)
			if err != nil {
				t.Fatalf("%s was refused: %v", name, err)
			}
			if artifact.Kind != domain.ArtifactLog {
				t.Fatalf("kind = %q", artifact.Kind)
			}
		})
	}

	// Widening the contract must not make the kind a way to import arbitrary
	// content under a name nobody declared.
	refused := map[string]workerproto.ArtifactObject{
		"archive under the wrong name": {
			ID: "thread-archive-attempt-1", Path: "results/elsewhere.json", Kind: string(domain.ArtifactLog),
			MediaType: "application/json", Size: 12, SHA256: digest,
		},
		"preflight name without a preflight id": {
			ID: "arbitrary-1", Path: "results/preflight/baseline.log", Kind: string(domain.ArtifactLog),
			MediaType: "text/plain; charset=utf-8", Size: 12, SHA256: digest,
		},
		"preflight id outside the preflight tree": {
			ID: "preflight-0123456789abcdef", Path: "results/secrets.txt", Kind: string(domain.ArtifactLog),
			MediaType: "text/plain; charset=utf-8", Size: 12, SHA256: digest,
		},
	}
	for name, object := range refused {
		t.Run(name, func(t *testing.T) {
			if _, err := resultArtifact(object, manifest, attempt, task, now); err == nil {
				t.Fatalf("%s was accepted", name)
			}
		})
	}
}
