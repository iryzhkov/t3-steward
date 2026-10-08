package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestFailedCommitResultMismatchPreservesOrdinaryFailure(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTask("task")
	task.Outputs = []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}}
	task.Verification = []string{"first"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	data := resultUploadOpener{
		"verification-1":           verificationBytes(t, "first", 7, now),
		"final-message-attempt-1":  []byte("BACKLOG STATUS: continue\n"),
		"thread-archive-attempt-1": []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`),
	}
	objects := []workerproto.ArtifactObject{
		resultObject("verification-1", "results/verification/001.json", "verification", "application/json", data["verification-1"]),
		resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
		resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"]),
	}
	p := CommitProvenance{WorkflowRunID: attempt.WorkflowRunID, TaskID: task.ID, Name: "candidate", Repository: "repository", Base: strings.Repeat("1", 40), Commit: strings.Repeat("2", 40), Ref: FailedCampaignRef(attempt.WorkflowRunID, task.ID, attempt.ID, "candidate"), FailedAttempt: &FailedCommitAttempt{ID: attempt.ID, VerificationFailures: []string{"verification command failed (7): first"}}}
	raw, err := MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	data["failed-record"] = raw
	objects = append(objects, resultObject("failed-record", "results/"+FailedCommitArtifactName("candidate"), string(domain.ArtifactGitState), "application/json", raw))

	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) }}
	report, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Artifacts) != 3 || len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressFailed ||
		!strings.Contains(report.Transition[0].Attempt.Failure, "verification command failed (7): first") ||
		!strings.Contains(report.Transition[0].Attempt.Failure, "missing declared output: candidate") {
		t.Fatalf("failed result report = %#v", report)
	}
}
func TestFailedCommitResultDropsOnlyInvalidRecord(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTask("task")
	task.Outputs = []domain.ArtifactDeclaration{{Name: "candidate", Commit: &domain.CommitOutput{}}, {Name: "invalid", Commit: &domain.CommitOutput{}}}
	task.Verification = []string{"first"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	data := resultUploadOpener{
		"verification-1":           verificationBytes(t, "first", 7, now),
		"final-message-attempt-1":  []byte("BACKLOG STATUS: done\n"),
		"thread-archive-attempt-1": []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`),
	}
	objects := []workerproto.ArtifactObject{
		resultObject("verification-1", "results/verification/001.json", "verification", "application/json", data["verification-1"]),
		resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
		resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"]),
	}
	p := CommitProvenance{WorkflowRunID: attempt.WorkflowRunID, TaskID: task.ID, Name: "candidate", Repository: "repository", Base: strings.Repeat("1", 40), Commit: strings.Repeat("2", 40), Ref: FailedCampaignRef(attempt.WorkflowRunID, task.ID, attempt.ID, "candidate"), FailedAttempt: &FailedCommitAttempt{ID: attempt.ID, VerificationFailures: []string{"verification command failed (7): first"}}}
	raw, err := MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	data["failed-record"] = raw
	objects = append(objects, resultObject("failed-record", "results/"+FailedCommitArtifactName("candidate"), string(domain.ArtifactGitState), "application/json", raw))
	p.Name = "invalid"
	p.FailedAttempt = nil
	p.Ref = CampaignRef(attempt.WorkflowRunID, task.ID, "invalid")
	ordinary, err := MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	data["ordinary"] = ordinary
	objects = append(objects, resultObject("ordinary", "results/invalid", "output", "application/json", ordinary))
	data["invalid-record"] = []byte("invalid json")
	objects = append(objects, resultObject("invalid-record", "results/"+FailedCommitArtifactName("invalid"), string(domain.ArtifactGitState), "application/json", data["invalid-record"]))

	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) }}
	report, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Artifacts) != 5 || len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressFailed ||
		!strings.Contains(report.Transition[0].Attempt.Failure, "verification command failed (7): first") ||
		report.Transition[0].Attempt.Failure != "verification command failed (7): first" {
		t.Fatalf("failed result report = %#v", report)
	}
	for _, a := range report.Artifacts {
		if a.Name == FailedCommitArtifactName("invalid") {
			t.Fatal("invalid record published")
		}
	}
	if !slices.ContainsFunc(report.Artifacts, func(a domain.Artifact) bool { return a.Name == FailedCommitArtifactName("candidate") }) {
		t.Fatal("valid record discarded")
	}
}
