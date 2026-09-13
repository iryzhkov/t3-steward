package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type resultUploadOpener map[string][]byte

func (o resultUploadOpener) OpenWorkerUpload(_ context.Context, object workerproto.ArtifactObject) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(o[object.ID])), nil
}

func TestCoordinatorResultImporterPublishesCustodyBeforeOutcomeAndReplays(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTask("task")
	task.Outputs = []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}
	task.Verification = []string{"true"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	data := resultUploadOpener{"output-1": []byte("answer\n"), "verification-1": verificationBytes(t, "true", 0, now), "final-message-attempt-1": []byte("finished\n"), "thread-archive-attempt-1": []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)}
	objects := []workerproto.ArtifactObject{resultObject("output-1", "results/answer.txt", "output", "text/plain", data["output-1"]), resultObject("verification-1", "results/verification/001.json", "verification", "application/json", data["verification-1"]), resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]), resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"])}
	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) }}
	report, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Artifacts) != 4 || len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressSucceeded || report.Transition[0].Attempt.FinalSummaryArtifactID != "final-message-attempt-1" {
		t.Fatalf("report = %#v", report)
	}
	replay, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Artifacts) != 4 || len(replay.Transition) != 0 {
		t.Fatalf("replay = %#v", replay)
	}
}

func TestCoordinatorResultImporterRejectsInvalidEvidenceBeforePublication(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	for _, test := range []struct {
		name            string
		mutate          func(*workerproto.ArtifactUploadResponse)
		terminalFailure bool
	}{
		{name: "custody checksum", mutate: func(response *workerproto.ArtifactUploadResponse) { response.Custody[0].RecordSHA256 = "bad" }},
		{name: "wrong output media type", mutate: func(response *workerproto.ArtifactUploadResponse) {
			response.Manifest.Objects[0].MediaType = "application/octet-stream"
			response.Custody = resultCustody(t, response.Manifest, "coordinator")
		}},
		{name: "unfinished marker", mutate: func(response *workerproto.ArtifactUploadResponse) {
			object := resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", []byte("BACKLOG STATUS: continue\n"))
			response.Manifest.TotalBytes += object.Size - response.Manifest.Objects[1].Size
			response.Manifest.Objects[1] = object
			response.Custody = resultCustody(t, response.Manifest, "coordinator")
		}, terminalFailure: true},
		{name: "malformed thread archive", mutate: func(response *workerproto.ArtifactUploadResponse) {
			object := resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", []byte("not json"))
			response.Manifest.TotalBytes += object.Size - response.Manifest.Objects[2].Size
			response.Manifest.Objects[2] = object
			response.Custody = resultCustody(t, response.Manifest, "coordinator")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			task := testTask("task")
			task.Outputs = []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}
			attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
			assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			data := resultUploadOpener{"output-1": []byte("answer\n"), "final-message-attempt-1": []byte("BACKLOG STATUS: done\n"), "thread-archive-attempt-1": []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)}
			objects := []workerproto.ArtifactObject{resultObject("output-1", "results/answer.txt", "output", "text/plain", data["output-1"]), resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]), resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"])}
			manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-result", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", AssignmentID: assignment.ID, AssignmentEpoch: 1, Objects: objects, TotalBytes: int64(len(data["output-1"]) + len(data["final-message-attempt-1"]) + len(data["thread-archive-attempt-1"])), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
			response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
			test.mutate(&response)
			if test.name == "unfinished marker" {
				data["final-message-attempt-1"] = []byte("BACKLOG STATUS: continue\n")
			}
			if test.name == "malformed thread archive" {
				data["thread-archive-attempt-1"] = []byte("not json")
			}
			importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) }}
			report, importErr := importer.Import(ctx, response, data)
			if test.terminalFailure {
				if importErr != nil || len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressFailed ||
					!strings.Contains(report.Transition[0].Attempt.Failure, "unfinished work") {
					t.Fatalf("deterministic failure report = %#v, err = %v", report, importErr)
				}
				return
			}
			if importErr == nil {
				t.Fatal("invalid evidence was accepted")
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Artifacts) != 0 || records.Attempts[0].Progress != domain.ProgressVerifying {
				t.Fatalf("records changed = %#v", records)
			}
		})
	}
}

func TestCoordinatorResultImporterProjectsVerificationAndMissingOutputFailure(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTask("task")
	task.Outputs = []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}
	task.Verification = []string{"first", "second"}
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
	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) }}
	report, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Artifacts) != 3 || len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressFailed ||
		!strings.Contains(report.Transition[0].Attempt.Failure, "verification command failed (7): first") ||
		!strings.Contains(report.Transition[0].Attempt.Failure, "missing declared output: answer.txt") {
		t.Fatalf("failed result report = %#v", report)
	}
}

func resultObject(id, path, kind, media string, data []byte) workerproto.ArtifactObject {
	sum := sha256.Sum256(data)
	return workerproto.ArtifactObject{ID: id, Path: path, Kind: kind, MediaType: media, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}

func resultManifest(now time.Time, assignment domain.Assignment, objects []workerproto.ArtifactObject) workerproto.ArtifactTransferManifest {
	var total int64
	for _, object := range objects {
		total += object.Size
	}
	return workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-result", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Objects: objects, TotalBytes: total, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
}

func verificationBytes(t *testing.T, command string, exitCode int, now time.Time) []byte {
	t.Helper()
	raw, err := json.Marshal(VerificationReport{Command: command, ExitCode: exitCode, StartedAt: now.Add(-time.Minute), CompletedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func resultCustody(t *testing.T, manifest workerproto.ArtifactTransferManifest, coordinator string) []workerproto.ArtifactCustodyRecord {
	t.Helper()
	result := make([]workerproto.ArtifactCustodyRecord, 0, len(manifest.Objects))
	previous := ""
	for index, object := range manifest.Objects {
		record, err := workerproto.BuildCustodyRecord(workerproto.ArtifactCustodyRecord{ManifestID: manifest.ID, ObjectID: object.ID, From: "worker:" + manifest.WorkerID, To: "outbox:" + coordinator, Sequence: int64(index + 1), Size: object.Size, SHA256: object.SHA256, VerifiedAt: manifest.CreatedAt, PreviousSHA256: previous})
		if err != nil {
			t.Fatal(err)
		}
		previous = record.RecordSHA256
		result = append(result, record)
	}
	return result
}
