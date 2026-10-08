package backlog

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const permanentResultArchive = `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`

// permanentResultCase is one immutable worker result and the coordinator state
// it is imported against.
type permanentResultCase struct {
	store    *sqlite.Store
	attempt  domain.Attempt
	response workerproto.ArtifactUploadResponse
	data     resultUploadOpener
	importer CoordinatorResultImporter
}

// newPermanentResultCase saves a verifying attempt of a task declaring
// answer.txt and one verification command, and builds a valid result for it.
// edit changes the task and the uploaded objects before the manifest and its
// custody chain are built, so every case is a well-formed upload whose content
// alone is wrong.
func newPermanentResultCase(t *testing.T, supervision bool, edit func(*domain.Task, *[]workerproto.ArtifactObject, resultUploadOpener)) permanentResultCase {
	t.Helper()
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	task := testTask("task")
	task.Outputs = []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}
	task.Verification = []string{"true"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
	if supervision {
		attempt.TaskID, attempt.SupervisionActivationID = "activation-1", "activation-1"
	}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	data := resultUploadOpener{
		"output-1":                 []byte("answer\n"),
		"verification-1":           verificationBytes(t, "true", 0, now),
		"final-message-attempt-1":  []byte("finished\n"),
		"thread-archive-attempt-1": []byte(permanentResultArchive),
	}
	objects := []workerproto.ArtifactObject{
		resultObject("output-1", "results/answer.txt", "output", "text/plain", data["output-1"]),
		resultObject("verification-1", "results/verification/001.json", "verification", "application/json", data["verification-1"]),
		resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
		resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"]),
	}
	if edit != nil {
		edit(&task, &objects, data)
	}
	records := sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}
	if !supervision {
		records.Tasks = []domain.Task{task}
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	manifest := resultManifest(now, assignment, objects)
	return permanentResultCase{
		store: store, attempt: attempt, data: data,
		response: workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")},
		importer: CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) }},
	}
}

// replaceResultObject swaps one uploaded object for new content under the same
// identity, so the manifest stays well formed and only the content is wrong.
func replaceResultObject(objects []workerproto.ArtifactObject, data resultUploadOpener, id string, content []byte) {
	for index, object := range objects {
		if object.ID == id {
			objects[index] = resultObject(id, object.Path, object.Kind, object.MediaType, content)
			data[id] = content
		}
	}
}

// A result whose content breaks the import contract breaks it on every retry,
// because the upload is immutable. Returning a plain error left the attempt
// verifying forever and, since the worker's result was never acknowledged,
// kept every later result from that worker queued behind it. Each such result
// is now settled as the attempt's failure through the dead-letter path, before
// anything is published.
func TestAPermanentlyInvalidResultSettlesTheAttemptInsteadOfRetrying(t *testing.T) {
	for _, test := range []struct {
		name        string
		supervision bool
		edit        func(*domain.Task, *[]workerproto.ArtifactObject, resultUploadOpener)
		want        string
	}{
		{name: "undeclared output", want: `undeclared output "extra.txt"`, edit: func(_ *domain.Task, objects *[]workerproto.ArtifactObject, data resultUploadOpener) {
			data["output-2"] = []byte("extra\n")
			*objects = append(*objects, resultObject("output-2", "results/extra.txt", "output", "text/plain", data["output-2"]))
		}},
		{name: "wrong output media type", want: `does not match declaration "text/plain"`, edit: func(_ *domain.Task, objects *[]workerproto.ArtifactObject, _ resultUploadOpener) {
			(*objects)[0].MediaType = "application/octet-stream"
		}},
		{name: "repeated output declaration", want: `declarations repeat output "answer.txt"`, edit: func(task *domain.Task, _ *[]workerproto.ArtifactObject, _ resultUploadOpener) {
			task.Outputs = append(task.Outputs, domain.ArtifactDeclaration{Name: "./answer.txt", MediaType: "text/plain"})
		}},
		{name: "malformed thread archive", want: "thread archive is invalid", edit: func(_ *domain.Task, objects *[]workerproto.ArtifactObject, data resultUploadOpener) {
			replaceResultObject(*objects, data, "thread-archive-attempt-1", []byte("not json"))
		}},
		{name: "malformed verification evidence", want: `verification "verification/001.json"`, edit: func(_ *domain.Task, objects *[]workerproto.ArtifactObject, data resultUploadOpener) {
			replaceResultObject(*objects, data, "verification-1", []byte(`{"command":`))
		}},
		{name: "verification evidence out of order", want: "not contiguous", edit: func(task *domain.Task, objects *[]workerproto.ArtifactObject, _ resultUploadOpener) {
			task.Verification = []string{"first", "true"}
			(*objects)[1].Path = "results/verification/002.json"
		}},
		{name: "recovery content without a proposal", supervision: true, want: "need a typed activation proposal", edit: func(_ *domain.Task, objects *[]workerproto.ArtifactObject, data resultUploadOpener) {
			// An activation declares no outputs and runs no verification.
			*objects = (*objects)[2:]
			data["recovery-instructions-attempt-1"] = []byte("retry it\n")
			*objects = append(*objects, resultObject("recovery-instructions-attempt-1", "results/recovery/instructions.md", "input", "text/markdown", data["recovery-instructions-attempt-1"]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			result := newPermanentResultCase(t, test.supervision, test.edit)
			report, err := result.importer.Import(ctx, result.response, result.data)
			if !errors.Is(err, ErrResultImportRejected) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want a rejection naming %q", err, test.want)
			}
			var invalid *ResultValidationError
			if !errors.As(err, &invalid) {
				t.Fatalf("rejection %v does not carry the typed validation error", err)
			}
			if len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressFailed ||
				!strings.Contains(report.Transition[0].Attempt.Failure, test.want) {
				t.Fatalf("the attempt was not settled with the reason: %#v", report.Transition)
			}
			records, err := result.store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Artifacts) != 0 || records.Attempts[0].Progress != domain.ProgressFailed {
				t.Fatalf("durable state after rejection: artifacts=%d progress=%q", len(records.Artifacts), records.Attempts[0].Progress)
			}
			// The upload stays immutable, so the worker may deliver it again
			// before it sees the acknowledgement. The replay is a rejection too,
			// and it changes nothing.
			replay, err := result.importer.Import(ctx, result.response, result.data)
			if !errors.Is(err, ErrResultImportRejected) && !errors.Is(err, ErrResultImportSuperseded) {
				t.Fatalf("replay error = %v, want one the caller acknowledges", err)
			}
			if len(replay.Transition) != 0 {
				t.Fatalf("replay transitioned the attempt again: %#v", replay.Transition)
			}
		})
	}
}

// failingUploadOpener fails to open any object, as a worker that is briefly
// unreachable does.
type failingUploadOpener struct{}

func (failingUploadOpener) OpenWorkerUpload(context.Context, workerproto.ArtifactObject) (io.ReadCloser, error) {
	return nil, errors.New("connection reset")
}

// A failure to read the upload says nothing about the result, so it stays an
// ordinary error that the next pass retries, and the attempt is not settled.
func TestATransientUploadFailureLeavesTheResultRetryable(t *testing.T) {
	ctx := context.Background()
	result := newPermanentResultCase(t, false, nil)
	_, err := result.importer.Import(ctx, result.response, failingUploadOpener{})
	if err == nil || errors.Is(err, ErrResultImportRejected) {
		t.Fatalf("error = %v, want a retryable error", err)
	}
	var invalid *ResultValidationError
	if errors.As(err, &invalid) {
		t.Fatalf("transport failure %v was classified as a validation error", err)
	}
	records, err := result.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if records.Attempts[0].Progress != domain.ProgressVerifying {
		t.Fatalf("a transient failure settled the attempt: %q", records.Attempts[0].Progress)
	}
	report, err := result.importer.Import(ctx, result.response, result.data)
	if err != nil || len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
		t.Fatalf("retry after the transient failure: report=%#v err=%v", report, err)
	}
}
