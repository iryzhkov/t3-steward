package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// N4: an authentic passing gate on one attempt approved a later attempt on
// the same tree and worker whose untracked or ignored declared output had
// changed, because the later attempt reused the cached pass instead of
// running the gate. With no gate reuse the later attempt runs the gate on its
// own content, and the coordinator fails it.
func TestGatePassOfOneAttemptNeverApprovesAnother(t *testing.T) {
	for _, ignored := range []bool{false, true} {
		t.Run(map[bool]string{false: "untracked", true: "ignored"}[ignored], func(t *testing.T) {
			ctx := context.Background()
			dir := h2GateRepository(t)
			if ignored {
				writeTestFile(t, dir, ".gitignore", "out.txt\n")
				gateBindingGit(t, dir, "add", ".gitignore")
				gateBindingGit(t, dir, "commit", "-qm", "ignore output")
			}
			writeTestFile(t, dir, "out.txt", "good\n")
			storage := t.TempDir()
			req := h2GateRequest(dir, "original")
			req.WorkerID = "worker-a"
			req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "out.txt"}}
			req.Task.Gate.Commands = []string{"grep -qx good out.txt"}
			now := coordinatorTestTime
			db, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			records := sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: req.Task.WorkflowID}}, Tasks: []domain.Task{req.Task}}
			for n, id := range []string{"original", "attempt-1"} {
				attempt := domain.Attempt{ID: id, WorkflowRunID: "run-1", TaskID: req.Task.ID, Number: n + 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: fmt.Sprintf("assignment-%d", n), UpdatedAt: now}
				records.Attempts = append(records.Attempts, attempt)
				records.Assignments = append(records.Assignments, domain.Assignment{ID: attempt.AssignmentID, AttemptID: id, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: fmt.Sprintf("lease-%d", n), DispatchToken: fmt.Sprintf("dispatch-%d", n), CreatedAt: now, UpdatedAt: now})
			}
			if err = db.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: db, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: db}, MaxArtifactBytes: 2 << 20, MaxTotalBytes: 4 << 20, Now: func() time.Time { return now.Add(time.Minute) }}
			upload := func(n int, finalized FinalizedAttempt) ResultImportReport {
				t.Helper()
				_, fixture, payloads, _ := gateImportFixture(t)
				var objects []workerproto.ArtifactObject
				data := resultUploadOpener{}
				id := records.Attempts[n].ID
				for k, a := range fixture[:2] {
					objectID, media := "final-message-"+id, "text/markdown"
					if k == 1 {
						objectID, media = "thread-archive-"+id, "application/json"
					}
					data[objectID] = payloads[k]
					objects = append(objects, resultObject(objectID, "results/"+a.Name, string(a.Kind), media, payloads[k]))
				}
				for _, a := range finalized.Artifacts {
					raw := readStoredArtifact(t, storage, a)
					name := a.Name
					if name == "gate" {
						name = "gate/report.json"
					}
					data[a.ID] = raw
					objects = append(objects, resultObject(a.ID, "results/"+name, string(a.Kind), a.MediaType, raw))
				}
				manifest := resultManifest(now, records.Assignments[n], objects)
				manifest.ID = "upload-" + id
				result, err := importer.Import(ctx, workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}, data)
				if err != nil {
					t.Fatal(err)
				}
				return result
			}

			f := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}}
			first, err := f.Finalize(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, first.StorageDir)
			if original := h2ReadGate(t, storage, first); !original.Passed || original.Attempt != "original" {
				t.Fatalf("original gate=%+v", original)
			}
			if imported := upload(0, first); len(imported.Transition) != 1 || imported.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
				t.Fatalf("original import=%+v", imported)
			}

			writeTestFile(t, dir, "out.txt", "bad\n")
			req.Attempt.ID = "attempt-1"
			second, err := f.Finalize(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, second.StorageDir)
			report := h2ReadGate(t, storage, second)
			if report.Passed || report.Attempt != "attempt-1" || len(report.Commands) != 1 || report.Commands[0].ExitCode == 0 || second.Completion.VerificationPassed {
				t.Fatalf("second attempt was not gated on its own content: report=%+v completion=%+v", report, second.Completion)
			}
			imported := upload(1, second)
			if len(imported.Transition) != 1 || imported.Transition[0].Attempt.Progress != domain.ProgressFailed {
				t.Fatalf("coordinator did not fail an attempt whose gate fails on bad out.txt: %+v", imported)
			}
		})
	}
}
