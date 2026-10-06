package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStructuredReviewImportRestartReplay(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		missing   bool
		want      domain.ProgressState
	}{
		{"changes", `{"verdict":"REQUEST_CHANGES","blocking_findings":2,"finding_titles":["lost evidence"]}`, false, domain.ProgressSucceeded},
		{"accept", `{"verdict":"APPROVE"}`, false, domain.ProgressSucceeded},
		{"malformed", `{"verdict":"maybe"}`, false, domain.ProgressFailed},
		{"missing", "", true, domain.ProgressFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			now := coordinatorTestTime
			dbPath := filepath.Join(t.TempDir(), "state.db")
			s, err := sqlite.OpenMigrated(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if s != nil {
					s.Close()
				}
			}()
			task := testTask("task")
			task.ReviewOutput = &domain.ReviewOutput{Verdict: "./verdict.json"}
			task.Outputs = []domain.ArtifactDeclaration{{Name: "./verdict.json", MediaType: "application/json"}}
			attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
			assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
			if err := s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			data := resultUploadOpener{"verdict": []byte(tc.raw), "final-message-attempt-1": []byte("review finished"), "thread-archive-attempt-1": []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)}
			objects := []workerproto.ArtifactObject{resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]), resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"])}
			if !tc.missing {
				objects = append(objects, resultObject("verdict", "results/verdict.json", "output", "application/json", data["verdict"]))
			}
			manifest := resultManifest(now, assignment, objects)
			response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
			importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: s, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: s}, MaxArtifactBytes: 4096, MaxTotalBytes: 16384, Now: func() time.Time { return now.Add(time.Minute) }}
			report, err := importer.Import(ctx, response, data)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != tc.want {
				t.Fatalf("report: %+v", report)
			}
			if tc.want == domain.ProgressFailed && !strings.Contains(report.Transition[0].Attempt.Failure, "review_output verification failed") {
				t.Fatalf("failure: %+v", report.Transition[0].Attempt)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = nil
			s, err = sqlite.OpenMigrated(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			importer.Store = s
			importer.Artifacts.Catalog = s
			replay, err := importer.Import(ctx, response, data)
			if err != nil || len(replay.Transition) != 0 {
				t.Fatalf("replay: %+v %v", replay, err)
			}
			records, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got := records.Attempts[0]
			if got.Progress != tc.want {
				t.Fatalf("restart progress: %+v", got)
			}
			if tc.want == domain.ProgressSucceeded {
				if got.ReviewVerdict == nil {
					t.Fatal("verdict lost on restart")
				}
				obs, err := domain.ResolveNodeState(domain.NodeRef{RunID: "run-1", TaskID: task.ID}, domain.NodeStateTerminal, records.WorkflowRuns, records.Tasks, records.Attempts, records.Assignments, nil)
				if err != nil || obs.ExitCode != 0 || obs.ReviewVerdict == nil || obs.Fields["review"] != got.ReviewVerdict.Verdict {
					t.Fatalf("node: %+v %v", obs, err)
				}
				if tc.name == "changes" && (obs.Fields["blocking"] != "2" || obs.ReviewVerdict.FindingTitles[0] != "lost evidence") {
					t.Fatal(obs)
				}
			}
		})
	}
}
