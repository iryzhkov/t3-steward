package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func gateImportFixture(t *testing.T) (domain.Task, []domain.Artifact, [][]byte, map[string]any) {
	t.Helper()
	now := coordinatorTestTime
	task := testTask("task")
	task.Gate = &domain.TaskGate{Commands: []string{"make check-review"}, Timeout: time.Hour}
	artifacts := []domain.Artifact{
		{Kind: domain.ArtifactSummary, Name: "final-message.md"},
		{Kind: domain.ArtifactLog, Name: "thread.json"},
		{Kind: domain.ArtifactGate, Name: "gate", MediaType: "application/json"},
		{Kind: domain.ArtifactGate, Name: "gate/log.txt", MediaType: "text/plain"},
	}
	gate := map[string]any{
		"commands": []any{map[string]any{"command": "make check-review", "exitCode": 0, "startedAt": now.Add(-time.Second), "completedAt": now, "duration": time.Second}},
		"treeHash": strings.Repeat("a", 40), "worker": "worker-a", "toolVersions": map[string]string{"git": "git version 2.50"},
		"startedAt": now.Add(-time.Second), "completedAt": now, "passed": true, "logArtifact": "gate/log.txt",
		"cacheKey":        strings.Repeat("b", 64),
		"originalAttempt": "attempt-1",
	}
	raw, err := json.Marshal(gate)
	if err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{[]byte("finished"), []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`), raw, []byte("full gate output")}
	return task, artifacts, payloads, gate
}

func TestCoordinatorResultImporterRetainsGateLogAndVerdict(t *testing.T) {
	for _, mode := range []string{"passed", "failed", "postcondition", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			failed := mode != "passed"
			ctx := context.Background()
			task, artifacts, payloads, _ := gateImportFixture(t)
			task.Gate.Commands = []string{"printf gated"}
			if mode == "failed" {
				task.Gate.Commands = []string{"printf failed; exit 7"}
			}
			if mode == "postcondition" {
				task.Gate.Commands = []string{"printf changed > source.txt"}
			}
			storage := t.TempDir()
			req := h2GateRequest(h2GateRepository(t), "attempt-1")
			req.Task = task
			req.Attempt.TaskID = task.ID
			req.WorkerID = "worker-a"
			finalized, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}}).Finalize(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, finalized.StorageDir)
			for _, artifact := range finalized.Artifacts {
				if artifact.Name == "gate" {
					payloads[2] = readStoredArtifact(t, storage, artifact)
				}
				if artifact.Name == "gate/log.txt" {
					payloads[3] = readStoredArtifact(t, storage, artifact)
				}
			}
			now := coordinatorTestTime
			attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
			assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			if mode == "invalid" {
				payloads[2] = []byte(`{"unexpected":true}`)
			}
			ids := []string{"final-message-attempt-1", "thread-archive-attempt-1", "gate-attempt-1", "gate-log-attempt-1"}
			media := []string{"text/markdown", "application/json", "application/json", "text/plain"}
			data := resultUploadOpener{}
			var objects []workerproto.ArtifactObject
			for n, a := range artifacts {
				name := a.Name
				if name == "gate" {
					name = "gate/report.json"
				}
				data[ids[n]] = payloads[n]
				objects = append(objects, resultObject(ids[n], "results/"+name, string(a.Kind), media[n], payloads[n]))
			}
			manifest := resultManifest(now, assignment, objects)
			importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 65536, MaxTotalBytes: 262144, Now: func() time.Time { return now.Add(time.Minute) }}
			response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
			result, err := importer.Import(ctx, response, data)
			if mode == "invalid" {
				if !errors.Is(err, ErrResultImportRejected) || len(result.Transition) != 1 || result.Transition[0].Attempt.Progress != domain.ProgressFailed {
					t.Fatalf("malformed gate stranded attempt: result=%+v err=%v", result, err)
				}
				records, loadErr := store.LoadCoordinatorRecords(ctx)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if len(records.Artifacts) != 0 {
					t.Fatal("malformed evidence published")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := domain.ProgressSucceeded
			if failed {
				want = domain.ProgressFailed
			}
			if len(result.Artifacts) != 4 || len(result.Transition) != 1 || result.Transition[0].Attempt.Progress != want {
				t.Fatalf("import=%+v", result)
			}
			wantFailure := "gate command failed (7): " + task.Gate.Commands[0]
			if mode == "postcondition" {
				wantFailure = "gate command failed (1): git status"
			}
			if failed && !strings.Contains(result.Transition[0].Attempt.Failure, wantFailure) {
				t.Fatalf("failure=%q", result.Transition[0].Attempt.Failure)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			logs := 0
			for _, a := range records.Artifacts {
				if a.Kind == domain.ArtifactGate && a.Name == "gate/log.txt" {
					logs++
				}
			}
			if logs != 1 {
				t.Fatalf("retained logs=%d", logs)
			}
			replay, err := importer.Import(ctx, response, data)
			if err != nil || len(replay.Transition) != 0 {
				t.Fatalf("replay=%+v err=%v", replay, err)
			}
		})
	}
}

func TestResultGateEvidenceContract(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mutate      func(*domain.Task, *[]domain.Artifact, *[][]byte, map[string]any)
		wantError   bool
		wantFailure string
	}{
		{name: "success"},
		{name: "postcondition failed", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["passed"] = false
			g["failure"] = map[string]any{"command": "git status", "exitCode": 1, "reason": "gate changed the committed tree or workspace"}
		}, wantFailure: "gate command failed (1): git status"},
		{name: "precondition failed", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["passed"] = false
			g["commands"] = []any{}
			g["treeHash"] = ""
			g["cacheKey"] = ""
			g["toolVersions"] = map[string]string{}
			g["failure"] = map[string]any{"command": "git rev-parse HEAD^{tree}", "exitCode": 1, "reason": "missing HEAD"}
		}, wantFailure: "gate command failed (1): git rev-parse HEAD^{tree}"},
		{name: "cached", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["cached"] = true
			g["originalAttempt"] = "attempt-original"
		}},
		{name: "failure retained", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["passed"] = false
			g["commands"].([]any)[0].(map[string]any)["exitCode"] = 7
			g["failure"] = map[string]any{"command": "make check-review", "exitCode": 7, "reason": "exit status 7"}
		}, wantFailure: "gate command failed (7): make check-review"},
		{name: "missing", mutate: func(_ *domain.Task, a *[]domain.Artifact, p *[][]byte, _ map[string]any) {
			*a = (*a)[:2]
			*p = (*p)[:2]
		}, wantFailure: "missing gate evidence"},
		{name: "undeclared", mutate: func(task *domain.Task, _ *[]domain.Artifact, _ *[][]byte, _ map[string]any) { task.Gate = nil }, wantError: true},
		{name: "missing log", mutate: func(_ *domain.Task, a *[]domain.Artifact, p *[][]byte, _ map[string]any) {
			*a = (*a)[:3]
			*p = (*p)[:3]
		}, wantError: true},
		{name: "wrong command", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["commands"].([]any)[0].(map[string]any)["command"] = "true"
		}, wantError: true},
		{name: "wrong duration", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["commands"].([]any)[0].(map[string]any)["duration"] = -1
		}, wantError: true},
		{name: "bad tree", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) { g["treeHash"] = "bogus" }, wantError: true},
		{name: "unknown field", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) { g["unexpected"] = true }, wantError: true},
		{name: "cached missing original", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["cached"] = true
			delete(g, "originalAttempt")
		}, wantError: true},
		{name: "false success", mutate: func(_ *domain.Task, _ *[]domain.Artifact, _ *[][]byte, g map[string]any) {
			g["commands"].([]any)[0].(map[string]any)["exitCode"] = 7
		}, wantError: true},
		{name: "verify failed no gate", mutate: func(task *domain.Task, a *[]domain.Artifact, p *[][]byte, _ map[string]any) {
			task.Verification = []string{"verify"}
			*a = append((*a)[:2], domain.Artifact{Kind: domain.ArtifactVerification, Name: "verification/001.json", MediaType: "application/json"})
			*p = append((*p)[:2], verificationBytes(t, "verify", 1, coordinatorTestTime))
		}, wantFailure: "verification command failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, artifacts, payloads, gate := gateImportFixture(t)
			if tc.mutate != nil {
				tc.mutate(&task, &artifacts, &payloads, gate)
			}
			raw, err := json.Marshal(gate)
			if err != nil {
				t.Fatal(err)
			}
			for n, a := range artifacts {
				if a.Kind == domain.ArtifactGate && a.Name == "gate" {
					payloads[n] = raw
				}
			}
			passed, failure, _, err := evaluateResultEvidence(task, "thread-1", artifacts, payloads, nil)
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v, wantError=%v", err, tc.wantError)
			}
			if tc.wantError {
				return
			}
			if tc.wantFailure != "" {
				if passed || !strings.Contains(failure, tc.wantFailure) {
					t.Fatalf("passed=%v failure=%q", passed, failure)
				}
			} else if !passed || failure != "" {
				t.Fatalf("passed=%v failure=%q", passed, failure)
			}
		})
	}
}
