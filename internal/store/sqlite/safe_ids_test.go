package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestScheduledAttemptsArePathSafeAndReplayStable(t *testing.T) {
	for _, source := range []domain.ScheduleTriggerSource{domain.ScheduleTriggerScheduled, domain.ScheduleTriggerManual} {
		t.Run(string(source), func(t *testing.T) {
			ctx := context.Background()
			store := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureNextCycle, nil)
			defer store.Close()
			tasks := []domain.Task{
				{ID: "task-root", WorkflowID: "workflow-1", Name: "root"},
				{ID: "task-child", WorkflowID: "workflow-1", Name: "child", Needs: []string{"root"}},
			}
			if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: tasks}); err != nil {
				t.Fatal(err)
			}
			request := scheduleTriggerRequest("trigger-safe", "scheduled-run-safe", scheduleTriggerTestTime)
			request.Source = source
			first, err := store.CommitScheduleTrigger(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if first.WorkflowRun == nil || first.Trigger.State != domain.TriggerAccepted {
				t.Fatalf("trigger = %#v", first)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Attempts) != len(tasks) {
				t.Fatalf("attempt count = %d", len(records.Attempts))
			}
			for _, attempt := range records.Attempts {
				if want := domain.ScheduledAttemptID(request.WorkflowRunID, attempt.TaskID); attempt.ID != want || !domain.PathSafeID(attempt.ID) {
					t.Errorf("attempt = %q, want path-safe %q", attempt.ID, want)
				}
			}
			before := safeIDDatabaseSnapshot(t, store)
			// A timer replay may carry a different proposed run ID; the stored occurrence wins.
			request.WorkflowRunID = "unused-replay-run"
			replay, err := store.CommitScheduleTrigger(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if !replay.Replay || !reflect.DeepEqual(first.WorkflowRun, replay.WorkflowRun) || replay.Trigger != first.Trigger {
				t.Fatalf("replay = %#v, first = %#v", replay, first)
			}
			if after := safeIDDatabaseSnapshot(t, store); !reflect.DeepEqual(before, after) {
				t.Fatal("schedule replay changed durable rows")
			}
		})
	}
}

func TestRecoveryRetryAttemptIsPathSafe(t *testing.T) {
	ctx := context.Background()
	store, err := openMigratedFixture(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 21, 14, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now.Add(time.Minute) })
	config := domain.RecoveryConfig{Version: domain.RecoveryContractV1,
		Route: domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "model"}, PromptArtifactID: "repair-prompt",
		MaxAttemptsPerIncident: 2, IncidentDeadline: time.Hour, StalledAfter: time.Minute}
	// A legacy current attempt is deliberately retained and used without shape checks.
	sourceID := "attempt:recovery:legacy"
	prepareRecoveryFence(t, store, "run", sourceID, now, config)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow-run", Name: "task", Class: domain.TaskClassRequired}}}); err != nil {
		t.Fatal(err)
	}
	recovery := domain.NewRecoveryIncident(config, domain.RecoveryDiagnosticIdentity{FailureFingerprint: "check-category", EvidenceFingerprint: "content-old", StrategyFingerprint: "strategy-old"}, now)
	if _, _, err := store.OpenRecoveryIncident(ctx, RecoveryIncidentRequest{
		RunID: "run", IncidentID: "incident", EventID: "event", SourceTaskID: "task", SourceAttemptID: sourceID,
		ExpectedGraphRevision: 1, SourceAttemptRevision: 1, RecoveryConfig: config, Reason: "failed", Recovery: *recovery,
		EventRecord: []byte(`{"id":"event","runId":"run"}`), OpenedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	activation := domain.Activation{ID: "activation", RunID: "run", Epoch: 1, Purpose: domain.RecoveryActivationRepair, GraphRevision: 1,
		Principal: "repair-principal", State: domain.ActivationActive, LeaseToken: "lease", LeaseExpiresAt: &expires, IncidentID: "incident"}
	raw, err := json.Marshal(activation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_supervision_activations(id,run_id,epoch,state,dispatch_identity,record) VALUES(?,?,?,?,?,?)`,
		activation.ID, activation.RunID, activation.Epoch, activation.State, "dispatch", raw); err != nil {
		t.Fatal(err)
	}
	instruction := domain.ArtifactDigest{ArtifactID: "instruction", Digest: "content-a"}
	request := domain.RecoveryRetryRequest{OperationID: "repair-op", RunID: "run", IncidentID: "incident", ExpectedIncidentRevision: 1, GraphRevision: 1,
		ActivationID: activation.ID, ActivationEpoch: 1, Principal: activation.Principal, SourceAttemptID: sourceID, SourceAttemptRevision: 1,
		InstructionArtifact: instruction, RequestedAt: now.Add(time.Minute),
		Diagnostic: domain.RecoveryDiagnosticIdentity{FailureFingerprint: "check-category", EvidenceFingerprint: "content-old",
			StrategyFingerprint: domain.RecoveryStrategyFingerprint(instruction, nil)}}
	prepareImportedRecoveryProposal(t, store, &request, activation, now)
	digest, err := supervisionPayloadDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitRecoveryRetry(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if want := domain.RecoveryAttemptID(digest); receipt.AttemptID != want || !domain.PathSafeID(receipt.AttemptID) {
		t.Errorf("recovery attempt = %q, want path-safe %q", receipt.AttemptID, want)
	}
	supplement, found, err := store.LoadRecoverySupplement(ctx, receipt.AttemptID)
	if err != nil || !found || supplement.AttemptID != receipt.AttemptID || supplement.SourceAttemptID != sourceID {
		t.Fatalf("supplement=%#v found=%v err=%v", supplement, found, err)
	}
	state, err := store.LoadSupervisionAdminState(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Incidents) != 1 || state.Incidents[0].Incident.Recovery.CurrentAttemptID != receipt.AttemptID {
		t.Fatalf("incident state = %#v", state.Incidents)
	}
	before := safeIDDatabaseSnapshot(t, store)
	replay, err := store.CommitRecoveryRetry(ctx, request)
	if err != nil || replay != receipt {
		t.Fatalf("replay=%#v err=%v want=%#v", replay, err, receipt)
	}
	if after := safeIDDatabaseSnapshot(t, store); !reflect.DeepEqual(before, after) {
		t.Fatal("recovery replay changed durable rows")
	}
}

func TestLegacyColonRecordsReplayUnchanged(t *testing.T) {
	for _, operation := range []string{"rerun", "clone", "task-add"} {
		t.Run(operation, func(t *testing.T) {
			store := openFleetTestStore(t)
			ctx := context.Background()
			request := domain.GraphAmendment{ID: "legacy-" + operation, RunID: "source", ExpectedRevision: 1, Operation: operation, Reason: "legacy request"}
			if operation == "rerun" {
				request.TaskID = "old-task"
			}
			if operation == "task-add" {
				request = amendmentAddTask("legacy-task-add", "added")
				request.RunID = "source"
			}
			runID, taskID := "source", "task:graph:legacy"
			if operation != "task-add" {
				runID = "run:" + operation + ":legacy"
				taskID = "task:" + operation + ":legacy:0"
			}
			result := domain.GraphAmendmentResult{
				Run:   domain.WorkflowRun{ID: runID},
				Graph: domain.GraphDefinition{RunID: runID, Tasks: []domain.Task{{ID: taskID}}},
			}
			encoded, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			reply, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(ctx, "INSERT INTO coordinator_graph_requests(id,actor,request,result) VALUES(?,?,?,?)",
				request.ID, "operator", string(encoded), string(reply)); err != nil {
				t.Fatal(err)
			}
			before := safeIDDatabaseSnapshot(t, store)
			replay, found, err := store.GraphAmendmentReplay(ctx, request, "operator")
			result.Replay = true
			if err != nil || !found || !reflect.DeepEqual(replay, result) {
				t.Fatalf("legacy replay=%#v found=%v err=%v want=%#v", replay, found, err, result)
			}
			// Store-level clone and rerun commits also must return the legacy reply
			// before inspecting the source or deriving replacement identities.
			commit := GraphCommit{Request: request, Actor: "operator", Now: fleetTestTime, Tasks: []domain.Task{{ID: "unused"}}, Rerun: &domain.RerunProvenance{}}
			switch operation {
			case "rerun":
				replay, err = store.CommitGraphRerun(ctx, commit)
			case "clone":
				replay, err = store.CommitGraphClone(ctx, commit)
			case "task-add":
				replay, err = store.CommitGraphAmendment(ctx, commit)
			}
			if err != nil || !reflect.DeepEqual(replay, result) {
				t.Fatalf("legacy commit replay=%#v err=%v want=%#v", replay, err, result)
			}
			if after := safeIDDatabaseSnapshot(t, store); !reflect.DeepEqual(before, after) {
				t.Fatal("legacy replay changed durable rows")
			}
		})
	}
}

func TestDerivedIDCollisionFailsTheTransaction(t *testing.T) {
	for _, operation := range []string{"rerun", "clone"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			sourceTasks := []domain.Task{amendmentTask("task-1", "producer")}
			store, source := amendmentFixture(t, false, sourceTasks, nil)
			if operation == "rerun" {
				source = currentRun(t, store, source.ID)
				source.Progress = domain.ProgressFailed
				source.Revision++
				if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{source}}); err != nil {
					t.Fatal(err)
				}
			}
			key := "collision-" + operation
			runID := domain.CloneRunID(key)
			if operation == "rerun" {
				runID = domain.RerunRunID(key)
			}
			tasks, inputs, remap := clonedTasks("collision-"+operation, runID, sourceTasks)
			existing := domain.WorkflowRun{ID: runID, WorkflowID: "unrelated", Revision: 1, Progress: domain.ProgressQueued, CreatedAt: fleetTestTime, UpdatedAt: fleetTestTime}
			if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{existing}}); err != nil {
				t.Fatal(err)
			}
			before := safeIDDatabaseSnapshot(t, store)
			commit := GraphCommit{Request: domain.GraphAmendment{ID: key, RunID: source.ID, ExpectedRevision: 1, Operation: operation, TaskID: "task-1", Reason: "collision probe"},
				Actor: "operator", Before: currentRun(t, store, source.ID), Tasks: tasks, Inputs: inputs, TaskIDRemap: remap, Now: fleetTestTime,
				Rerun: &domain.RerunProvenance{SourceRunID: source.ID, SourceTaskID: "task-1", SourceAttemptID: "attempt-task-1", IdempotencyKey: key, Reason: "collision probe"}}
			if operation == "clone" {
				commit.Request.TaskID = ""
			}
			var err error
			if operation == "rerun" {
				_, err = store.CommitGraphRerun(ctx, commit)
			} else {
				_, err = store.CommitGraphClone(ctx, commit)
			}
			if err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed: coordinator_workflow_runs.id") {
				t.Fatalf("expected run ID collision, got %v", err)
			}
			if after := safeIDDatabaseSnapshot(t, store); !reflect.DeepEqual(before, after) {
				t.Fatal("collision changed durable rows: task, attempt, input, pin or request escaped rollback")
			}
		})
	}
}

// Snapshot every coordinator table, including immutable receipts and pins that
// LoadCoordinatorRecords does not expose. Sorting permits arbitrary SELECT order.
func safeIDDatabaseSnapshot(t *testing.T, store *Store) map[string][]string {
	t.Helper()
	rows, err := store.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'coordinator_%'")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := make(map[string][]string, len(names))
	for _, name := range names {
		rows, err := store.db.Query("SELECT * FROM " + name)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var records []string
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			records = append(records, fmt.Sprintf("%#v", values))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		sort.Strings(records)
		snapshot[name] = records
	}
	return snapshot
}
