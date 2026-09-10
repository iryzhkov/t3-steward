package domain

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestProgressAndControlStateAreIndependent(t *testing.T) {
	attempt := Attempt{
		ID:            "attempt-1",
		WorkflowRunID: "run-1",
		TaskID:        "task-1",
		Number:        1,
		Progress:      ProgressActive,
		Control:       ControlPaused,
	}

	if attempt.Progress.Terminal() {
		t.Fatal("an active attempt must not be terminal while paused")
	}
	if attempt.Control.HoldsProviderSlot() {
		t.Fatal("a paused attempt must release its provider slot")
	}

	attempt.Control = ControlDraining
	if !attempt.Control.HoldsProviderSlot() {
		t.Fatal("a draining attempt must retain its provider slot")
	}
	if attempt.Progress != ProgressActive {
		t.Fatal("changing execution control must not change progress")
	}
}

func TestProgressTerminalStates(t *testing.T) {
	tests := []struct {
		state    ProgressState
		terminal bool
	}{
		{ProgressQueued, false},
		{ProgressBlocked, false},
		{ProgressReady, false},
		{ProgressActive, false},
		{ProgressNeedsInput, false},
		{ProgressVerifying, false},
		{ProgressSucceeded, true},
		{ProgressFailed, true},
		{ProgressCancelled, true},
		{ProgressSkipped, true},
	}
	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("%q Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}

func TestOrchestratorDomainJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	later := now.Add(time.Hour)
	estimatedCost := 37.5
	route := ProviderRoute{
		WorkerID:           "normandy",
		ProviderInstanceID: "codex",
		Model:              "gpt-5.6-sol",
		Options:            map[string]string{"effort": "high"},
		QuotaPoolID:        "openai-primary",
	}
	fixture := struct {
		Workflow         Workflow         `json:"workflow"`
		WorkflowRun      WorkflowRun      `json:"workflowRun"`
		Task             Task             `json:"task"`
		Attempt          Attempt          `json:"attempt"`
		Assignment       Assignment       `json:"assignment"`
		Schedule         Schedule         `json:"schedule"`
		ScheduleTemplate ScheduleTemplate `json:"scheduleTemplate"`
		Trigger          Trigger          `json:"trigger"`
		QuotaPool        QuotaPool        `json:"quotaPool"`
		Artifact         Artifact         `json:"artifact"`
		AdminCommand     AdminCommand     `json:"adminCommand"`
	}{
		Workflow: Workflow{
			ID: "workflow-1", Version: 2, Name: "build", Project: "t3-steward", Class: TaskClassRequired,
			TaskIDs: []string{"task-1"}, InputArtifactIDs: []string{"artifact-input"}, CreatedAt: now,
		},
		WorkflowRun: WorkflowRun{
			ID: "run-1", WorkflowID: "workflow-1", ScheduleID: "schedule-1", TriggerID: "trigger-1",
			Progress: ProgressActive, InputArtifactIDs: []string{"artifact-input"}, Revision: 3,
			CreatedAt: now, UpdatedAt: later,
		},
		Task: Task{
			ID: "task-1", WorkflowID: "workflow-1", Name: "implement", Class: TaskClassRequired,
			Needs: []string{"inspect"}, PromptArtifactID: "prompt-1",
			InputArtifactIDs: []string{"artifact-input"},
			DependencyInputs: map[string][]string{"inspect": {"findings.md"}},
			Outputs:          []ArtifactDeclaration{{Name: "result.md", MediaType: "text/markdown"}},
			Verification:     []string{"go test ./..."}, Placement: Placement{Hosts: []string{"normandy"}, Capabilities: []string{"internet"}},
			Routes: []ProviderRoute{route}, ResourceLocks: []string{"project:t3-steward"},
			Importance: 5, Difficulty: 5, EstimatedCost: &estimatedCost, MaxTurns: 6, NotBefore: &now, Deadline: &later,
		},
		Attempt: Attempt{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: ProgressActive, Control: ControlPaused, AssignmentID: "assignment-1",
			ThreadID: "thread-1", CheckpointArtifactID: "checkpoint-1", UpdatedAt: later,
		},
		Assignment: Assignment{
			ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy", Route: route,
			State: AssignmentClaimed, Epoch: 7, LeaseToken: "lease-1", LeaseExpiresAt: later,
			DispatchToken: "dispatch-1", ThreadID: "thread-1", DispatchState: DispatchConfirmed,
			DispatchRevision: 2, DispatchConfirmedAt: &now, CreatedAt: now, UpdatedAt: later,
		},
		Schedule: Schedule{
			ID: "schedule-1", Name: "nightly", Version: 4, WorkflowID: "workflow-1",
			Expression: "0 2 * * *", Timezone: "America/Los_Angeles",
			Overlap: ScheduleOverlapForbid, Misfire: ScheduleMisfireSkip,
			AfterFailure: ScheduleFailureHold, Enabled: true, ActiveRunID: "run-1",
			Revision: 2, CreatedAt: now, UpdatedAt: later,
		},
		ScheduleTemplate: ScheduleTemplate{
			ScheduleID: "schedule-1", Version: 4, WorkflowID: "workflow-1",
			Expression: "0 2 * * *", Timezone: "America/Los_Angeles",
			Overlap: ScheduleOverlapForbid, Misfire: ScheduleMisfireSkip,
			AfterFailure: ScheduleFailureHold, CreatedAt: now,
		},
		Trigger: Trigger{
			ID: "trigger-1", ScheduleID: "schedule-1", ScheduleVersion: 4, NominalAt: now,
			OccurrenceKey: "schedule-1/2026-09-09T12:00:00Z", State: TriggerAccepted,
			WorkflowRunID: "run-1", ObservedAt: now,
		},
		QuotaPool: QuotaPool{
			ID: "openai-primary", Provider: "openai", AccountID: "account-1",
			ProviderInstanceIDs: []string{"codex"}, Buckets: []BucketKey{{
				ProviderInstanceID: "codex", LimitID: "primary", Window: WindowPrimary,
			}}, Admission: AdmissionConstrained, MaxConcurrent: 1, ActiveAssignments: 1, UpdatedAt: later,
		},
		Artifact: Artifact{
			ID: "artifact-1", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Kind: ArtifactCheckpoint, Name: "checkpoint.md", MediaType: "text/markdown",
			Size: 42, SHA256: "abc123", StoragePath: "artifacts/abc123", Producer: "task-1", CreatedAt: later,
		},
		AdminCommand: AdminCommand{
			ID: "command-1", Kind: "pause", TargetType: "task", TargetID: "task-1",
			ExpectedRevision: 3, Reason: "operator request", RequestedBy: "user",
			Payload: json.RawMessage(`{"now":false}`), State: AdminCommandPending, CreatedAt: later,
		},
	}

	raw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatalf("marshal domain fixture: %v", err)
	}
	var got struct {
		Workflow         Workflow         `json:"workflow"`
		WorkflowRun      WorkflowRun      `json:"workflowRun"`
		Task             Task             `json:"task"`
		Attempt          Attempt          `json:"attempt"`
		Assignment       Assignment       `json:"assignment"`
		Schedule         Schedule         `json:"schedule"`
		ScheduleTemplate ScheduleTemplate `json:"scheduleTemplate"`
		Trigger          Trigger          `json:"trigger"`
		QuotaPool        QuotaPool        `json:"quotaPool"`
		Artifact         Artifact         `json:"artifact"`
		AdminCommand     AdminCommand     `json:"adminCommand"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal domain fixture: %v", err)
	}
	if !reflect.DeepEqual(got, fixture) {
		t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", got, fixture)
	}
}
