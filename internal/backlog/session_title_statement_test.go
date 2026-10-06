package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// titleStore is a park store that also holds the worker observations the
// coordinator reads to learn which build a worker runs.
type titleStore struct {
	parkStore
	snapshots []domain.WorkerSnapshot
}

func (s titleStore) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return s.snapshots, nil
}

func titleWorker(capabilities ...string) domain.WorkerSnapshot {
	return domain.WorkerSnapshot{WorkerID: "normandy", WorkerEpoch: "worker-1", Inventory: domain.WorkerInventory{ID: "normandy", Capabilities: capabilities}}
}

func titleRecords(now time.Time) sqlite.CoordinatorRecords {
	completed := now.Add(-time.Hour)
	return sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{
			{ID: "run-1", WorkflowID: "wf-1", Progress: domain.ProgressActive, Sink: &domain.SinkTask{ID: domain.SinkTaskID("run-1"), Name: domain.SinkTaskName, Needs: []string{"t1", "t2", "t3"}}},
			{ID: "run-empty", WorkflowID: "wf-empty", Progress: domain.ProgressActive},
		},
		Tasks: []domain.Task{
			{ID: "t1", WorkflowID: "wf-1", Name: "plan"},
			{ID: "t2", WorkflowID: "wf-1", Name: "implement"},
			{ID: "t3", WorkflowID: "wf-1", Name: "review"},
			// A synthetic sink node is never executable work, wherever it is found.
			{ID: "sink-shadow", WorkflowID: "wf-1", Name: domain.SinkTaskName},
		},
		Attempts: []domain.Attempt{
			{ID: "a1-old", WorkflowRunID: "run-1", TaskID: "t1", Number: 1, Progress: domain.ProgressFailed, Revision: 3},
			{ID: "a1", WorkflowRunID: "run-1", TaskID: "t1", Number: 2, Progress: domain.ProgressSucceeded, Revision: 9, CompletedAt: &completed},
			{ID: "a2", WorkflowRunID: "run-1", TaskID: "t2", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 4},
			{ID: "a3", WorkflowRunID: "run-1", TaskID: "t3", Number: 1, Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal, Revision: 2},
			{ID: "a-old", WorkflowRunID: "run-1", TaskID: "t1", Number: 0, Progress: domain.ProgressSucceeded, Revision: 1},
			{ID: "a-empty", WorkflowRunID: "run-empty", TaskID: "gone", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlPreparing, Revision: 1},
		},
		Assignments: []domain.Assignment{
			{ID: "assign-1", AttemptID: "a1", WorkerID: "normandy", Epoch: 1, State: domain.AssignmentCompleted, UpdatedAt: completed},
			{ID: "assign-2", AttemptID: "a2", WorkerID: "normandy", Epoch: 3, State: domain.AssignmentClaimed, UpdatedAt: now},
			{ID: "assign-3", AttemptID: "a3", WorkerID: "tempest", Epoch: 1, State: domain.AssignmentClaimed, UpdatedAt: now},
			{ID: "assign-old", AttemptID: "a-old", WorkerID: "normandy", Epoch: 1, State: domain.AssignmentCompleted, UpdatedAt: now.Add(-72 * time.Hour)},
			{ID: "assign-offered", AttemptID: "a-empty", WorkerID: "normandy", Epoch: 1, State: domain.AssignmentOffered, UpdatedAt: now},
			{ID: "assign-empty", AttemptID: "a-empty", WorkerID: "normandy", Epoch: 2, State: domain.AssignmentClaimed, UpdatedAt: now},
		},
	}
}

// The coordinator states each of a worker's executions' recorded lifecycle and
// its campaign's executable progress, scoped to that worker, and only to a
// worker build that advertises it can use the statement.
func TestSessionStatementCarriesRecordedStateAndCampaignProgress(t *testing.T) {
	now := time.Now().UTC()
	source := titleStore{
		parkStore: parkStore{parked: map[string]string{}, records: titleRecords(now)},
		snapshots: []domain.WorkerSnapshot{titleWorker(workerproto.CapabilitySessionTitles)},
	}
	request, err := ParkedAssignmentsFor(context.Background(), source, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.ValidateSnapshotRequest(request); err != nil {
		t.Fatalf("the coordinator built a statement its own worker would refuse: %v", err)
	}
	if !request.SessionStatesReported {
		t.Fatal("session states are not reported to a capable worker")
	}
	got := map[string]workerproto.AssignmentSessionState{}
	for _, state := range request.SessionStates {
		got[state.AssignmentID] = state
	}
	if len(got) != 3 {
		t.Fatalf("statement = %+v, want this worker's three current executions", request.SessionStates)
	}
	done := got["assign-1"]
	if done.State != workerproto.SessionCompleted || done.AttemptID != "a1" || done.AssignmentEpoch != 1 || done.AttemptRevision != 9 {
		t.Fatalf("completed execution = %+v", done)
	}
	// Three executable tasks: the sink is excluded, and only the latest attempt
	// of each task counts, so the earlier failure of t1 does not.
	if done.Progress == nil || *done.Progress != (workerproto.CampaignProgress{Completed: 1, Total: 3}) {
		t.Fatalf("campaign progress = %+v, want 1/3", done.Progress)
	}
	running := got["assign-2"]
	if running.State != workerproto.SessionRunning || running.Progress == nil || *running.Progress != (workerproto.CampaignProgress{Completed: 1, Total: 3}) {
		t.Fatalf("running execution = %+v", running)
	}
	// A run with no executable task has no authoritative count; progress is omitted.
	if empty := got["assign-empty"]; empty.State != workerproto.SessionStarting || empty.Progress != nil {
		t.Fatalf("execution without a count = %+v", empty)
	}
	if _, ok := got["assign-3"]; ok {
		t.Fatal("another worker's execution was stated")
	}
	if _, ok := got["assign-old"]; ok {
		t.Fatal("a long-finished execution was stated")
	}
}

// A worker that does not advertise the capability decodes the snapshot request
// strictly, so the fields are never sent to it.
func TestSessionStatementIsNotSentToALegacyWorker(t *testing.T) {
	now := time.Now().UTC()
	for name, snapshots := range map[string][]domain.WorkerSnapshot{
		"no observation":     nil,
		"without capability": {titleWorker(workerproto.CapabilityTaskWaitCollectionFence)},
	} {
		source := titleStore{parkStore: parkStore{parked: map[string]string{}, records: titleRecords(now)}, snapshots: snapshots}
		request, err := ParkedAssignmentsFor(context.Background(), source, "normandy")
		if err != nil {
			t.Fatal(err)
		}
		if request.SessionStatesReported || len(request.SessionStates) != 0 {
			t.Fatalf("%s: legacy worker received session states: %+v", name, request.SessionStates)
		}
	}
}

func TestSessionStateComesOnlyFromCoordinatorRecordedLifecycle(t *testing.T) {
	for _, test := range []struct {
		progress domain.ProgressState
		control  domain.ControlState
		want     workerproto.SessionState
		ok       bool
	}{
		{domain.ProgressQueued, "", workerproto.SessionStarting, true},
		{domain.ProgressReady, domain.ControlUnassigned, workerproto.SessionStarting, true},
		{domain.ProgressActive, domain.ControlPreparing, workerproto.SessionStarting, true},
		{domain.ProgressActive, domain.ControlRunning, workerproto.SessionRunning, true},
		{domain.ProgressActive, domain.ControlResuming, workerproto.SessionRunning, true},
		{domain.ProgressActive, domain.ControlDraining, workerproto.SessionRunning, true},
		{domain.ProgressActive, domain.ControlPaused, workerproto.SessionWaiting, true},
		{domain.ProgressActive, domain.ControlWaitingExternal, workerproto.SessionWaiting, true},
		{domain.ProgressWaitingExternal, domain.ControlWaitingExternal, workerproto.SessionWaiting, true},
		{domain.ProgressNeedsInput, domain.ControlStopped, workerproto.SessionWaiting, true},
		{domain.ProgressActive, domain.ControlStopped, workerproto.SessionCollecting, true},
		{domain.ProgressVerifying, domain.ControlStopped, workerproto.SessionCollecting, true},
		{domain.ProgressSucceeded, domain.ControlStopped, workerproto.SessionCompleted, true},
		{domain.ProgressFailed, domain.ControlStopped, workerproto.SessionFailed, true},
		{domain.ProgressCancelled, domain.ControlStopped, workerproto.SessionCancelled, true},
		{domain.ProgressSkipped, "", "", false},
		{"invented", domain.ControlRunning, "", false},
	} {
		got, ok := SessionStateForAttempt(domain.Attempt{Progress: test.progress, Control: test.control})
		if got != test.want || ok != test.ok {
			t.Fatalf("%s/%s = %q, %v; want %q, %v", test.progress, test.control, got, ok, test.want, test.ok)
		}
	}
}
