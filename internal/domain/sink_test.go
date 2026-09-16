package domain

import (
	"reflect"
	"testing"
	"time"
)

func TestSinkAggregatesLatestOutcomesAndPreservesCancellation(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name                       string
		states                     []ProgressState
		sink, run                  ProgressState
		failed, cancelled, skipped []string
	}{
		{"empty", nil, ProgressSucceeded, ProgressSucceeded, nil, nil, nil},
		{"success", []ProgressState{ProgressSucceeded, ProgressSucceeded}, ProgressSucceeded, ProgressSucceeded, nil, nil, nil},
		{"failure", []ProgressState{ProgressFailed, ProgressSkipped}, ProgressFailed, ProgressFailed, []string{"a"}, nil, []string{"b"}},
		{"cancel", []ProgressState{ProgressCancelled, ProgressSucceeded}, ProgressSucceeded, ProgressCancelled, nil, []string{"a"}, nil},
		{"skip", []ProgressState{ProgressSkipped}, ProgressSucceeded, ProgressSkipped, nil, nil, []string{"a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := WorkflowRun{ID: "r", WorkflowID: "w"}
			var tasks []Task
			var attempts []Attempt
			for i, state := range tc.states {
				id := string(rune('a' + i))
				tasks = append(tasks, Task{ID: id, Name: id, WorkflowID: "w"})
				attempts = append(attempts, Attempt{ID: id + "1", TaskID: id, WorkflowRunID: "r", Number: 1, Progress: ProgressFailed, Control: ControlStopped},
					Attempt{ID: id + "2", TaskID: id, WorkflowRunID: "r", Number: 2, Progress: state, Control: ControlStopped})
			}
			got, err := ProjectRunSink(run, tasks, attempts, nil, now)
			if err != nil {
				t.Fatal(err)
			}
			if got.Sink.Progress != tc.sink || got.Progress != tc.run || !got.Sink.CompletedAt.Equal(now) {
				t.Fatalf("run=%+v sink=%+v", got, got.Sink)
			}
			want := &SinkResult{FailedTaskIDs: append([]string{}, tc.failed...), CancelledTaskIDs: append([]string{}, tc.cancelled...), SkippedTaskIDs: append([]string{}, tc.skipped...)}
			if !reflect.DeepEqual(got.Sink.Result, want) {
				t.Fatalf("result=%+v want=%+v", got.Sink.Result, want)
			}
			again, err := ProjectRunSink(got, tasks, attempts, nil, now.Add(time.Hour))
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatalf("final changed: %+v %v", again, err)
			}
		})
	}
}

func TestSupervisionSettlementBarrier(t *testing.T) {
	finalGate := func(state GateState) Gate {
		return Gate{Definition: GateDefinition{ID: "final", Name: "final", ObservedTaskIDs: []string{"a"}, Final: true}, State: state}
	}
	protecting := Gate{
		Definition: GateDefinition{ID: "ship", Name: "ship", ObservedTaskIDs: []string{"a"}, ProtectedTaskIDs: []string{"b"}},
		State:      GateReadyForReview,
	}
	open := ReviewIncident{ID: "incident", State: IncidentOpen, RequiredDisposition: DispositionConcludeFailure}
	escalated := ReviewIncident{ID: "incident", State: IncidentEscalated, RequiredDisposition: DispositionOperatorAction}
	resolved := ReviewIncident{ID: "incident", State: IncidentResolved, RequiredDisposition: DispositionGateDecision}
	for _, tc := range []struct {
		name      string
		barrier   SupervisionBarrier
		runFailed bool
		settles   bool
		reason    SinkBarrierReason
	}{
		{name: "unsupervised settles", barrier: SupervisionBarrier{Gates: []Gate{finalGate(GatePendingEvidence)}, Incidents: []ReviewIncident{open}}, settles: true},
		{name: "open incident withholds", barrier: SupervisionBarrier{Supervised: true, Incidents: []ReviewIncident{open}}, reason: SinkBarrierUnresolvedIncident},
		{name: "escalated incident withholds", barrier: SupervisionBarrier{Supervised: true, Incidents: []ReviewIncident{escalated}}, reason: SinkBarrierUnresolvedIncident},
		{name: "open incident withholds a failed run too", barrier: SupervisionBarrier{Supervised: true, Incidents: []ReviewIncident{open}}, runFailed: true, reason: SinkBarrierUnresolvedIncident},
		{name: "resolved incident settles", barrier: SupervisionBarrier{Supervised: true, Incidents: []ReviewIncident{resolved}}, settles: true},
		{name: "unaccepted final gate withholds", barrier: SupervisionBarrier{Supervised: true, Gates: []Gate{finalGate(GateReadyForReview)}}, reason: SinkBarrierFinalGateUnaccepted},
		{name: "accepted final gate settles", barrier: SupervisionBarrier{Supervised: true, Gates: []Gate{finalGate(GateAccepted)}}, settles: true},
		{name: "cancelled final gate settles", barrier: SupervisionBarrier{Supervised: true, Gates: []Gate{finalGate(GateCancelled)}}, settles: true},
		{name: "failed run is not kept alive for a summary", barrier: SupervisionBarrier{Supervised: true, Gates: []Gate{finalGate(GatePendingEvidence)}}, runFailed: true, settles: true},
		{name: "a gate protecting a task never withholds settlement", barrier: SupervisionBarrier{Supervised: true, Gates: []Gate{protecting}}, settles: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := SupervisionSettlementBarrier(tc.barrier, tc.runFailed)
			if got.Settles != tc.settles || got.Reason != tc.reason {
				t.Fatalf("verdict = %+v, want settles %v reason %q", got, tc.settles, tc.reason)
			}
		})
	}
}

func TestSupervisedSinkWaitsForIncidentsAndFinalGate(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	tasks := []Task{{ID: "a", Name: "a", WorkflowID: "w"}}
	succeeded := []Attempt{{ID: "a1", TaskID: "a", WorkflowRunID: "r", Number: 1, Progress: ProgressSucceeded, Control: ControlStopped}}
	run := WorkflowRun{ID: "r", WorkflowID: "w"}
	barrier := SupervisionBarrier{
		Supervised: true,
		Gates: []Gate{{
			Definition: GateDefinition{ID: "final", Name: "final", ObservedTaskIDs: []string{"a"}, Final: true},
			State:      GateReadyForReview,
		}},
		Incidents: []ReviewIncident{{ID: "incident", RunID: "r", GateID: "final", State: IncidentOpen, RequiredDisposition: DispositionGateDecision}},
	}
	held, err := ProjectSupervisedRunSink(run, tasks, succeeded, nil, barrier, now)
	if err != nil || held.Sink.Progress.Terminal() {
		t.Fatalf("unresolved incident settled: %+v %v", held.Sink, err)
	}
	barrier.Incidents[0].State = IncidentResolved
	stillHeld, err := ProjectSupervisedRunSink(run, tasks, succeeded, nil, barrier, now)
	if err != nil || stillHeld.Sink.Progress.Terminal() {
		t.Fatalf("unaccepted final gate settled: %+v %v", stillHeld.Sink, err)
	}
	barrier.Gates[0].State = GateAccepted
	settled, err := ProjectSupervisedRunSink(run, tasks, succeeded, nil, barrier, now)
	if err != nil || settled.Sink.Progress != ProgressSucceeded || settled.Progress != ProgressSucceeded {
		t.Fatalf("resolved decisions did not settle: %+v %v", settled.Sink, err)
	}

	// A failed campaign is never kept alive solely for an optional summary.
	failed := []Attempt{{ID: "a1", TaskID: "a", WorkflowRunID: "r", Number: 1, Progress: ProgressFailed, Control: ControlStopped}}
	optional := SupervisionBarrier{Supervised: true, Gates: []Gate{{
		Definition: GateDefinition{ID: "final", Name: "final", ObservedTaskIDs: []string{"a"}, Final: true},
		State:      GatePendingEvidence,
	}}}
	concluded, err := ProjectSupervisedRunSink(run, tasks, failed, nil, optional, now)
	if err != nil || concluded.Sink.Progress != ProgressFailed || concluded.Progress != ProgressFailed {
		t.Fatalf("failed run held open for a summary: %+v %v", concluded.Sink, err)
	}

	// An unsupervised run settles exactly as it always has.
	plain, err := ProjectRunSink(run, tasks, succeeded, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := ProjectSupervisedRunSink(run, tasks, succeeded, nil, SupervisionBarrier{}, now)
	if err != nil || !reflect.DeepEqual(plain, zero) {
		t.Fatalf("zero barrier changed settlement: %+v %+v %v", plain, zero, err)
	}
}

func TestSinkWaitsForEveryExecutionIncludingOldAttempts(t *testing.T) {
	now := time.Now()
	tasks := []Task{{ID: "t", Name: "task", WorkflowID: "w"}}
	base := Attempt{ID: "a", TaskID: "t", WorkflowRunID: "r", Number: 1, Progress: ProgressSucceeded, Control: ControlStopped}
	for _, control := range []ControlState{ControlRunning, ControlPreparing, ControlDraining, ControlPaused, ControlPausedUncheckpointed, ControlResuming} {
		a := base
		a.Control = control
		got, err := ProjectRunSink(WorkflowRun{ID: "r", WorkflowID: "w"}, tasks, []Attempt{a}, nil, now)
		if err != nil || got.Sink.Progress.Terminal() {
			t.Fatalf("control %s settled: %+v %v", control, got.Sink, err)
		}
	}
	for _, state := range []AssignmentState{AssignmentOffered, AssignmentClaimed, AssignmentUnknown} {
		a := base
		a.AssignmentID = "assignment"
		a.Progress = ProgressFailed
		retry := base
		retry.ID = "retry"
		retry.Number = 2
		got, err := ProjectRunSink(WorkflowRun{ID: "r", WorkflowID: "w"}, tasks, []Attempt{a, retry}, []Assignment{{ID: "assignment", AttemptID: "a", State: state}}, now)
		if err != nil || got.Sink.Progress.Terminal() {
			t.Fatalf("old %s settled: %+v %v", state, got.Sink, err)
		}
	}
	a := base
	a.AssignmentID = "missing"
	got, err := ProjectRunSink(WorkflowRun{ID: "r", WorkflowID: "w"}, tasks, []Attempt{a}, nil, now)
	if err != nil || got.Sink.Progress.Terminal() {
		t.Fatalf("missing containment settled: %+v %v", got.Sink, err)
	}
	got, err = ProjectRunSink(WorkflowRun{ID: "r", WorkflowID: "w"}, tasks, nil, nil, now)
	if err != nil || got.Sink.Progress.Terminal() {
		t.Fatalf("missing attempt settled: %+v %v", got.Sink, err)
	}
}

func TestSinkRebindsAtGraphRevisionAndFinalityIsImmutable(t *testing.T) {
	tasks := []Task{{ID: "a", Name: "a", WorkflowID: "w"}}
	run, err := BindRunSink(WorkflowRun{ID: "r", WorkflowID: "w"}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	tasks = append(tasks, Task{ID: "b", Name: "b", WorkflowID: "w"})
	if _, err := BindRunSink(run, tasks); err == nil {
		t.Fatal("changed graph without revision")
	}
	run.GraphRevision++
	next, err := BindRunSink(run, tasks)
	if err != nil || next.Sink.ID != run.Sink.ID || !reflect.DeepEqual(next.Sink.Needs, []string{"a", "b"}) || next.Sink.GraphRevision != 2 {
		t.Fatalf("rebind=%+v err=%v", next.Sink, err)
	}
	if !reflect.DeepEqual(run.Sink.Needs, []string{"a"}) {
		t.Fatal("mutated prior revision")
	}
	attempts := []Attempt{{ID: "a1", TaskID: "a", WorkflowRunID: "r", Number: 1, Progress: ProgressSucceeded}, {ID: "b1", TaskID: "b", WorkflowRunID: "r", Number: 1, Progress: ProgressSucceeded}}
	final, err := ProjectRunSink(next, tasks, attempts, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	final.GraphRevision++
	if _, err := BindRunSink(final, tasks); err == nil {
		t.Fatal("amended final sink")
	}
	reserved := []Task{{ID: "bad", Name: SinkTaskName, WorkflowID: "w"}}
	if _, err := BindRunSink(WorkflowRun{ID: "r", WorkflowID: "w"}, reserved); err == nil {
		t.Fatal("accepted user sink")
	}
}
