package domain

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// SinkTaskName is reserved for the coordinator-created terminal task.
const SinkTaskName = "__sink"

// SinkTask is a run-local task, stored with its owning run rather than in the
// immutable workflow's executable task definitions. It deliberately cannot name
// a worker, route or attempt.
type SinkTask struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	GraphRevision int64         `json:"graphRevision"`
	Needs         []string      `json:"needs"`
	Progress      ProgressState `json:"progress"`
	Result        *SinkResult   `json:"result,omitempty"`
	CompletedAt   *time.Time    `json:"completedAt,omitempty"`
}

type SinkResult struct {
	FailedTaskIDs    []string `json:"failedTaskIds"`
	CancelledTaskIDs []string `json:"cancelledTaskIds"`
	SkippedTaskIDs   []string `json:"skippedTaskIds"`
}

func SinkTaskID(runID string) string { return "sink:" + runID }

// SinkBarrierReason names the supervision condition that keeps a supervised run
// nonterminal. It is empty exactly when settlement proceeds.
type SinkBarrierReason string

const (
	// SinkBarrierUnresolvedIncident reports a review incident that is still
	// open or escalated. A reviewable condition must not settle before the
	// authorized actor has recorded its permitted disposition.
	SinkBarrierUnresolvedIncident SinkBarrierReason = "supervision-unresolved-incident"
	// SinkBarrierFinalGateUnaccepted reports a final-settlement gate that has
	// not been accepted.
	SinkBarrierFinalGateUnaccepted SinkBarrierReason = "supervision-final-gate-unaccepted"
)

// SupervisionBarrier is the supervision state the settlement barrier reads. Its
// zero value is the unsupervised run, which settles exactly as it always has.
type SupervisionBarrier struct {
	// Supervised is false for every run without a supervision record, which is
	// every run that exists today. A false value never withholds settlement.
	Supervised bool `json:"supervised"`
	// Gates are the run's gates. Only a final-settlement gate can withhold
	// settlement; a gate protecting a downstream task withholds that task's
	// dispatch instead, through SupervisionAdmits.
	Gates []Gate `json:"gates,omitempty"`
	// Incidents are the run's review incidents, resolved ones included.
	Incidents []ReviewIncident `json:"incidents,omitempty"`
}

// SinkBarrierVerdict is settle-or-not with the identity behind the refusal, so
// a status renderer can say which incident or which gate is holding the run
// open rather than only that something is.
type SinkBarrierVerdict struct {
	Settles    bool              `json:"settles"`
	Reason     SinkBarrierReason `json:"reason,omitempty"`
	IncidentID string            `json:"incidentId,omitempty"`
	GateID     string            `json:"gateId,omitempty"`
}

// SupervisionSettlementBarrier reports whether supervision permits terminal
// sink settlement of a run that is otherwise ready to settle.
//
// Two conditions withhold settlement, and only these two. An unresolved review
// incident withholds it, because a reviewable condition must not settle before
// the authorized actor records its permitted disposition. An unaccepted
// final-settlement gate withholds it, because that gate guards settlement
// itself rather than a downstream task.
//
// A run whose outcome already failed or was cancelled is never held open by a
// final gate alone: final reporting is an optional read-only delivery from the
// terminal snapshot, and the plan forbids keeping a failed campaign alive to
// obtain one. An unresolved incident still withholds such a run, because its
// disposition is a decision the overseer is required to record and is bounded
// by escalation rather than open-ended.
//
// It is pure: no I/O, no clock, no mutation.
func SupervisionSettlementBarrier(barrier SupervisionBarrier, runFailed bool) SinkBarrierVerdict {
	if !barrier.Supervised {
		return SinkBarrierVerdict{Settles: true}
	}
	for _, incident := range barrier.Incidents {
		if incident.State == IncidentOpen || incident.State == IncidentEscalated {
			return SinkBarrierVerdict{Reason: SinkBarrierUnresolvedIncident, IncidentID: incident.ID, GateID: incident.GateID}
		}
	}
	if runFailed {
		return SinkBarrierVerdict{Settles: true}
	}
	for _, gate := range barrier.Gates {
		if !gate.Definition.Final {
			continue
		}
		if gate.State == GateAccepted || gate.State == GateCancelled {
			continue
		}
		return SinkBarrierVerdict{Reason: SinkBarrierFinalGateUnaccepted, GateID: gate.Definition.ID}
	}
	return SinkBarrierVerdict{Settles: true}
}

// RunExecutionsQuiescent includes old attempts: a retry never erases custody of
// an earlier execution whose stop is still unproven.
//
// ControlWaitingExternal is not quiescent, and that is the whole point of the
// state. Letting a parked attempt look settled is the observed bug wearing a
// different name: the sink would publish a run result while a thread was still
// waiting to be woken and carry on working.
func RunExecutionsQuiescent(runID string, attempts []Attempt, assignments []Assignment) bool {
	byID := make(map[string]Assignment, len(assignments))
	owned := make(map[string]bool, len(attempts))
	for _, assignment := range assignments {
		byID[assignment.ID] = assignment
	}
	for _, attempt := range attempts {
		if attempt.WorkflowRunID != runID {
			continue
		}
		// A live overseer activation does not keep the run's own work from
		// being quiescent. It is not that work: it holds no task's workspace
		// and publishes no result, and the settlement barrier that does keep a
		// supervised run open -- an unresolved review incident or an
		// unaccepted final gate -- is expressed separately and honestly. Were
		// an activation counted here, an overseer would be waiting for a
		// settlement that was waiting for the overseer.
		if attempt.IsSupervisionActivation() {
			continue
		}
		owned[attempt.ID] = true
		if attempt.Control != "" && attempt.Control != ControlStopped && attempt.Control != ControlUnassigned {
			return false
		}
		if attempt.AssignmentID != "" {
			assignment, ok := byID[attempt.AssignmentID]
			if !ok || assignment.AttemptID != attempt.ID ||
				(assignment.State != AssignmentCompleted && assignment.State != AssignmentReleased) {
				return false
			}
		}
	}
	for _, assignment := range assignments {
		if owned[assignment.AttemptID] && assignment.State != AssignmentCompleted && assignment.State != AssignmentReleased {
			return false
		}
	}
	return true
}

func CloneSink(sink *SinkTask) *SinkTask {
	if sink == nil {
		return nil
	}
	copy := *sink
	copy.Needs = append([]string{}, sink.Needs...)
	if sink.Result != nil {
		result := *sink.Result
		result.FailedTaskIDs = append([]string{}, result.FailedTaskIDs...)
		result.CancelledTaskIDs = append([]string{}, result.CancelledTaskIDs...)
		result.SkippedTaskIDs = append([]string{}, result.SkippedTaskIDs...)
		copy.Result = &result
	}
	if sink.CompletedAt != nil {
		value := *sink.CompletedAt
		copy.CompletedAt = &value
	}
	return &copy
}

// BindRunSink creates or rebinds the implicit terminal task for a graph revision.
// Callers commit this result with the graph change. It never changes a final sink.
func BindRunSink(run WorkflowRun, tasks []Task) (WorkflowRun, error) {
	if run.ID == "" || run.WorkflowID == "" {
		return run, errors.New("sink requires run and workflow identity")
	}
	if run.GraphRevision == 0 {
		run.GraphRevision = 1
	}
	if run.GraphRevision < 1 {
		return run, errors.New("sink requires a positive graph revision")
	}
	needs := make([]string, 0, len(tasks))
	names := make(map[string]bool, len(tasks))
	for _, task := range tasks {
		if task.WorkflowID != run.WorkflowID || task.ID == "" || task.Name == "" || task.Name == SinkTaskName || task.ID == SinkTaskID(run.ID) {
			return run, fmt.Errorf("invalid or reserved sink predecessor %q", task.ID)
		}
		if names[task.Name] {
			return run, fmt.Errorf("duplicate sink predecessor name %q", task.Name)
		}
		names[task.Name] = true
		needs = append(needs, task.ID)
	}
	slices.Sort(needs)
	if len(slices.Compact(append([]string(nil), needs...))) != len(needs) {
		return run, errors.New("duplicate sink predecessor ID")
	}
	previous := run.Sink
	if previous != nil {
		if previous.ID != SinkTaskID(run.ID) || previous.Name != SinkTaskName || previous.GraphRevision < 1 {
			return run, errors.New("sink identity is inconsistent with its run")
		}
		if run.GraphRevision < previous.GraphRevision {
			return run, errors.New("sink graph revision moved backwards")
		}
		changed := !slices.Equal(needs, previous.Needs)
		if changed && run.GraphRevision == previous.GraphRevision {
			return run, errors.New("sink dependencies changed without a graph revision")
		}
		if previous.Progress.Terminal() {
			if run.GraphRevision != previous.GraphRevision || changed {
				return run, errors.New("completed sink is immutable; submit a new run")
			}
			run.Sink = CloneSink(previous)
			return run, nil
		}
	}
	run.Sink = &SinkTask{ID: SinkTaskID(run.ID), Name: SinkTaskName, GraphRevision: run.GraphRevision, Needs: needs, Progress: ProgressBlocked}
	return run, nil
}

// ProjectRunSink aggregates current task outcomes only after every execution in
// the run is proven quiescent. Missing assignment evidence is not containment.
// Failed earlier attempts do not poison a task that subsequently succeeded.
func ProjectRunSink(run WorkflowRun, tasks []Task, attempts []Attempt, assignments []Assignment, now time.Time) (WorkflowRun, error) {
	return ProjectSupervisedRunSink(run, tasks, attempts, assignments, SupervisionBarrier{}, now)
}

// ProjectSupervisedRunSink is ProjectRunSink with the supervision settlement
// barrier applied. A zero barrier is the unsupervised run and behaves exactly
// like ProjectRunSink, so an unsupervised projection is unchanged.
//
// The barrier is evaluated after the ordinary settlement conditions, never
// before them: quiescence and terminal predecessors still decide whether there
// is anything to settle, and supervision only decides whether a run that would
// otherwise settle may do so yet.
func ProjectSupervisedRunSink(run WorkflowRun, tasks []Task, attempts []Attempt, assignments []Assignment, barrier SupervisionBarrier, now time.Time) (WorkflowRun, error) {
	run, err := BindRunSink(run, tasks)
	if err != nil || run.Sink.Progress.Terminal() {
		return run, err
	}
	if !RunExecutionsQuiescent(run.ID, attempts, assignments) {
		return run, nil
	}
	current := make(map[string]Attempt, len(tasks))
	for _, attempt := range attempts {
		if attempt.WorkflowRunID != run.ID {
			continue
		}
		if prior, ok := current[attempt.TaskID]; !ok || prior.Number < attempt.Number {
			current[attempt.TaskID] = attempt
		}
	}
	result := &SinkResult{FailedTaskIDs: []string{}, CancelledTaskIDs: []string{}, SkippedTaskIDs: []string{}}
	allSucceeded := true
	for _, id := range run.Sink.Needs {
		attempt, ok := current[id]
		if !ok || !attempt.Progress.Terminal() {
			return run, nil
		}
		switch attempt.Progress {
		case ProgressFailed:
			result.FailedTaskIDs = append(result.FailedTaskIDs, id)
		case ProgressCancelled:
			result.CancelledTaskIDs = append(result.CancelledTaskIDs, id)
		case ProgressSkipped:
			result.SkippedTaskIDs = append(result.SkippedTaskIDs, id)
		}
		allSucceeded = allSucceeded && attempt.Progress == ProgressSucceeded
	}
	if verdict := SupervisionSettlementBarrier(barrier, len(result.FailedTaskIDs) > 0 || len(result.CancelledTaskIDs) > 0); !verdict.Settles {
		return run, nil
	}
	if now.IsZero() {
		return run, errors.New("sink settlement requires a timestamp")
	}
	completed := now.UTC()
	run.Sink.Progress = ProgressSucceeded
	if len(result.FailedTaskIDs) > 0 {
		run.Sink.Progress = ProgressFailed
	}
	run.Sink.Result = result
	run.Sink.CompletedAt = &completed
	switch {
	case len(result.FailedTaskIDs) > 0:
		run.Progress = ProgressFailed
	case len(result.CancelledTaskIDs) > 0:
		run.Progress = ProgressCancelled
	case allSucceeded:
		run.Progress = ProgressSucceeded
	default:
		run.Progress = ProgressSkipped
	}
	run.CompletedAt = &completed
	return run, nil
}
