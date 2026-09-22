package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ProjectionStore publishes one run against its complete run-local read set.
type ProjectionStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	CommitWorkflowProjection(context.Context, sqlite.WorkflowProjectionSnapshot, domain.WorkflowRun, []domain.Attempt, time.Time) error
}

// SupervisionReadSetSource is the optional fenced supervision read set of one
// run.
//
// It is separate from SupervisionProjectionSource because the two answer
// different questions. The projection source answers what supervision says about
// the run; this one answers exactly what the store will compare the publication
// against. A projection that fills one and not the other never commits a
// supervised run, because the fence compares a read set the caller never read.
type SupervisionReadSetSource interface {
	SupervisionReadSet(context.Context, string) (*sqlite.SupervisionReadSet, error)
}

// SupervisionProjectionSource is the optional supervision read set of one run.
// A ProjectionStore that does not implement it projects every run as
// unsupervised, which is exactly today's behaviour.
type SupervisionProjectionSource interface {
	SupervisionProjection(context.Context, string) (SupervisionProjection, error)
}

// SupervisionProjection is the run-local supervision state the projection
// reads: the readiness snapshot the shared predicate consumes, and the run's
// review incidents.
type SupervisionProjection struct {
	Snapshot         domain.SupervisionSnapshot
	Incidents        []domain.ReviewIncident
	DispatchFailures []domain.ActivationDispatchFailure
}

// RunGateView is one gate as status and explain render it.
type RunGateView struct {
	GateID string           `json:"gateId"`
	Name   string           `json:"name,omitempty"`
	State  domain.GateState `json:"state"`
	Final  bool             `json:"final,omitempty"`
}

// RunIncidentView is one unresolved review incident and what closing it needs.
type RunIncidentView struct {
	IncidentID          string                     `json:"incidentId"`
	GateID              string                     `json:"gateId,omitempty"`
	State               domain.IncidentState       `json:"state"`
	RequiredDisposition domain.IncidentDisposition `json:"requiredDisposition"`
	Reason              string                     `json:"reason,omitempty"`
}

// RunSupervisionView is the derived supervision explanation of one run: the
// status label, every gate state, the open incidents and the settlement
// barrier, if one is holding the run open. It is rendering, never scheduler
// state: nothing branches on it.
type RunSupervisionView struct {
	RunID         string                     `json:"runId"`
	Status        domain.SupervisedRunStatus `json:"status"`
	Gates         []RunGateView              `json:"gates,omitempty"`
	OpenIncidents []RunIncidentView          `json:"openIncidents,omitempty"`
	Barrier       domain.SinkBarrierVerdict  `json:"barrier"`
}

type ProjectionReport struct {
	Runs     []string
	Attempts []string
	// Supervision carries one view per supervised run projected in this pass,
	// in run order, for status and explain output.
	Supervision []RunSupervisionView
}

// ProjectWorkflowRuns advances dependencies and the coordinator-owned sink.
// It also backfills sink metadata on old imported runs. Quota planning is not a
// prerequisite: a stopped run can settle while an unrelated bucket is broken.
func ProjectWorkflowRuns(ctx context.Context, store ProjectionStore, now time.Time) (ProjectionReport, error) {
	var report ProjectionReport
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return report, fmt.Errorf("load records for projection: %w", err)
	}
	for _, run := range records.WorkflowRuns {
		if run.Sink != nil && run.Sink.Progress.Terminal() {
			continue
		}
		before := sqlite.WorkflowProjectionSnapshot{Run: run}
		if source, ok := store.(SupervisionReadSetSource); ok {
			read, err := source.SupervisionReadSet(ctx, run.ID)
			if err != nil {
				return report, fmt.Errorf("load supervision read set of run %q: %w", run.ID, err)
			}
			before.Supervision = read
		}
		owned := make(map[string]bool)
		before.Tasks = domain.TasksForRun(run, records.Tasks)
		// The projection is over the run's declared graph. An overseer
		// activation runs as assigned work but is not a node of that graph:
		// it names no declared task, so a DAG built with it would refuse to
		// validate, and a sink that waited for it would wait for the thing
		// that has to decide whether the run may settle.
		for _, attempt := range domain.DeclaredTaskAttempts(records.Attempts) {
			if attempt.WorkflowRunID == run.ID {
				before.Attempts = append(before.Attempts, attempt)
				owned[attempt.ID] = true
			}
		}
		for _, assignment := range records.Assignments {
			if owned[assignment.AttemptID] {
				before.Assignments = append(before.Assignments, assignment)
			}
		}
		external := ResolveExternalNodes(before.Tasks, records.WorkflowRuns, records.Tasks, records.Attempts, records.Assignments)
		for _, task := range before.Tasks {
			for _, ref := range task.ExternalNeeds {
				before.Dependencies = append(before.Dependencies, external[ref.String()])
			}
		}
		execution, err := NewDAGExecution(DAGState{Run: run, Tasks: before.Tasks, Attempts: before.Attempts, External: external})
		if err != nil {
			slog.Warn("workflow run projection skipped", "run", run.ID, "error", err)
			continue
		}
		state := execution.Snapshot()
		supervision, err := loadSupervisionProjection(ctx, store, run.ID)
		if err != nil {
			return report, fmt.Errorf("load supervision for run %q: %w", run.ID, err)
		}
		finalizeBlockedSinkPredecessors(&state, before.Assignments, supervision, now)
		projectedByID := make(map[string]domain.Attempt, len(state.Attempts))
		for _, attempt := range state.Attempts {
			projectedByID[attempt.ID] = attempt
		}
		updated := append([]domain.Attempt(nil), before.Attempts...)
		changedIDs := []string{}
		for index, current := range updated {
			projected := projectedByID[current.ID]
			if current.AssignmentID != "" || current.Progress.Terminal() ||
				(current.Progress == projected.Progress && current.Failure == projected.Failure) {
				continue
			}
			current.Progress = projected.Progress
			current.Failure = projected.Failure
			if current.Progress == domain.ProgressSkipped {
				current.Control = domain.ControlStopped
				completed := now.UTC()
				current.CompletedAt = &completed
			}
			current.Revision++
			current.UpdatedAt = now.UTC()
			updated[index] = current
			changedIDs = append(changedIDs, current.ID)
		}
		projected, err := domain.ProjectSupervisedRunSink(run, before.Tasks, updated, before.Assignments, supervisionBarrier(supervision), now)
		if err != nil {
			slog.Warn("workflow sink projection skipped", "run", run.ID, "error", err)
			continue
		}
		if supervision != nil {
			report.Supervision = append(report.Supervision, runSupervisionView(*supervision, state, updated, projected))
		}
		if !projected.Sink.Progress.Terminal() {
			projected.Progress = state.Run.Progress
			if projected.Progress.Terminal() {
				projected.Progress = domain.ProgressActive
			}
			projected.CompletedAt = nil
		}
		oldSink, _ := json.Marshal(run.Sink)
		newSink, _ := json.Marshal(projected.Sink)
		if len(changedIDs) == 0 && run.Progress == projected.Progress && run.GraphRevision == projected.GraphRevision &&
			string(oldSink) == string(newSink) && sameCompletionTime(run.CompletedAt, projected.CompletedAt) {
			continue
		}
		projected.Revision = run.Revision + 1
		projected.UpdatedAt = now.UTC()
		if err := store.CommitWorkflowProjection(ctx, before, projected, updated, now); err != nil {
			if errors.Is(err, sqlite.ErrStaleWorkflowProjection) {
				continue
			}
			return report, fmt.Errorf("persist workflow projection %q: %w", run.ID, err)
		}
		report.Runs = append(report.Runs, run.ID)
		report.Attempts = append(report.Attempts, changedIDs...)
	}
	return report, nil
}

// loadSupervisionProjection reads one run's supervision state when the store
// offers it and the run is actually supervised. It returns nil for every
// unsupervised run, and nil is the value the rest of this file treats as
// "behave exactly as before supervision existed".
func loadSupervisionProjection(ctx context.Context, store ProjectionStore, runID string) (*SupervisionProjection, error) {
	source, ok := store.(SupervisionProjectionSource)
	if !ok {
		return nil, nil
	}
	projection, err := source.SupervisionProjection(ctx, runID)
	if err != nil {
		return nil, err
	}
	if !projection.Snapshot.Supervised {
		return nil, nil
	}
	return &projection, nil
}

func supervisionBarrier(projection *SupervisionProjection) domain.SupervisionBarrier {
	if projection == nil {
		return domain.SupervisionBarrier{}
	}
	return domain.SupervisionBarrier{
		Supervised: true,
		Gates:      projection.Snapshot.Gates,
		Incidents:  projection.Incidents,
	}
}

// runSupervisionView derives the supervised run's status label and the gate and
// incident detail behind it. Task outcomes and their explanations are untouched
// by this: the label says which of several true things matters most, and waiting
// is never flattened into blocked.
func runSupervisionView(projection SupervisionProjection, state DAGState, attempts []domain.Attempt, projected domain.WorkflowRun) RunSupervisionView {
	var ready []string
	tasksRemaining := false
	for _, attempt := range state.Attempts {
		if attempt.Progress == domain.ProgressReady {
			ready = append(ready, attempt.TaskID)
		}
		if !attempt.Progress.Terminal() {
			tasksRemaining = true
		}
	}
	settled := projected.Sink != nil && projected.Sink.Progress.Terminal()
	view := RunSupervisionView{
		RunID: projection.Snapshot.RunID,
		Status: domain.SupervisedRunStatusLabel(domain.SupervisedRunStatusInput{
			Snapshot:       projection.Snapshot,
			Attempts:       attempts,
			ReadyTaskIDs:   ready,
			Incidents:      projection.Incidents,
			TasksRemaining: tasksRemaining,
			SinkSettled:    settled,
		}),
		Barrier: domain.SupervisionSettlementBarrier(
			supervisionBarrier(&projection),
			projected.Sink != nil && projected.Sink.Result != nil &&
				(len(projected.Sink.Result.FailedTaskIDs) > 0 || len(projected.Sink.Result.CancelledTaskIDs) > 0),
		),
	}
	for _, gate := range projection.Snapshot.Gates {
		view.Gates = append(view.Gates, RunGateView{
			GateID: gate.Definition.ID, Name: gate.Definition.Name,
			State: gate.State, Final: gate.Definition.Final,
		})
	}
	for _, incident := range projection.Incidents {
		if incident.State == domain.IncidentResolved {
			continue
		}
		view.OpenIncidents = append(view.OpenIncidents, RunIncidentView{
			IncidentID: incident.ID, GateID: incident.GateID, State: incident.State,
			RequiredDisposition: incident.RequiredDisposition, Reason: incident.Reason,
		})
	}
	return view
}

// supervisionDefersSkip reports whether supervision is withholding this task for
// a reason that a later decision can lift. Such a task is waiting, not
// impossible, so the sink projection must leave it blocked instead of skipping
// it. A cancelled gate, a cancelled run and a settled run are not deferrable:
// nothing lifts them, and the ordinary skip is correct there.
func supervisionDefersSkip(projection *SupervisionProjection, taskID string) bool {
	if projection == nil {
		return false
	}
	verdict := domain.SupervisionAdmits(projection.Snapshot, domain.SupervisionQuery{TaskID: taskID})
	for _, blocker := range verdict.Blockers {
		switch blocker.Code {
		case domain.SupervisionBlockerGatePending, domain.SupervisionBlockerGateAwaitingReview,
			domain.SupervisionBlockerGateHeld, domain.SupervisionBlockerGateEscalated,
			domain.SupervisionBlockerBranchHold, domain.SupervisionBlockerRunHold,
			domain.SupervisionBlockerRouteUnavailable:
			return true
		}
	}
	return false
}

func sameCompletionTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

// If no branch can still make progress, exhausted non-success dependencies
// terminalize their blocked descendants. While another branch is active/ready,
// keep blocked descendants intact so an accepted retry can still unblock them.
// The final skip and sink publication share the same optimistic transaction.
func finalizeBlockedSinkPredecessors(state *DAGState, assignments []domain.Assignment, supervision *SupervisionProjection, now time.Time) {
	if !domain.RunExecutionsQuiescent(state.Run.ID, state.Attempts, assignments) {
		return
	}
	latest := make(map[string]int)
	for index, attempt := range state.Attempts {
		if prior, ok := latest[attempt.TaskID]; !ok || state.Attempts[prior].Number < attempt.Number {
			latest[attempt.TaskID] = index
		}
	}
	for _, index := range latest {
		attempt := state.Attempts[index]
		if !attempt.Progress.Terminal() && attempt.Progress != domain.ProgressBlocked {
			return
		}
	}
	byName := make(map[string]domain.Task)
	for _, task := range state.Tasks {
		byName[task.Name] = task
	}
	for changed := true; changed; {
		changed = false
		for _, task := range state.Tasks {
			index, ok := latest[task.ID]
			if !ok {
				continue
			}
			attempt := &state.Attempts[index]
			if attempt.Progress != domain.ProgressBlocked || attempt.AssignmentID != "" {
				continue
			}
			// A task a gate or a hold is withholding is waiting for a
			// decision, not impossible. Skipping it here would silently
			// terminalize the exact tasks supervision protects.
			if supervisionDefersSkip(supervision, task.ID) {
				continue
			}
			var failed []string
			for _, name := range task.Needs {
				dependency, ok := byName[name]
				if !ok {
					continue
				}
				dependencyIndex, ok := latest[dependency.ID]
				if !ok {
					continue
				}
				progress := state.Attempts[dependencyIndex].Progress
				if progress.Terminal() && progress != domain.ProgressSucceeded {
					failed = append(failed, name)
				}
			}
			for _, ref := range task.ExternalNeeds {
				if obs, ok := state.External[ref.String()]; ok && obs.ExitCode == 2 {
					failed = append(failed, ref.String())
				}
			}
			if len(failed) == 0 {
				continue
			}
			slices.Sort(failed)
			attempt.Progress = domain.ProgressSkipped
			attempt.Control = domain.ControlStopped
			attempt.Failure = fmt.Sprintf("terminal dependency prevented execution: %v", failed)
			attempt.CompletedAt = timePointer(now.UTC())
			changed = true
		}
	}
}
