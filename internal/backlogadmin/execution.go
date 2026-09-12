package backlogadmin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var ErrCommandExecutionUnavailable = errors.New("backlog admin command execution is unavailable")

type CommandExecutorStore interface {
	Reader
	ApplyAdminCommand(context.Context, domain.AdminCommandApplication) (domain.AdminCommandDecision, error)
}

type CommandExecutionReport struct {
	Decisions []MutationResponse
}

// ExecutePendingCommands executes pending commands in deterministic submission order.
func (s *Service) ExecutePendingCommands(ctx context.Context) (CommandExecutionReport, error) {
	store, ok := s.reader.(CommandExecutorStore)
	if !ok {
		return CommandExecutionReport{}, ErrCommandExecutionUnavailable
	}
	report := CommandExecutionReport{}
	for {
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			return report, fmt.Errorf("load command execution snapshot: %w", err)
		}
		workers, err := store.LoadWorkerSnapshots(ctx)
		if err != nil {
			return report, fmt.Errorf("load command worker snapshot: %w", err)
		}
		admissions, err := store.LoadQuotaAdmissions(ctx)
		if err != nil {
			return report, fmt.Errorf("load command quota snapshot: %w", err)
		}
		pending := pendingAdminCommands(records.AdminCommands)
		if len(pending) == 0 {
			return report, nil
		}
		command := pending[0]
		application, trigger, err := planAdminCommand(records, workers, admissions, command, s.now().UTC())
		if err != nil {
			return report, fmt.Errorf("plan admin command %q: %w", command.ID, err)
		}
		application.ScheduleTrigger = trigger
		decision, err := store.ApplyAdminCommand(ctx, application)
		if errors.Is(err, sqlite.ErrStaleAdminSafetyFence) {
			continue
		}
		if err != nil {
			return report, fmt.Errorf("apply admin command %q: %w", command.ID, err)
		}
		report.Decisions = append(report.Decisions, mutationResponse(decision))
	}
}

func pendingAdminCommands(commands []domain.AdminCommand) []domain.AdminCommand {
	var pending []domain.AdminCommand
	for _, command := range commands {
		if command.State == domain.AdminCommandPending {
			pending = append(pending, command)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].CreatedAt.Equal(pending[j].CreatedAt) {
			return pending[i].ID < pending[j].ID
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})
	return pending
}

func planAdminCommand(records sqlite.CoordinatorRecords, workers []domain.WorkerSnapshot, admissions []domain.QuotaAdmissionRecord, command domain.AdminCommand, now time.Time) (domain.AdminCommandApplication, *domain.ScheduleTriggerRequest, error) {
	application := domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending,
		ExpectedTargetRevision: command.ExpectedRevision, State: domain.AdminCommandApplied, AppliedAt: now,
	}
	if command.State != domain.AdminCommandPending {
		return application, nil, fmt.Errorf("command state is %q, want pending", command.State)
	}
	switch command.TargetType {
	case domain.AdminTargetAttempt:
		attempt, task, run, err := commandAttemptContext(records, command.TargetID)
		if err != nil {
			return rejectApplication(application, err.Error()), nil, nil
		}
		switch command.Kind {
		case domain.AdminCommandCancel:
			next, related, nextRun, cancelErr := planDAGCancellation(records, attempt, task, run, now)
			if cancelErr != nil {
				return rejectApplication(application, cancelErr.Error()), nil, nil
			}
			application.Attempt, application.RelatedAttempts, application.WorkflowRun = next, related, nextRun
		case domain.AdminCommandSkip:
			next, nextRun, skipErr := planDAGSkip(records, attempt, task, run, now)
			if skipErr != nil {
				return rejectApplication(application, skipErr.Error()), nil, nil
			}
			application.Attempt, application.WorkflowRun = next, nextRun
		default:
			next, newAttempt, nextRun, planErr := planAttemptCommand(records, workers, admissions, command, attempt, task, run, now)
			if planErr != nil {
				return rejectApplication(application, planErr.Error()), nil, nil
			}
			application.Attempt, application.NewAttempt, application.WorkflowRun = next, newAttempt, nextRun
		}
		if command.Kind == domain.AdminCommandPause {
			hard, pauseErr := commandPauseNow(command.Payload)
			if pauseErr != nil {
				return rejectApplication(application, pauseErr.Error()), nil, nil
			}
			binding, bindingErr := adminPauseBinding(attempt, records.Assignments, workers)
			if bindingErr != nil {
				return rejectApplication(application, bindingErr.Error()), nil, nil
			}
			intent, intentErr := backlog.PlanAdminPauseDelivery(
				stableAdminID("pause-directive", command.ID), command.Reason, hard, binding, now,
			)
			if intentErr != nil {
				return rejectApplication(application, intentErr.Error()), nil, nil
			}
			application.PauseIntent = &intent
		}
		if command.Kind == domain.AdminCommandStart || command.Kind == domain.AdminCommandResume || command.Kind == domain.AdminCommandPause {
			fingerprint, fingerprintErr := domain.AdminSafetyFingerprint(domain.AdminSafetyState{
				Tasks: records.Tasks, Attempts: records.Attempts, Assignments: records.Assignments,
				QuotaPools: records.QuotaPools, Workers: workers, QuotaAdmissions: admissions,
			})
			if fingerprintErr != nil {
				return application, nil, fmt.Errorf("fingerprint admin safety state: %w", fingerprintErr)
			}
			application.SafetyFingerprint = fingerprint
			if command.Kind == domain.AdminCommandStart || command.Kind == domain.AdminCommandResume {
				validUntil, ok := domain.AdminWorkerSafetyValidUntil(workers, now)
				if !ok {
					return application, nil, errors.New("applied start/resume has no fresh ready worker validity")
				}
				application.SafetyValidUntil = &validUntil
			}
		}
	case domain.AdminTargetSchedule:
		schedule, err := commandSchedule(records, command.TargetID)
		if err != nil {
			return rejectApplication(application, err.Error()), nil, nil
		}
		if command.Kind == domain.AdminCommandScheduleRun {
			if err := manualScheduleRunPolicy(records, schedule); err != nil {
				return rejectApplication(application, err.Error()), nil, nil
			}
		}
		next, trigger, err := planScheduleCommand(command, schedule, now)
		if err != nil {
			return rejectApplication(application, err.Error()), nil, nil
		}
		application.Schedule = next
		application.ScheduleTrigger = trigger
		return application, trigger, nil
	default:
		return rejectApplication(application, "unsupported admin target"), nil, nil
	}
	return application, nil, nil
}

func planAttemptCommand(records sqlite.CoordinatorRecords, workers []domain.WorkerSnapshot, admissions []domain.QuotaAdmissionRecord, command domain.AdminCommand, attempt domain.Attempt, task domain.Task, run domain.WorkflowRun, now time.Time) (*domain.Attempt, *domain.Attempt, *domain.WorkflowRun, error) {
	if run.Sink != nil && run.Sink.Progress.Terminal() {
		return nil, nil, nil, errors.New("run sink is final; submit a new workflow")
	}
	next := attempt
	next.Revision++
	next.UpdatedAt = now
	switch command.Kind {
	case domain.AdminCommandStart:
		if attempt.Progress.Terminal() || attempt.Control != domain.ControlUnassigned {
			return nil, nil, nil, fmt.Errorf("start is invalid from %s/%s", attempt.Progress, attempt.Control)
		}
		if blocker := commandSafetyBlocker(records, workers, admissions, attempt, task, now, false); blocker != "" {
			return nil, nil, nil, errors.New(blocker)
		}
		next.Progress = domain.ProgressReady
		next.Failure = ""
		next.AdminNotBefore = nil
		next.AdminForceStart = true
	case domain.AdminCommandDelay:
		if attempt.Progress.Terminal() || attempt.Control != domain.ControlUnassigned {
			return nil, nil, nil, fmt.Errorf("delay is invalid from %s/%s", attempt.Progress, attempt.Control)
		}
		until, err := commandUntil(command.Payload, now)
		if err != nil {
			return nil, nil, nil, err
		}
		next.AdminNotBefore = &until
		next.AdminForceStart = false
	case domain.AdminCommandPause:
		if attempt.Progress.Terminal() || (attempt.Control != domain.ControlPreparing && attempt.Control != domain.ControlRunning && attempt.Control != domain.ControlResuming) {
			return nil, nil, nil, fmt.Errorf("pause is invalid from %s/%s", attempt.Progress, attempt.Control)
		}
		if _, err := commandPauseNow(command.Payload); err != nil {
			return nil, nil, nil, err
		}
		next.Control = domain.ControlDraining
	case domain.AdminCommandResume:
		if attempt.Progress.Terminal() || (attempt.Control != domain.ControlPaused && attempt.Control != domain.ControlPausedUncheckpointed) {
			return nil, nil, nil, fmt.Errorf("resume is invalid from %s/%s", attempt.Progress, attempt.Control)
		}
		if blocker := commandSafetyBlocker(records, workers, admissions, attempt, task, now, true); blocker != "" {
			return nil, nil, nil, errors.New(blocker)
		}
		next.Control = domain.ControlResuming
	case domain.AdminCommandCancel:
		if attempt.Progress.Terminal() {
			return nil, nil, nil, fmt.Errorf("cancel is invalid from terminal progress %s", attempt.Progress)
		}
		next.Progress, next.Control = domain.ProgressCancelled, domain.ControlStopped
		next.Failure, next.CompletedAt, next.AdminForceStart = "", timePtr(now), false
	case domain.AdminCommandSkip:
		if attempt.Progress == domain.ProgressSucceeded || attempt.Progress == domain.ProgressSkipped {
			return nil, nil, nil, fmt.Errorf("skip is invalid from progress %s", attempt.Progress)
		}
		next.Progress, next.Control = domain.ProgressSkipped, domain.ControlStopped
		next.Failure, next.CompletedAt, next.AdminForceStart = "", timePtr(now), false
	case domain.AdminCommandRetry:
		if attempt.Progress != domain.ProgressFailed && attempt.Progress != domain.ProgressCancelled {
			return nil, nil, nil, fmt.Errorf("retry is invalid from progress %s", attempt.Progress)
		}
		retry := domain.Attempt{
			ID: stableAdminID("attempt", command.ID), WorkflowRunID: attempt.WorkflowRunID,
			TaskID: attempt.TaskID, Number: attempt.Number + 1, Progress: retryProgress(records, task, run.ID),
			Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now,
		}
		nextRun := run
		nextRun.Progress, nextRun.Revision, nextRun.UpdatedAt, nextRun.CompletedAt = domain.ProgressQueued, run.Revision+1, now, nil
		return &next, &retry, &nextRun, nil
	default:
		return nil, nil, nil, fmt.Errorf("command %q is invalid for an attempt", command.Kind)
	}
	return &next, nil, nil, nil
}

func planDAGCancellation(records sqlite.CoordinatorRecords, attempt domain.Attempt, task domain.Task, run domain.WorkflowRun, now time.Time) (*domain.Attempt, []domain.Attempt, *domain.WorkflowRun, error) {
	if attempt.Progress.Terminal() {
		return nil, nil, nil, fmt.Errorf("cancel is invalid from terminal progress %s", attempt.Progress)
	}
	state := backlog.DAGState{Run: run}
	for _, candidate := range records.Tasks {
		if candidate.WorkflowID == task.WorkflowID {
			state.Tasks = append(state.Tasks, candidate)
		}
	}
	original := make(map[string]domain.Attempt)
	for _, candidate := range records.Attempts {
		if candidate.WorkflowRunID == run.ID {
			state.Attempts = append(state.Attempts, candidate)
			original[candidate.ID] = candidate
		}
	}
	execution, err := backlog.NewDAGExecution(state)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build cancellation DAG: %w", err)
	}
	if err := execution.CancelTask(task.ID, now); err != nil {
		return nil, nil, nil, err
	}
	snapshot := execution.Snapshot()
	var target *domain.Attempt
	var related []domain.Attempt
	for _, candidate := range snapshot.Attempts {
		previous, found := original[candidate.ID]
		if !found || previous.Progress.Terminal() || candidate.Progress != domain.ProgressCancelled {
			continue
		}
		candidate.Revision = previous.Revision + 1
		candidate.AdminForceStart = false
		candidate.AdminNotBefore = nil
		if candidate.ID == attempt.ID {
			item := candidate
			target = &item
		} else {
			related = append(related, candidate)
		}
	}
	if target == nil {
		return nil, nil, nil, errors.New("cancellation DAG did not update the target attempt")
	}
	sort.Slice(related, func(i, j int) bool { return related[i].ID < related[j].ID })
	nextRun := snapshot.Run
	return target, related, &nextRun, nil
}

func planDAGSkip(records sqlite.CoordinatorRecords, attempt domain.Attempt, task domain.Task, run domain.WorkflowRun, now time.Time) (*domain.Attempt, *domain.WorkflowRun, error) {
	state := backlog.DAGState{Run: run}
	for _, candidate := range records.Tasks {
		if candidate.WorkflowID == task.WorkflowID {
			state.Tasks = append(state.Tasks, candidate)
		}
	}
	for _, candidate := range records.Attempts {
		if candidate.WorkflowRunID == run.ID {
			state.Attempts = append(state.Attempts, candidate)
		}
	}
	execution, err := backlog.NewDAGExecution(state)
	if err != nil {
		return nil, nil, fmt.Errorf("build skip DAG: %w", err)
	}
	if err := execution.SkipTask(task.ID, now); err != nil {
		return nil, nil, err
	}
	snapshot := execution.Snapshot()
	for _, candidate := range snapshot.Attempts {
		if candidate.ID == attempt.ID {
			candidate.Revision = attempt.Revision + 1
			candidate.AdminForceStart = false
			candidate.AdminNotBefore = nil
			nextRun := snapshot.Run
			return &candidate, &nextRun, nil
		}
	}
	return nil, nil, errors.New("skip DAG did not update the target attempt")
}

func manualScheduleRunPolicy(records sqlite.CoordinatorRecords, schedule domain.Schedule) error {
	if schedule.ActiveRunID == "" {
		return nil
	}
	for _, run := range records.WorkflowRuns {
		if run.ID != schedule.ActiveRunID {
			continue
		}
		if !run.Progress.Terminal() {
			return errors.New("manual schedule run refused while another run is open")
		}
		if run.Progress == domain.ProgressFailed && schedule.AfterFailure == domain.ScheduleFailureHold {
			return errors.New("manual schedule run refused while failure hold is active")
		}
		return nil
	}
	return errors.New("manual schedule run refused because active run state is unavailable")
}

func planScheduleCommand(command domain.AdminCommand, schedule domain.Schedule, now time.Time) (*domain.Schedule, *domain.ScheduleTriggerRequest, error) {
	next := schedule
	next.Revision++
	next.UpdatedAt = now
	switch command.Kind {
	case domain.AdminCommandEnable:
		if schedule.Enabled {
			return nil, nil, errors.New("schedule is already enabled")
		}
		next.Enabled = true
	case domain.AdminCommandDisable:
		if !schedule.Enabled {
			return nil, nil, errors.New("schedule is already disabled")
		}
		next.Enabled = false
	case domain.AdminCommandDelayNext:
		until, err := commandUntil(command.Payload, now)
		if err != nil {
			return nil, nil, err
		}
		next.NextNotBefore = &until
	case domain.AdminCommandScheduleRun:
		return nil, &domain.ScheduleTriggerRequest{
			ScheduleID: schedule.ID, TriggerID: stableAdminID("trigger", command.ID),
			WorkflowRunID: stableAdminID("run", command.ID), NominalAt: command.CreatedAt.UTC(),
			ObservedAt: now, Source: domain.ScheduleTriggerManual,
		}, nil
	default:
		return nil, nil, fmt.Errorf("command %q is invalid for a schedule", command.Kind)
	}
	return &next, nil, nil
}

func commandAttemptContext(records sqlite.CoordinatorRecords, id string) (domain.Attempt, domain.Task, domain.WorkflowRun, error) {
	var attempt domain.Attempt
	found := false
	for _, item := range records.Attempts {
		if item.ID == id {
			attempt, found = item, true
			break
		}
	}
	if !found {
		return attempt, domain.Task{}, domain.WorkflowRun{}, fmt.Errorf("attempt %q not found", id)
	}
	var task domain.Task
	found = false
	for _, item := range records.Tasks {
		if item.ID == attempt.TaskID {
			task, found = item, true
			break
		}
	}
	if !found {
		return attempt, task, domain.WorkflowRun{}, fmt.Errorf("task %q not found", attempt.TaskID)
	}
	for _, item := range records.WorkflowRuns {
		if item.ID == attempt.WorkflowRunID {
			return attempt, task, item, nil
		}
	}
	return attempt, task, domain.WorkflowRun{}, fmt.Errorf("workflow run %q not found", attempt.WorkflowRunID)
}

func commandSchedule(records sqlite.CoordinatorRecords, id string) (domain.Schedule, error) {
	for _, schedule := range records.Schedules {
		if schedule.ID == id {
			return schedule, nil
		}
	}
	return domain.Schedule{}, fmt.Errorf("schedule %q not found", id)
}

func adminPauseBinding(
	attempt domain.Attempt,
	assignments []domain.Assignment,
	workers []domain.WorkerSnapshot,
) (backlog.ThrottleAttemptBinding, error) {
	assignment, found := assignmentByID(assignments, attempt.AssignmentID)
	if !found || (assignment.State != domain.AssignmentClaimed && assignment.State != domain.AssignmentUnknown) {
		return backlog.ThrottleAttemptBinding{}, fmt.Errorf("pause requires a claimed or unknown assignment")
	}
	if assignment.ThreadID == "" || assignment.Route.QuotaPoolID == "" {
		return backlog.ThrottleAttemptBinding{}, fmt.Errorf("pause assignment execution identity is incomplete")
	}
	for _, worker := range workers {
		if worker.WorkerID != assignment.WorkerID || worker.WorkerEpoch != assignment.WorkerEpoch {
			continue
		}
		for _, observation := range worker.Assignments {
			if observation.AssignmentID != assignment.ID || observation.AssignmentEpoch != assignment.Epoch {
				continue
			}
			if observation.ThreadID != assignment.ThreadID || observation.WorkspacePath == "" {
				return backlog.ThrottleAttemptBinding{}, fmt.Errorf("pause worker observation execution identity is incomplete or changed")
			}
			return backlog.ThrottleAttemptBinding{
				Attempt: attempt, Assignment: assignment, WorkspacePath: observation.WorkspacePath,
			}, nil
		}
	}
	return backlog.ThrottleAttemptBinding{}, fmt.Errorf("pause has no matching worker assignment observation")
}

func commandSafetyBlocker(records sqlite.CoordinatorRecords, workers []domain.WorkerSnapshot, admissions []domain.QuotaAdmissionRecord, attempt domain.Attempt, task domain.Task, now time.Time, resume bool) string {
	for _, dependency := range task.Needs {
		succeeded := false
		for _, dependencyTask := range records.Tasks {
			if dependencyTask.WorkflowID != task.WorkflowID || dependencyTask.Name != dependency {
				continue
			}
			for _, candidate := range records.Attempts {
				if candidate.WorkflowRunID == attempt.WorkflowRunID && candidate.TaskID == dependencyTask.ID && candidate.Progress == domain.ProgressSucceeded {
					succeeded = true
				}
			}
		}
		if !succeeded {
			return "dependency has not succeeded: " + dependency
		}
	}
	for _, other := range records.Attempts {
		if other.ID == attempt.ID || other.Progress.Terminal() || other.AssignmentID == "" {
			continue
		}
		assignment, found := assignmentByID(records.Assignments, other.AssignmentID)
		if !found || assignment.State == domain.AssignmentReleased || assignment.State == domain.AssignmentCompleted {
			continue
		}
		var otherTask domain.Task
		for _, candidate := range records.Tasks {
			if candidate.ID == other.TaskID {
				otherTask = candidate
				break
			}
		}
		for _, lock := range task.ResourceLocks {
			if stringContains(otherTask.ResourceLocks, lock) {
				return "resource lock is held by " + other.ID + ": " + lock
			}
		}
	}
	healthyWorker := func(worker domain.WorkerSnapshot, requiredWorker string) bool {
		if requiredWorker != "" && worker.WorkerID != requiredWorker {
			return false
		}
		if len(task.Placement.Hosts) != 0 && !stringContains(task.Placement.Hosts, worker.WorkerID) {
			return false
		}
		return worker.Connected && !now.After(worker.ValidUntil) && worker.Inventory.AcceptBacklog &&
			worker.Inventory.Health == domain.WorkerHealthReady &&
			capabilitiesInclude(worker.Inventory.Capabilities, task.Placement.Capabilities)
	}
	hasHealthyWorker := func(requiredWorker string) bool {
		for _, worker := range workers {
			if healthyWorker(worker, requiredWorker) {
				return true
			}
		}
		return false
	}
	quotaAdmitted := func(poolID string) (bool, string) {
		if poolID == "" {
			return false, "unavailable"
		}
		state, found := admissionFor(records, admissions, poolID)
		if !found {
			return false, "unavailable"
		}
		if state == domain.AdmissionClosed || state == domain.AdmissionDraining {
			return false, string(state)
		}
		return true, ""
	}

	if resume {
		assignment, found := assignmentByID(records.Assignments, attempt.AssignmentID)
		if !found || !hasHealthyWorker(assignment.WorkerID) {
			return "assigned worker is not fresh, ready, or placement-compatible"
		}
		if admitted, reason := quotaAdmitted(assignment.Route.QuotaPoolID); !admitted {
			return "assigned quota route is not admitted: " + assignment.Route.QuotaPoolID + "=" + reason
		}
		return ""
	}
	if len(task.Routes) == 0 {
		if !hasHealthyWorker("") {
			return "no fresh ready worker satisfies placement"
		}
		return ""
	}

	eligible := make([]domain.WorkerInventory, 0, len(workers))
	for _, worker := range workers {
		if !healthyWorker(worker, "") {
			continue
		}
		inventory := worker.Inventory
		inventory.ID = worker.WorkerID
		eligible = append(eligible, inventory)
	}
	resolved, err := backlog.ResolveProviderRoutePools(task, attempt, eligible, records.QuotaPools)
	if err != nil {
		return "provider route resolution failed: " + err.Error()
	}
	if len(resolved) == 0 {
		return "no fresh ready worker has a provider-compatible route"
	}
	if !resume {
		return ""
	}
	var blocked []string
	for _, route := range resolved {
		if admitted, reason := quotaAdmitted(route.QuotaPoolID); admitted {
			return ""
		} else {
			blocked = append(blocked, route.WorkerID+"/"+route.QuotaPoolID+"="+reason)
		}
	}
	return "no healthy worker and quota route is admitted: " + strings.Join(blocked, ", ")
}

func assignmentByID(assignments []domain.Assignment, id string) (domain.Assignment, bool) {
	for _, assignment := range assignments {
		if assignment.ID == id {
			return assignment, true
		}
	}
	return domain.Assignment{}, false
}

func admissionFor(records sqlite.CoordinatorRecords, admissions []domain.QuotaAdmissionRecord, poolID string) (domain.AdmissionState, bool) {
	for _, admission := range admissions {
		if admission.QuotaPoolID == poolID {
			return admission.Admission, true
		}
	}
	for _, pool := range records.QuotaPools {
		if pool.ID == poolID {
			return pool.Admission, true
		}
	}
	return "", false
}

func retryProgress(records sqlite.CoordinatorRecords, task domain.Task, runID string) domain.ProgressState {
	for _, need := range task.Needs {
		found := false
		for _, dependency := range records.Tasks {
			if dependency.WorkflowID != task.WorkflowID || dependency.Name != need {
				continue
			}
			for _, attempt := range records.Attempts {
				if attempt.WorkflowRunID == runID && attempt.TaskID == dependency.ID && attempt.Progress == domain.ProgressSucceeded {
					found = true
				}
			}
		}
		if !found {
			return domain.ProgressBlocked
		}
	}
	return domain.ProgressReady
}

func commandUntil(raw json.RawMessage, now time.Time) (time.Time, error) {
	var payload struct{ Until string }
	if err := json.Unmarshal(raw, &payload); err != nil || payload.Until == "" {
		return time.Time{}, errors.New("command requires an RFC3339 until payload")
	}
	until, err := time.Parse(time.RFC3339, payload.Until)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse command until: %w", err)
	}
	until = until.UTC()
	if !until.After(now) {
		return time.Time{}, errors.New("command until must be in the future")
	}
	return until, nil
}

func commandPauseNow(raw json.RawMessage) (bool, error) {
	var payload struct{ Now *bool }
	if err := json.Unmarshal(raw, &payload); err != nil || payload.Now == nil {
		return false, errors.New("pause command requires an explicit now payload")
	}
	return *payload.Now, nil
}

func rejectApplication(application domain.AdminCommandApplication, failure string) domain.AdminCommandApplication {
	application.State, application.Failure = domain.AdminCommandRejected, failure
	return application
}

func stableAdminID(kind, commandID string) string {
	digest := sha256.Sum256([]byte(kind + "\x00" + commandID))
	return "admin-" + kind + "-" + hex.EncodeToString(digest[:12])
}

func stringContains(items []string, wanted string) bool {
	for _, item := range items {
		if item == wanted {
			return true
		}
	}
	return false
}

func timePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
