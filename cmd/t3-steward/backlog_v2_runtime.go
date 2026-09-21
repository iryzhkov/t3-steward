package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func runBacklogV2(ctx context.Context, cfg config.Config, logger *slog.Logger) (bool, error) {
	switch cfg.BacklogV2.Mode {
	case "disabled", "worker":
		return false, nil
	case "coordinator":
		if cfg.Backlog.Enabled {
			return true, errors.New("backlog-v2 coordinator and legacy backlog runner are mutually exclusive")
		}
		return true, runBacklogV2Coordinator(ctx, cfg, logger)
	default:
		return true, fmt.Errorf("unsupported backlog-v2 mode %q", cfg.BacklogV2.Mode)
	}
}

func resolveBacklogV2AdminSocketPath(cfg config.Config) (string, error) {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return "", err
	}
	if statePath == ":memory:" {
		return "", errors.New("backlog-v2 coordinator requires a file-backed state path")
	}
	path, err := filepath.Abs(statePath + ".admin.sock")
	if err != nil {
		return "", fmt.Errorf("resolve backlog-v2 admin socket: %w", err)
	}
	return path, nil
}

// runBacklogV2Coordinator establishes authority with admission closed. Its
// startup pass is local-only; configured worker exchange begins on later ticks.
type coordinatorLocalService struct {
	admin       *backlogadmin.Service
	submissions *backlog.SubmissionService
	schedules   *backlog.ScheduleDefinitionService
}

func (s coordinatorLocalService) EnrollWorker(ctx context.Context, p backlogadmin.Principal, r domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error) {
	return s.admin.EnrollWorker(ctx, p, r)
}

func (s coordinatorLocalService) AmendGraph(ctx context.Context, p backlogadmin.Principal, r domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
	return s.admin.AmendGraph(ctx, p, r)
}

func (s coordinatorLocalService) NodeWait(ctx context.Context, p backlogadmin.Principal, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	return s.admin.NodeWait(ctx, p, op)
}

func (s coordinatorLocalService) Query(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	return s.admin.Query(ctx, query)
}

// Supervise answers one supervision operation.
//
// It is forwarded explicitly, like every other capability above, because the
// local transport reaches supervision through an optional interface on this
// type. Omitting it did not fail to compile: it made every supervision request
// on both carriers answer "malformed supervision request", because the type
// assertion that looks for this method was the same branch that reports a
// request whose shape is wrong.
func (s coordinatorLocalService) Supervise(
	ctx context.Context,
	principal backlogadmin.Principal,
	request backlogadmin.SupervisionRequest,
) (backlogadmin.SupervisionResponse, error) {
	return s.admin.Supervise(ctx, principal, request)
}

func (s coordinatorLocalService) Mutate(ctx context.Context, mutation backlogadmin.Mutation) (backlogadmin.MutationResponse, error) {
	return s.admin.Mutate(ctx, mutation)
}

func (s coordinatorLocalService) RecoverUnknown(
	ctx context.Context,
	principal backlogadmin.Principal,
	request backlogadmin.UnknownRecoveryRequest,
) (domain.UnknownAssignmentRecoveryDecision, error) {
	return s.admin.RecoverUnknown(ctx, principal, request)
}

func (s coordinatorLocalService) ReleaseQuarantine(
	ctx context.Context,
	principal backlogadmin.Principal,
	request backlogadmin.QuarantineReleaseRequest,
) (domain.QuarantineRelease, error) {
	return s.admin.ReleaseQuarantine(ctx, principal, request)
}

func (s coordinatorLocalService) PutSchedule(
	ctx context.Context,
	principal backlogadmin.Principal,
	request backlogadmin.LocalScheduleDefinitionRequest,
) (backlogadmin.LocalScheduleDefinitionResponse, error) {
	if s.schedules == nil {
		return backlogadmin.LocalScheduleDefinitionResponse{}, errors.New("schedule definition service is unavailable")
	}
	result, err := s.schedules.Put(ctx, backlog.ScheduleDefinitionRequest{
		RequestID: request.RequestID, ID: request.ID, Name: request.Name,
		WorkflowID: request.WorkflowID, Expression: request.Expression, Timezone: request.Timezone,
		AfterFailure: request.AfterFailure, Enabled: request.Enabled,
		ExpectedRevision: request.ExpectedRevision, Actor: principal.ID, Reason: request.Reason,
	})
	if err != nil {
		return backlogadmin.LocalScheduleDefinitionResponse{}, err
	}
	return backlogadmin.LocalScheduleDefinitionResponse{Schedule: result.Schedule, Replay: result.Replay}, nil
}

func (s coordinatorLocalService) OpenArtifact(
	ctx context.Context,
	principal backlogadmin.Principal,
	artifactID string,
) (backlogadmin.ArtifactContent, error) {
	return s.admin.OpenArtifact(ctx, principal, artifactID)
}

func (s coordinatorLocalService) SubmitArchive(
	ctx context.Context,
	principal backlogadmin.Principal,
	request backlogadmin.LocalSubmissionRequest,
	archive io.Reader,
) (backlogadmin.LocalSubmissionResponse, error) {
	result, err := s.submissions.SubmitArchive(ctx, backlog.ArchiveSubmission{
		IdempotencyKey: request.IdempotencyKey,
		Archive:        archive,
		// The principal is the one this carrier authenticated, never the one
		// the request claimed.
		Principal:        principal.ID,
		Unverified:       request.Unverified,
		UnverifiedReason: request.UnverifiedReason,
	})
	if err != nil {
		return backlogadmin.LocalSubmissionResponse{}, err
	}
	if result.Record.AcceptedAt == nil {
		return backlogadmin.LocalSubmissionResponse{}, errors.New("submission completed without an acceptance time")
	}
	return backlogadmin.LocalSubmissionResponse{
		Key: result.Record.Key, Digest: result.Record.Digest,
		WorkflowID: result.Record.WorkflowID, RunID: result.Record.RunID,
		State: string(result.Record.State), AcceptedAt: result.Record.AcceptedAt.Format(time.RFC3339Nano),
		Replay: result.Replay,
	}, nil
}

type coordinatorQuotaTicker interface {
	Tick(context.Context) (backlog.QuotaBridgeReport, error)
}

type coordinatorQuotaReconciler struct {
	store  *sqlite.Store
	bridge backlog.QuotaBridge
}

func (r coordinatorQuotaReconciler) Tick(ctx context.Context) (backlog.QuotaBridgeReport, error) {
	records, err := r.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return backlog.QuotaBridgeReport{}, fmt.Errorf("load quota coordinator snapshot: %w", err)
	}
	var throttleRecords []domain.ThrottleAttemptRecord
	if !r.bridge.Disabled {
		throttleRecords, err = r.store.LoadThrottleAttemptRecords(ctx)
	}
	if err != nil {
		return backlog.QuotaBridgeReport{}, fmt.Errorf("load quota throttle snapshot: %w", err)
	}
	workers, err := r.store.LoadWorkerSnapshots(ctx)
	if err != nil {
		return backlog.QuotaBridgeReport{}, fmt.Errorf("load quota worker snapshots: %w", err)
	}
	report, err := r.bridge.ReconcileState(ctx, backlog.QuotaPlanningStateInput{
		Tasks: records.Tasks, Attempts: records.Attempts,
		Assignments: records.Assignments, ThrottleRecords: throttleRecords,
		WorkerSnapshots: workers,
	})
	if err != nil {
		return backlog.QuotaBridgeReport{}, err
	}
	if err := r.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{QuotaPools: report.Pools}); err != nil {
		return backlog.QuotaBridgeReport{}, fmt.Errorf("persist quota pool projection: %w", err)
	}
	return report, nil
}

type coordinatorPlanningTicker interface {
	Tick(context.Context, backlog.QuotaBridgeReport) (backlog.AssignmentPlanningReport, error)
}

type coordinatorPlanner struct {
	settings               *config.BacklogV2
	store                  *sqlite.Store
	coordinator            backlog.FleetCoordinator
	epoch                  int64
	maxWorkerSnapshotAge   time.Duration
	maxQuotaObservationAge time.Duration
	deadlineRiskWindow     time.Duration
	checkpointMargin       time.Duration
	// supervisorClientConfigured is the third condition of supervisor route
	// availability, which the planner cannot read from the store. See
	// backlog.SupervisionRouteRequest.
	supervisorClientConfigured bool
	now                        func() time.Time
}

func (p coordinatorPlanner) Tick(ctx context.Context, quota backlog.QuotaBridgeReport) (backlog.AssignmentPlanningReport, error) {
	return p.tick(ctx, quota, true)
}

// yieldToOlderActivationContender admits already-eligible work only when its
// durable age wins at a worker or quota-pool bottleneck used by candidate.
func (p coordinatorPlanner) yieldToOlderActivationContender(ctx context.Context, quota backlog.QuotaBridgeReport, candidate activationFairnessCandidate) (bool, error) {
	now := time.Now().UTC()
	if p.now != nil {
		now = p.now().UTC()
	}
	if _, err := backlog.RepairCoordinatorState(ctx, p.store, now); err != nil {
		slog.Warn("coordinator state repair failed; activation arbitration continues", "error", err)
	}
	records, err := p.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return false, fmt.Errorf("load activation arbitration snapshot: %w", err)
	}
	snapshots, err := p.store.LoadWorkerSnapshots(ctx)
	if err != nil {
		return false, fmt.Errorf("load activation arbitration workers: %w", err)
	}
	if p.settings != nil {
		snapshots = workerruntime.AuthorizedPlanningSnapshots(*p.settings, snapshots, now)
	}
	supervision, err := coordinatorSupervisionSnapshots(ctx, p.store, records.WorkflowRuns, snapshots, p.supervisorClientConfigured)
	if err != nil {
		return false, err
	}
	input, err := backlog.BuildCoordinatorPlanInput(backlog.CoordinatorPlanningStateInput{
		Now: now, CoordinatorEpoch: p.epoch,
		Workflows: records.Workflows, WorkflowRuns: records.WorkflowRuns,
		Tasks: records.Tasks, Attempts: records.Attempts, Assignments: records.Assignments,
		WorkerSnapshots: snapshots, QuotaPools: quota.Pools, QuotaWindows: quota.Windows,
		DisableQuotaChecks:   quota.ChecksDisabled,
		MaxWorkerSnapshotAge: p.maxWorkerSnapshotAge, MaxQuotaObservationAge: p.maxQuotaObservationAge,
		DeadlineRiskWindow: p.deadlineRiskWindow, CheckpointMargin: p.checkpointMargin,
		SupervisionSnapshots: supervision,
	})
	if err != nil {
		return false, err
	}
	contenders, err := backlog.BuildUnreservedProposals(input)
	if err != nil {
		return false, err
	}
	cutoff := sqlite.TaskWakeCutoff{ReadyAt: candidate.ReadyAt, AttemptID: candidate.ID}
	// WakeTaskWaitsBefore treats a missing cutoff as unconstrained. Block every
	// disjoint worker and pool, then open only the two resources this activation
	// would consume. Unrelated wakes may run in the normal planning pass, but they
	// are not evidence that this activation lost its own fairness contest.
	blocked := sqlite.TaskWakeCutoff{}
	cutoffs := sqlite.TaskWakeCutoffs{
		Worker:              make(map[string]sqlite.TaskWakeCutoff),
		Pool:                make(map[string]sqlite.TaskWakeCutoff),
		AuthorizedWorkers:   make(map[string]sqlite.TaskWakeWorkerAuthorization),
		AdmissionValidAfter: now.Add(-p.maxQuotaObservationAge),
	}
	for _, snapshot := range snapshots {
		cutoffs.Worker[snapshot.WorkerID] = blocked
		cutoffs.AuthorizedWorkers[snapshot.WorkerID] = sqlite.TaskWakeWorkerAuthorization{
			WorkerEpoch: snapshot.WorkerEpoch, SnapshotSequence: snapshot.Sequence,
			CatalogRevision: snapshot.Inventory.CatalogRevision, ValidUntil: snapshot.ValidUntil,
			Providers: append([]domain.WorkerProviderInventory(nil), snapshot.Inventory.Providers...),
			Projects:  append([]domain.WorkerProjectInventory(nil), snapshot.Inventory.Projects...),
		}
		for _, provider := range snapshot.Inventory.Providers {
			if provider.QuotaPoolID != "" {
				cutoffs.Pool[provider.QuotaPoolID] = blocked
			}
		}
	}
	cutoffs.Worker[candidate.Worker] = cutoff
	cutoffs.Pool[candidate.Pool] = cutoff
	older := func(at time.Time, id string, than sqlite.TaskWakeCutoff) bool {
		return at.Before(than.ReadyAt) || at.Equal(than.ReadyAt) && id < than.AttemptID
	}
	hasOlderOrdinary := false
	for _, proposal := range contenders {
		if proposal.Route == nil || proposal.WorkerID != candidate.Worker && proposal.Route.QuotaPoolID != candidate.Pool {
			continue
		}
		ordering, ok := input.Ordering.Attempts[proposal.AttemptID]
		if !ok || ordering.ReadySince.IsZero() {
			return false, fmt.Errorf("ordinary proposal %q has no durable ready age", proposal.AttemptID)
		}
		ordinary := sqlite.TaskWakeCutoff{ReadyAt: ordering.ReadySince, AttemptID: proposal.AttemptID}
		if older(ordinary.ReadyAt, ordinary.AttemptID, cutoff) {
			hasOlderOrdinary = true
		}
		if proposal.WorkerID == candidate.Worker && older(ordinary.ReadyAt, ordinary.AttemptID, cutoffs.Worker[candidate.Worker]) {
			cutoffs.Worker[candidate.Worker] = ordinary
		}
		if proposal.Route.QuotaPoolID == candidate.Pool && older(ordinary.ReadyAt, ordinary.AttemptID, cutoffs.Pool[candidate.Pool]) {
			cutoffs.Pool[candidate.Pool] = ordinary
		}
	}
	wakes, err := p.store.WakeTaskWaitsBefore(ctx, now, cutoffs)
	if err != nil {
		return false, err
	}
	if len(wakes) != 0 {
		return true, nil
	}
	if hasOlderOrdinary {
		report, err := p.coordinator.PlanAndCommit(ctx, input)
		if err != nil {
			return false, err
		}
		// Planning may commit independent work, or older work in a shared pool
		// that still has another slot. Yield only when the committed batch
		// actually exhausted this activation's worker or pool.
		workerAvailable, err := p.store.ExecutorSlotAvailable(ctx, candidate.Worker, now)
		if err != nil {
			return false, fmt.Errorf("recheck activation executor capacity: %w", err)
		}
		if !workerAvailable {
			return true, nil
		}
		for _, pool := range quota.Pools {
			if pool.ID != candidate.Pool {
				continue
			}
			active := pool.ActiveAssignments
			for _, assignment := range report.Assignments {
				if assignment.Route.QuotaPoolID == candidate.Pool {
					active++
				}
			}
			if active >= pool.MaxConcurrent {
				return true, nil
			}
			break
		}
	}
	return false, nil
}

func (p coordinatorPlanner) tick(ctx context.Context, quota backlog.QuotaBridgeReport, reconcileWakes bool) (backlog.AssignmentPlanningReport, error) {
	now := time.Now().UTC()
	if p.now != nil {
		now = p.now().UTC()
	}
	// Publish DAG progress first so dependents of finished tasks are ready
	// in the durable projection operators read, not only in planner memory.
	if _, err := backlog.RepairCoordinatorState(ctx, p.store, now); err != nil {
		slog.Warn("coordinator state repair failed; planning continues", "error", err)
	}
	records, err := p.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return backlog.AssignmentPlanningReport{}, fmt.Errorf("load planning coordinator snapshot: %w", err)
	}
	snapshots, err := p.store.LoadWorkerSnapshots(ctx)
	if err != nil {
		return backlog.AssignmentPlanningReport{}, fmt.Errorf("load planning worker snapshots: %w", err)
	}
	if p.settings != nil {
		snapshots = workerruntime.AuthorizedPlanningSnapshots(*p.settings, snapshots, now)
	}
	supervision, err := coordinatorSupervisionSnapshots(
		ctx, p.store, records.WorkflowRuns, snapshots, p.supervisorClientConfigured)
	if err != nil {
		return backlog.AssignmentPlanningReport{}, err
	}
	input, err := backlog.BuildCoordinatorPlanInput(backlog.CoordinatorPlanningStateInput{
		Now: now, CoordinatorEpoch: p.epoch,
		Workflows: records.Workflows, WorkflowRuns: records.WorkflowRuns,
		Tasks: records.Tasks, Attempts: records.Attempts, Assignments: records.Assignments,
		WorkerSnapshots: snapshots, QuotaPools: quota.Pools, QuotaWindows: quota.Windows,
		DisableQuotaChecks:     quota.ChecksDisabled,
		MaxWorkerSnapshotAge:   p.maxWorkerSnapshotAge,
		MaxQuotaObservationAge: p.maxQuotaObservationAge,
		DeadlineRiskWindow:     p.deadlineRiskWindow, CheckpointMargin: p.checkpointMargin,
		// Supervision is resolved per run and fenced on nothing here: BuildPlan
		// is pure, so the snapshot it reasons from is read once, now.
		SupervisionSnapshots: supervision,
	})
	if err != nil {
		return backlog.AssignmentPlanningReport{}, err
	}
	// Compare settled wakes with ordinary work that is actually placeable in
	// this planning snapshot. Wakes older than the oldest ordinary proposal at
	// either shared bottleneck get the first transactional capacity check;
	// newer wakes yield. The normal planner then reloads after any resumption.
	if !reconcileWakes {
		return p.coordinator.PlanAndCommit(ctx, input)
	}
	hasReadyWakes, err := p.store.HasReadyTaskWaits(ctx)
	if err != nil {
		return backlog.AssignmentPlanningReport{}, fmt.Errorf("inspect settled task wakes: %w", err)
	}
	if !hasReadyWakes {
		return p.coordinator.PlanAndCommit(ctx, input)
	}
	contenders, err := backlog.BuildUnreservedProposals(input)
	if err != nil {
		return backlog.AssignmentPlanningReport{}, err
	}
	cutoffs := sqlite.TaskWakeCutoffs{
		Worker:              make(map[string]sqlite.TaskWakeCutoff),
		Pool:                make(map[string]sqlite.TaskWakeCutoff),
		AuthorizedWorkers:   make(map[string]sqlite.TaskWakeWorkerAuthorization),
		AdmissionValidAfter: now.Add(-p.maxQuotaObservationAge),
	}
	for _, snapshot := range snapshots {
		cutoffs.AuthorizedWorkers[snapshot.WorkerID] = sqlite.TaskWakeWorkerAuthorization{
			WorkerEpoch: snapshot.WorkerEpoch, SnapshotSequence: snapshot.Sequence,
			CatalogRevision: snapshot.Inventory.CatalogRevision, ValidUntil: snapshot.ValidUntil,
			Providers: append([]domain.WorkerProviderInventory(nil), snapshot.Inventory.Providers...),
			Projects:  append([]domain.WorkerProjectInventory(nil), snapshot.Inventory.Projects...),
		}
	}
	older := func(current sqlite.TaskWakeCutoff, candidate sqlite.TaskWakeCutoff) bool {
		return current.ReadyAt.IsZero() || candidate.ReadyAt.Before(current.ReadyAt) || candidate.ReadyAt.Equal(current.ReadyAt) && candidate.AttemptID < current.AttemptID
	}
	for _, proposal := range contenders {
		ordering, ok := input.Ordering.Attempts[proposal.AttemptID]
		if !ok || ordering.ReadySince.IsZero() {
			return backlog.AssignmentPlanningReport{}, fmt.Errorf("ordinary proposal %q has no durable ready age", proposal.AttemptID)
		}
		if proposal.Route == nil {
			continue
		}
		candidate := sqlite.TaskWakeCutoff{ReadyAt: ordering.ReadySince, AttemptID: proposal.AttemptID}
		if current := cutoffs.Worker[proposal.WorkerID]; older(current, candidate) {
			cutoffs.Worker[proposal.WorkerID] = candidate
		}
		if current := cutoffs.Pool[proposal.Route.QuotaPoolID]; older(current, candidate) {
			cutoffs.Pool[proposal.Route.QuotaPoolID] = candidate
		}
	}
	wakes, err := p.store.WakeTaskWaitsBefore(ctx, now, cutoffs)
	if err != nil {
		return backlog.AssignmentPlanningReport{}, fmt.Errorf("admit settled task wakes: %w", err)
	}
	if len(wakes) != 0 {
		// Reload once after transactional wake admission so ordinary planning sees
		// the newly occupied capacity. Concurrent settlements wait for the next
		// coordinator boundary instead of recursively extending this one.
		return p.tick(ctx, quota, false)
	}
	return p.coordinator.PlanAndCommit(ctx, input)
}

// coordinatorWorkerAuthorization reports, per worker, the provider instances
// this coordinator's effective configuration authorizes, and the instances the
// fleet projection authorized that the load dropped.
//
// The two halves are joined here because nothing downstream can: a dropped
// instance is in no worker catalog and therefore in no quota pool, so the
// only record that it was ever authorized is the load's own. "t3-steward
// models" reports it as the reason a route is missing.
func coordinatorWorkerAuthorization(cfg config.Config) map[string][]backlogadmin.WorkerProviderAuthorization {
	authorization := make(map[string][]backlogadmin.WorkerProviderAuthorization, len(cfg.BacklogV2.Workers))
	for id, worker := range cfg.BacklogV2.Workers {
		for instance, provider := range worker.Providers {
			authorization[id] = append(authorization[id], backlogadmin.WorkerProviderAuthorization{
				Instance: instance, QuotaPool: provider.QuotaPool,
				Models: append([]string(nil), provider.Models...),
			})
		}
	}
	for _, dropped := range cfg.DroppedFleetProviders() {
		authorization[dropped.Worker] = append(authorization[dropped.Worker], backlogadmin.WorkerProviderAuthorization{
			Instance: dropped.Instance, Dropped: dropped.Reason,
		})
	}
	for id := range authorization {
		sort.Slice(authorization[id], func(i, j int) bool {
			return authorization[id][i].Instance < authorization[id][j].Instance
		})
	}
	return authorization
}

func coordinatorQuotaPoolBindings(cfg config.Config) []backlog.QuotaPoolBinding {
	instancesByPool := make(map[string]map[string]struct{}, len(cfg.BacklogV2.QuotaPools))
	modelsByPool := make(map[string]map[string]struct{}, len(cfg.BacklogV2.QuotaPools))
	for _, worker := range cfg.BacklogV2.Workers {
		for instanceID, provider := range worker.Providers {
			if instancesByPool[provider.QuotaPool] == nil {
				instancesByPool[provider.QuotaPool] = make(map[string]struct{})
				modelsByPool[provider.QuotaPool] = make(map[string]struct{})
			}
			instancesByPool[provider.QuotaPool][instanceID] = struct{}{}
			for _, model := range provider.Models {
				modelsByPool[provider.QuotaPool][model] = struct{}{}
			}
		}
	}
	poolIDs := make([]string, 0, len(cfg.BacklogV2.QuotaPools))
	for poolID := range cfg.BacklogV2.QuotaPools {
		poolIDs = append(poolIDs, poolID)
	}
	sort.Strings(poolIDs)
	bindings := make([]backlog.QuotaPoolBinding, 0, len(poolIDs))
	for _, poolID := range poolIDs {
		pool := cfg.BacklogV2.QuotaPools[poolID]
		instances := make([]string, 0, len(instancesByPool[poolID]))
		for instanceID := range instancesByPool[poolID] {
			instances = append(instances, instanceID)
		}
		sort.Strings(instances)
		models := make([]string, 0, len(modelsByPool[poolID]))
		for model := range modelsByPool[poolID] {
			models = append(models, model)
		}
		sort.Strings(models)
		bindings = append(bindings, backlog.QuotaPoolBinding{
			ID: poolID, Provider: pool.Provider,
			ProviderInstanceIDs: instances, MaxConcurrent: pool.MaxConcurrent,
			Models: models, IgnoredWindows: append([]string(nil), cfg.Policy.IgnoreWindows...),
		})
	}
	return bindings
}

type coordinatorScheduleTicker interface {
	Tick(context.Context) (backlog.ScheduleTickReport, error)
}

type coordinatorAdminExecutor interface {
	ExecutePendingCommands(context.Context) (backlogadmin.CommandExecutionReport, error)
}

type coordinatorLegacyTicker interface {
	Tick(context.Context) backlog.LegacySubmissionReport
}

type coordinatorWorkerTicker interface {
	Tick(context.Context, backlog.QuotaBridgeReport) coordinatorWorkerTickReport
}

type coordinatorCampaignRefTicker interface {
	Tick(context.Context) backlog.CampaignRefReleaseReport
}

type coordinatorBoundaryCycle struct {
	projection   backlog.ProjectionStore
	quota        coordinatorQuotaTicker
	schedules    coordinatorScheduleTicker
	planning     coordinatorPlanningTicker
	admin        coordinatorAdminExecutor
	legacy       coordinatorLegacyTicker
	workers      coordinatorWorkerTicker
	campaignRefs coordinatorCampaignRefTicker
	// supervision advances gates, raises review incidents and escalates a
	// supervisor route nobody can run. A nil value is the unsupervised
	// deployment and changes nothing.
	supervision *coordinatorSupervision
	logger      *slog.Logger
}

func (c coordinatorBoundaryCycle) Tick(ctx context.Context) {
	c.tick(ctx, false)
}

// TickWithWorkers is used only after the startup-local pass. Worker observation
// remains available when quota reconciliation fails, but the empty admission
// report makes every new-work transport boundary fail closed.
func (c coordinatorBoundaryCycle) TickWithWorkers(ctx context.Context) {
	c.tick(ctx, true)
}

// logTickFailure reports one failed step of a boundary cycle. A step that
// failed because the cycle's context was cancelled is the coordinator shutting
// down, not an operational fault: every store call in flight returns "context
// canceled" at once, and logging that burst at ERROR buried the errors an
// operator does have to read. Those are reported at INFO as shutdown; every
// other failure keeps its severity.
func logTickFailure(ctx context.Context, logger *slog.Logger, msg string, err error, attrs ...any) {
	// The cycle's own context is the authoritative signal. The error is also
	// checked because a store call that observed the cancellation returns it
	// wrapped, sometimes before ctx.Err() is visible to this goroutine; no tick
	// path returns context.Canceled from a per-request context of its own.
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		logger.Info("shutting down: "+msg, append(attrs, "error", err)...)
		return
	}
	logger.Error(msg, append(attrs, "error", err)...)
}

func (c coordinatorBoundaryCycle) tick(ctx context.Context, exchangeWorkers bool) {
	if c.projection != nil {
		if _, err := backlog.ProjectWorkflowRuns(ctx, c.projection, time.Now().UTC()); err != nil {
			logTickFailure(ctx, c.logger, "workflow run projection failed", err)
		}
	}
	// Supervision runs after the projection and before worker exchange: a gate
	// whose producers just succeeded becomes reviewable on the same boundary
	// that recorded their success, and before anything the gate protects could
	// be offered.
	if c.supervision != nil {
		c.supervision.Tick(ctx)
	}
	if store, ok := c.projection.(interface {
		SettleNodeWaits(context.Context, time.Time) error
	}); ok {
		if err := store.SettleNodeWaits(ctx, time.Now().UTC()); err != nil {
			logTickFailure(ctx, c.logger, "node wait settlement failed", err)
		}
	}
	// Task-bound wait expiry is driven from here, not only from the watchdog's
	// wait runner. A parked attempt holds its directory writer binding, and
	// those have no deadline of their own, so a coordinator running without the
	// watchdog would otherwise let one parked task block the fleet forever.
	if store, ok := c.projection.(interface {
		ExpireTaskWaits(context.Context, time.Time) ([]domain.TaskWait, error)
	}); ok {
		if expired, err := store.ExpireTaskWaits(ctx, time.Now().UTC()); err != nil {
			logTickFailure(ctx, c.logger, "task-bound wait expiry failed", err)
		} else if len(expired) != 0 {
			c.logger.Warn("task-bound waits exceeded their maximum duration", "waits", len(expired))
		}
	}
	// A settled run stops pinning its declared commits. This runs after the
	// projection so that a run which settled on this boundary is released on
	// this boundary, and its failures are operational: a ref that could not be
	// deleted is retried next time and changes nothing about the finished run.
	if c.campaignRefs != nil {
		report := c.campaignRefs.Tick(ctx)
		for _, err := range report.Errors {
			logTickFailure(ctx, c.logger, "campaign commit release failed", err)
		}
		if len(report.Released) != 0 {
			c.logger.Info("campaign commits released", "runs", len(report.Released))
		}
	}
	quotaHealthy := true
	quotaReport, err := c.quota.Tick(ctx)
	if err != nil {
		quotaHealthy = false
		logTickFailure(ctx, c.logger, "backlog-v2 quota reconciliation failed", err)
	} else if len(quotaReport.Directives) != 0 {
		c.logger.Info("backlog-v2 quota transitions reconciled", "directives", len(quotaReport.Directives))
	}
	if report, err := c.schedules.Tick(ctx); err != nil {
		logTickFailure(ctx, c.logger, "backlog-v2 schedule reconciliation failed", err)
	} else if len(report.Results) != 0 {
		c.logger.Info("backlog-v2 schedule occurrences reconciled", "results", len(report.Results))
	}
	// Supervision reconciliation runs even when quota reconciliation failed:
	// completed, lost and expired activations must still settle. A failed
	// quota pass supplies an empty fail-closed policy, so any transition
	// that would create provider work remains blocked.
	if c.supervision != nil {
		admission := backlog.WorkerAdmissionPolicy{}
		if quotaHealthy {
			admission = backlog.WorkerAdmissionPolicyFromQuotaReport(quotaReport)
		}
		if planner, ok := c.planning.(*coordinatorPlanner); ok && quotaHealthy {
			c.supervision.yieldToOlderWork = func(ctx context.Context, candidate activationFairnessCandidate) (bool, error) {
				return planner.yieldToOlderActivationContender(ctx, quotaReport, candidate)
			}
		} else {
			c.supervision.yieldToOlderWork = nil
		}
		c.supervision.quotaMaxConcurrent = make(map[string]int, len(quotaReport.Pools))
		for _, pool := range quotaReport.Pools {
			c.supervision.quotaMaxConcurrent[pool.ID] = pool.MaxConcurrent
		}
		c.supervision.DispatchActivations(ctx, admission)
	}
	if quotaHealthy {
		if report, err := c.planning.Tick(ctx, quotaReport); err != nil {
			logTickFailure(ctx, c.logger, "backlog-v2 assignment planning failed", err)
		} else if len(report.Assignments) != 0 {
			c.logger.Info("backlog-v2 assignment plans committed", "assignments", len(report.Assignments))
		}
		if report, err := c.admin.ExecutePendingCommands(ctx); err != nil {
			logTickFailure(ctx, c.logger, "backlog-v2 admin command execution failed", err)
		} else if len(report.Decisions) != 0 {
			c.logger.Info("backlog-v2 admin commands executed", "decisions", len(report.Decisions))
		}
	} else {
		c.logger.Warn("backlog-v2 planning and admin command execution deferred until quota reconciliation succeeds")
	}
	report := c.legacy.Tick(ctx)
	for _, err := range report.Errors {
		logTickFailure(ctx, c.logger, "legacy backlog-v2 submission failed", err)
	}
	if len(report.Accepted) != 0 {
		c.logger.Info("legacy backlog-v2 submissions reconciled", "accepted", len(report.Accepted))
	}
	if exchangeWorkers && c.workers != nil {
		workerReport := c.workers.Tick(ctx, quotaReport)
		for _, result := range workerReport.Results {
			if result.Err != nil {
				logTickFailure(ctx, c.logger, "backlog-v2 worker reconciliation failed", result.Err, "worker", result.WorkerID)
				continue
			}
			c.logger.Info("backlog-v2 worker reconciled", "worker", result.WorkerID,
				"offered", len(result.Report.Offered), "claimed", len(result.Report.Claimed),
				"withheld", len(result.Report.Withheld)+len(result.Report.Delivery.Withheld))
		}
	}
}

func runBacklogV2Coordinator(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, cfg.BacklogV2.Coordinator.ID)
	if err != nil {
		return err
	}
	if _, err := backlog.RepairCoordinatorState(ctx, store, time.Now().UTC()); err != nil {
		logger.Warn("coordinator state repair failed; continuing", "error", err)
	}

	return coordinatorConfigLoop(ctx, cfg, logger, store, epoch)
}

func runCoordinatorConfiguration(ctx context.Context, cfg config.Config, logger *slog.Logger, store *sqlite.Store, epoch int64, receipts *reloadReceiptWriter, ready func()) error {
	// A supervisor capability is enforced server-side, around the ordinary
	// authorizer rather than instead of it: every other principal is delegated
	// unchanged, and a supervisor is bound to the one run and epoch the
	// coordinator's own activation records say it was woken for.
	service, err := backlogadmin.New(store, backlogadmin.SupervisorAuthorizer{
		Scope:    backlogadmin.CoordinatorSupervisionStore{Store: store},
		Delegate: localAdminAuthorizer{},
	})
	if err != nil {
		return err
	}
	service.SetGraphAmendmentSupport(cfg.BacklogV2.Storage.Artifacts, graphTaskValidator(cfg.BacklogV2))
	configurationDigest, err := coordinatorConfigurationDigest(cfg.BacklogV2)
	if err != nil {
		return err
	}
	appliedAt := time.Now().UTC()
	// A project the catalog cannot hold no longer stops the fleet, so nothing
	// else breaks to make an operator look. The coordinator says so at
	// startup and keeps saying so in its status, naming the project and the
	// exact validation failure.
	catalogIssues := workerruntime.FleetCatalogIssues(cfg.BacklogV2)
	for _, issue := range catalogIssues {
		logger.Warn("backlog-v2 project configuration is unusable",
			"issue", issue,
			"effect", "this project cannot be scheduled; every other project is unaffected")
	}
	service.SetRuntimeInfo(backlogadmin.RuntimeInfo{
		Release: version, ConfigurationDigest: configurationDigest, LastReload: appliedAt, LastReloadReceipt: receipts.Last,
		Mode: "coordinator", Owner: cfg.BacklogV2.Coordinator.ID, Epoch: epoch,
		Transport:              cfg.BacklogV2.Transport.Kind,
		MaxWorkerSnapshotAge:   cfg.BacklogV2.Freshness.WorkerMaxAge.D(),
		MaxQuotaObservationAge: cfg.BacklogV2.Freshness.QuotaMaxAge.D(),
		CatalogIssues:          catalogIssues,
	})
	artifactStore := backlog.CoordinatorArtifactStore{Root: cfg.BacklogV2.Storage.Artifacts, SubmissionRoot: cfg.BacklogV2.Storage.Bundles, Catalog: store}
	service.SetArtifactOpener(func(ctx context.Context, artifactID string) (domain.Artifact, io.ReadCloser, error) {
		artifact, content, openErr := artifactStore.Open(ctx, artifactID)
		return artifact, content, openErr
	})
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		return err
	}
	listener, err := backlogadmin.ListenLocal(socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(socketPath)
	projectTargets := make(map[string]string, len(cfg.BacklogV2.Projects))
	for name, project := range cfg.BacklogV2.Projects {
		projectTargets[name] = project.T3Project
	}
	aliases, err := backlog.LegacyProjectAliases(projectTargets)
	if err != nil {
		return err
	}
	backlogDir, err := cfg.ResolveBacklogDir()
	if err != nil {
		return err
	}
	directoryCatalogs := make(map[string][]directoryresource.Binding)
	for name, project := range cfg.BacklogV2.Projects {
		directoryCatalogs[name] = directoryresource.CloneBindings(project.DirectoryResources)
	}
	fleetProjects, fleetProfiles := workerruntime.BuildFleetDefinitions(cfg.BacklogV2)
	// The supervisor admin client is resolved here, before anything that
	// reports on supervision is composed, because its absence is not a detail of
	// activation dispatch alone: it is the answer the readiness matrix, the
	// explanation blockers and "campaign supervision show" each have to give.
	supervisorPrincipal, supervisorCredential, err := coordinatorSupervisorClient(cfg.BacklogV2.Coordinator.AdminClients)
	if err != nil {
		// Ambiguous supervisor configuration disables activation dispatch and
		// says so. Every other boundary keeps running: a supervised run then
		// waits for an operator decision instead of getting an overseer whose
		// authority nobody can name.
		logger.Error("campaign supervision activations are disabled", "error", err)
	}
	if supervisorPrincipal == "" {
		// Said once at startup, where an operator reading the coordinator's first
		// lines learns it before a campaign is ever submitted.
		logger.Warn("campaign supervision activations are disabled",
			"reason", backlog.SupervisionNoSupervisorClient,
			"remedy", "declare exactly one backlog_v2.coordinator.admin_clients entry with supervisor: true")
	}
	for _, name := range cfg.DefaultedFleetProjects() {
		// Said once at startup: the project runs, but with nothing host-local
		// bound, and an operator reading the first lines should know which.
		logger.Warn("fleet project has no backlog_v2.projects entry; loaded with default local bindings",
			"project", name,
			"effect", "no credentials, resource locks or directory resources",
			"remedy", "add a backlog_v2.projects entry only if the project needs host-local bindings")
	}
	for _, dropped := range cfg.DroppedFleetProviders() {
		if dropped.Reason != config.DroppedProviderMissingBinding {
			// An instance the projection authorizes with no desired models is
			// not a fault: it grants no execution authorization and never did.
			// It is recorded so "models" can say so, not warned about here.
			continue
		}
		// Said once at startup, where an operator reading the first lines
		// learns which route the fleet authorized and this coordinator cannot
		// charge. The rest of the worker runs.
		logger.Warn("fleet provider instance has no authorized quota binding; dropped for this worker",
			"instance", dropped.Instance, "worker", dropped.Worker,
			"effect", "no task is routed to this instance on this worker",
			"remedy", dropped.Remedy)
	}
	projectWorkers := make(map[string][]string, len(cfg.BacklogV2.Projects))
	for name, project := range cfg.BacklogV2.Projects {
		projectWorkers[name] = append([]string(nil), project.Workers...)
	}
	service.SetWorkerAuthorization(coordinatorWorkerAuthorization(cfg))
	service.SetViability(backlogadmin.ViabilitySettings{
		Projects:          fleetProjects,
		SetupProfiles:     fleetProfiles,
		DefaultedProjects: cfg.DefaultedFleetProjects(),
		ProjectWorkers:    projectWorkers,
		MaxBundleBytes:    cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxBundleFiles:    cfg.BacklogV2.MessageLimits.MaxFiles,
		// Repository reachability is observed on the candidate worker, over the
		// worker protocol, under the credential references the real task would
		// use. Without this the readiness check reported nothing at all about the
		// one failure it was built for: a campaign accepted against a repository
		// that does not exist, which fails hours later in workspace preparation.
		Repository: newCoordinatorRepositoryObserver(
			cfg.BacklogV2, workerruntime.ProtocolResolver{}, epoch, nil),
		SupervisorClientConfigured: supervisorPrincipal != "",
	})
	// Every supervision answer this coordinator gives -- the readiness matrix
	// above, the explanation blockers and "campaign supervision show" -- has to
	// know whether an overseer could be dispatched at all, and none of them can
	// discover it from a record.
	service.SetSupervisorClientConfigured(supervisorPrincipal != "")
	submissions := &backlog.SubmissionService{
		DirectoryCatalogs: directoryCatalogs,
		StorageRoot:       cfg.BacklogV2.Storage.Bundles,
		Store:             store,
		MaxBytes:          cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxFiles:          cfg.BacklogV2.MessageLimits.MaxFiles,
		// The permanent part of the readiness check is repeated here, so a
		// client that skipped it, or a fleet that changed after the client
		// checked, still cannot create an impossible run.
		Permanent: coordinatorPermanentValidator{admin: service},
		Audit: func(_ context.Context, audit backlog.SubmissionAudit) {
			if !audit.Unverified {
				return
			}
			logger.Warn("campaign submitted without a client-side readiness check",
				"key", audit.Key, "digest", audit.Digest,
				"principal", audit.Principal, "reason", audit.UnverifiedReason)
		},
	}
	service.SetWorkerEnrollmentHandler(coordinatorEnrollmentHandler(cfg.BacklogV2, store, epoch, artifactStore))
	scheduleDefinitions := &backlog.ScheduleDefinitionService{Store: store}
	server := backlogadmin.LocalServer{
		Listener: listener,
		Service: coordinatorLocalService{
			admin: service, submissions: submissions, schedules: scheduleDefinitions,
		},
		AllowedUID:         uint32(os.Getuid()),
		CoordinatorID:      cfg.BacklogV2.Coordinator.ID,
		MaxRequestBytes:    int64(cfg.BacklogV2.MessageLimits.MaxBytes),
		MaxArtifactBytes:   int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
		MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		RequestTimeout:     cfg.BacklogV2.Transport.RequestTimeout.D(),
		MaxConcurrent:      16,
	}
	workers, err := newCoordinatorWorkerSessions(
		cfg.BacklogV2, store, epoch, workerruntime.ProtocolResolver{}, nil, artifactStore,
	)
	if err != nil {
		return err
	}
	defer workers.close()
	supervisionStore := backlog.CoordinatorSupervisionStore{Store: store}
	service.SetSupervisionStore(backlogadmin.CoordinatorSupervisionStore{Store: store})
	cycle := coordinatorBoundaryCycle{
		projection: supervisionStore,
		supervision: &coordinatorSupervision{
			store: supervisionStore, logger: logger,
			workers:                  store.LoadWorkerSnapshots,
			activations:              backlog.SupervisionActivationService{Store: supervisionStore},
			warnedNoSupervisorClient: make(map[string]bool),
			settings: coordinatorActivationSettings{
				CoordinatorID:                 cfg.BacklogV2.Coordinator.ID,
				CoordinatorEpoch:              epoch,
				SupervisorClient:              supervisorPrincipal,
				SupervisorCredentialReference: supervisorCredential,
			},
		},
		quota: coordinatorQuotaReconciler{store: store, bridge: backlog.QuotaBridge{
			Store: store, Pools: coordinatorQuotaPoolBindings(cfg), Disabled: !cfg.QuotaChecksEnabled(),
			MaxObservationAge:       cfg.BacklogV2.Freshness.QuotaMaxAge.D(),
			SafetyMargin:            cfg.Backlog.SafetyMargin,
			FallbackForecastPerHour: cfg.Backlog.FallbackPerHour,
			LongWindowCap:           cfg.Backlog.LongWindowCap,
			SurplusHorizon:          24 * time.Hour,
		}},
		schedules: backlog.ScheduleTimer{Store: store, CatchUpMax: cfg.BacklogV2.Scheduling.CatchUpMax},
		planning: coordinatorPlanner{
			settings: &cfg.BacklogV2,
			store:    store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch,
			maxWorkerSnapshotAge:       cfg.BacklogV2.Freshness.WorkerMaxAge.D(),
			maxQuotaObservationAge:     cfg.BacklogV2.Freshness.QuotaMaxAge.D(),
			deadlineRiskWindow:         24 * time.Hour,
			checkpointMargin:           cfg.BacklogV2.Leases.RenewInterval.D(),
			supervisorClientConfigured: supervisorPrincipal != "",
		},
		admin: service,
		legacy: backlog.LegacySubmissionSource{
			Dir: backlogDir, Submitter: submissions, ProjectAliases: aliases,
			MaxBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
			MaxFiles: cfg.BacklogV2.MessageLimits.MaxFiles, AllowedUID: uint32(os.Getuid()),
			Quarantine: store,
		},
		workers: workers,
		// The campaign ref store is the one this host's worker publishes into,
		// a sibling of the repository cache under the same configured
		// workspaces root. A worker on another host keeps its own store and
		// releases it from the keep list this coordinator states on every
		// snapshot exchange.
		campaignRefs: &backlog.CampaignRefReleaseReconciler{
			Records: store.LoadCoordinatorRecords,
			Refs:    backlog.CampaignRefStore{Root: filepath.Join(cfg.BacklogV2.Storage.Workspaces, "campaign-refs")},
		},
		logger: logger,
	}
	logger.Info("backlog-v2 coordinator authority acquired",
		"coordinator", cfg.BacklogV2.Coordinator.ID,
		"epoch", epoch,
		"admission", "closed",
		"admin_socket", socketPath)
	// The quota watchdog, wait polling, and archiving keep running on this
	// host alongside the coordinator; they share the state database.
	if err := store.RecordCoordinatorConfiguration(ctx, epoch, configurationDigest, appliedAt); err != nil {
		return err
	}
	ready()
	return serveCoordinatorBoundaries(ctx, &server, cycle, cfg.BacklogV2.Scheduling.Interval.D())
}

func serveCoordinatorBoundaries(
	ctx context.Context,
	server *backlogadmin.LocalServer,
	cycle coordinatorBoundaryCycle,
	interval time.Duration,
) error {
	if interval <= 0 {
		return errors.New("coordinator boundary interval must be positive")
	}
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(ctx)
	}()
	cycle.Tick(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := <-serverDone; err != nil {
				return err
			}
			return nil
		case err := <-serverDone:
			return err
		case <-ticker.C:
			cycle.TickWithWorkers(ctx)
		}
	}
}
