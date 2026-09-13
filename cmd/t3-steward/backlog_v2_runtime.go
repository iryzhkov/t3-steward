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
	_ backlogadmin.Principal,
	request backlogadmin.LocalSubmissionRequest,
	archive io.Reader,
) (backlogadmin.LocalSubmissionResponse, error) {
	result, err := s.submissions.SubmitArchive(ctx, backlog.ArchiveSubmission{
		IdempotencyKey: request.IdempotencyKey,
		Archive:        archive,
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
	throttleRecords, err := r.store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		return backlog.QuotaBridgeReport{}, fmt.Errorf("load quota throttle snapshot: %w", err)
	}
	report, err := r.bridge.ReconcileState(ctx, backlog.QuotaPlanningStateInput{
		Tasks: records.Tasks, Attempts: records.Attempts,
		Assignments: records.Assignments, ThrottleRecords: throttleRecords,
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
	store                  *sqlite.Store
	coordinator            backlog.FleetCoordinator
	epoch                  int64
	maxWorkerSnapshotAge   time.Duration
	maxQuotaObservationAge time.Duration
	deadlineRiskWindow     time.Duration
	checkpointMargin       time.Duration
	now                    func() time.Time
}

func (p coordinatorPlanner) Tick(ctx context.Context, quota backlog.QuotaBridgeReport) (backlog.AssignmentPlanningReport, error) {
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
	input, err := backlog.BuildCoordinatorPlanInput(backlog.CoordinatorPlanningStateInput{
		Now: now, CoordinatorEpoch: p.epoch,
		Workflows: records.Workflows, WorkflowRuns: records.WorkflowRuns,
		Tasks: records.Tasks, Attempts: records.Attempts, Assignments: records.Assignments,
		WorkerSnapshots: snapshots, QuotaPools: quota.Pools, QuotaWindows: quota.Windows,
		MaxWorkerSnapshotAge:   p.maxWorkerSnapshotAge,
		MaxQuotaObservationAge: p.maxQuotaObservationAge,
		DeadlineRiskWindow:     p.deadlineRiskWindow, CheckpointMargin: p.checkpointMargin,
	})
	if err != nil {
		return backlog.AssignmentPlanningReport{}, err
	}
	return p.coordinator.PlanAndCommit(ctx, input)
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

type coordinatorBoundaryCycle struct {
	projection backlog.ProjectionStore
	quota      coordinatorQuotaTicker
	schedules  coordinatorScheduleTicker
	planning   coordinatorPlanningTicker
	admin      coordinatorAdminExecutor
	legacy     coordinatorLegacyTicker
	workers    coordinatorWorkerTicker
	logger     *slog.Logger
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

func (c coordinatorBoundaryCycle) tick(ctx context.Context, exchangeWorkers bool) {
	if c.projection != nil {
		if _, err := backlog.ProjectWorkflowRuns(ctx, c.projection, time.Now().UTC()); err != nil {
			c.logger.Error("workflow run projection failed", "error", err)
		}
	}
	if store, ok := c.projection.(interface {
		SettleNodeWaits(context.Context, time.Time) error
	}); ok {
		if err := store.SettleNodeWaits(ctx, time.Now().UTC()); err != nil {
			c.logger.Error("node wait settlement failed", "error", err)
		}
	}
	quotaHealthy := true
	quotaReport, err := c.quota.Tick(ctx)
	if err != nil {
		quotaHealthy = false
		c.logger.Error("backlog-v2 quota reconciliation failed", "error", err)
	} else if len(quotaReport.Directives) != 0 {
		c.logger.Info("backlog-v2 quota transitions reconciled", "directives", len(quotaReport.Directives))
	}
	if report, err := c.schedules.Tick(ctx); err != nil {
		c.logger.Error("backlog-v2 schedule reconciliation failed", "error", err)
	} else if len(report.Results) != 0 {
		c.logger.Info("backlog-v2 schedule occurrences reconciled", "results", len(report.Results))
	}
	if quotaHealthy {
		if report, err := c.planning.Tick(ctx, quotaReport); err != nil {
			c.logger.Error("backlog-v2 assignment planning failed", "error", err)
		} else if len(report.Assignments) != 0 {
			c.logger.Info("backlog-v2 assignment plans committed", "assignments", len(report.Assignments))
		}
		if report, err := c.admin.ExecutePendingCommands(ctx); err != nil {
			c.logger.Error("backlog-v2 admin command execution failed", "error", err)
		} else if len(report.Decisions) != 0 {
			c.logger.Info("backlog-v2 admin commands executed", "decisions", len(report.Decisions))
		}
	} else {
		c.logger.Warn("backlog-v2 planning and admin command execution deferred until quota reconciliation succeeds")
	}
	report := c.legacy.Tick(ctx)
	for _, err := range report.Errors {
		c.logger.Error("legacy backlog-v2 submission failed", "error", err)
	}
	if len(report.Accepted) != 0 {
		c.logger.Info("legacy backlog-v2 submissions reconciled", "accepted", len(report.Accepted))
	}
	if exchangeWorkers && c.workers != nil {
		workerReport := c.workers.Tick(ctx, quotaReport)
		for _, result := range workerReport.Results {
			if result.Err != nil {
				c.logger.Error("backlog-v2 worker reconciliation failed", "worker", result.WorkerID, "error", result.Err)
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

func runCoordinatorConfiguration(ctx context.Context, cfg config.Config, logger *slog.Logger, store *sqlite.Store, epoch int64, ready func()) error {
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		return err
	}
	service.SetGraphAmendmentSupport(cfg.BacklogV2.Storage.Artifacts, graphTaskValidator(cfg.BacklogV2))
	configurationDigest, err := coordinatorConfigurationDigest(cfg.BacklogV2)
	if err != nil {
		return err
	}
	appliedAt := time.Now().UTC()
	service.SetRuntimeInfo(backlogadmin.RuntimeInfo{
		Release: version, ConfigurationDigest: configurationDigest, LastReload: appliedAt,
		Mode: "coordinator", Owner: cfg.BacklogV2.Coordinator.ID, Epoch: epoch,
		Transport:              cfg.BacklogV2.Transport.Kind,
		MaxWorkerSnapshotAge:   cfg.BacklogV2.Freshness.WorkerMaxAge.D(),
		MaxQuotaObservationAge: cfg.BacklogV2.Freshness.QuotaMaxAge.D(),
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
	submissions := &backlog.SubmissionService{
		StorageRoot: cfg.BacklogV2.Storage.Bundles,
		Store:       store,
		MaxBytes:    cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxFiles:    cfg.BacklogV2.MessageLimits.MaxFiles,
	}
	service.SetWorkerEnrollmentHandler(coordinatorEnrollmentHandler(cfg.BacklogV2, store, epoch, artifactStore))
	scheduleDefinitions := &backlog.ScheduleDefinitionService{Store: store}
	server := backlogadmin.LocalServer{
		Listener: listener,
		Service: coordinatorLocalService{
			admin: service, submissions: submissions, schedules: scheduleDefinitions,
		},
		AllowedUID:         uint32(os.Getuid()),
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
	cycle := coordinatorBoundaryCycle{
		projection: store,
		quota: coordinatorQuotaReconciler{store: store, bridge: backlog.QuotaBridge{
			Store: store, Pools: coordinatorQuotaPoolBindings(cfg),
			MaxObservationAge:       cfg.BacklogV2.Freshness.QuotaMaxAge.D(),
			SafetyMargin:            cfg.Backlog.SafetyMargin,
			FallbackForecastPerHour: cfg.Backlog.FallbackPerHour,
			LongWindowCap:           cfg.Backlog.LongWindowCap,
			SurplusHorizon:          24 * time.Hour,
		}},
		schedules: backlog.ScheduleTimer{Store: store, CatchUpMax: cfg.BacklogV2.Scheduling.CatchUpMax},
		planning: coordinatorPlanner{
			store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch,
			maxWorkerSnapshotAge:   cfg.BacklogV2.Freshness.WorkerMaxAge.D(),
			maxQuotaObservationAge: cfg.BacklogV2.Freshness.QuotaMaxAge.D(),
			deadlineRiskWindow:     24 * time.Hour,
			checkpointMargin:       cfg.BacklogV2.Leases.RenewInterval.D(),
		},
		admin: service,
		legacy: backlog.LegacySubmissionSource{
			Dir: backlogDir, Submitter: submissions, ProjectAliases: aliases,
			MaxBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
			MaxFiles: cfg.BacklogV2.MessageLimits.MaxFiles, AllowedUID: uint32(os.Getuid()),
		},
		workers: workers,
		logger:  logger,
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
