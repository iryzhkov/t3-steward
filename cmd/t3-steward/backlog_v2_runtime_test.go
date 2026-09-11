package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func setCoordinatorTestRoots(t *testing.T, cfg *config.Config) string {
	t.Helper()
	root := t.TempDir()
	cfg.StatePath = filepath.Join(root, "state.db")
	cfg.Backlog.Dir = filepath.Join(root, "drop")
	cfg.BacklogV2.Storage.Bundles = filepath.Join(root, "bundles")
	cfg.BacklogV2.Storage.Artifacts = filepath.Join(root, "artifacts")
	cfg.BacklogV2.Storage.Workspaces = filepath.Join(root, "workspaces")
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{
		"codex-main": {Provider: "codex", MaxConcurrent: 1},
	}
	cfg.BacklogV2.Workers = map[string]config.V2Worker{
		"normandy": {
			Address: "normandy", Epoch: "worker-1", AcceptBacklog: true, Credential: "test",
			Providers: map[string]config.V2Provider{
				"codex": {Models: []string{"test"}, QuotaPool: "codex-main"},
			},
		},
	}
	cfg.BacklogV2.Projects = map[string]config.V2Project{
		"steward": {
			Repository: "test", DefaultRef: "main", T3Project: "t3-steward development",
			Workers: []string{"normandy"},
		},
	}
	if err := os.MkdirAll(cfg.Backlog.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = filepath.WalkDir(cfg.BacklogV2.Storage.Bundles, func(path string, entry os.DirEntry, _ error) error {
			if entry != nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})
	return root
}

func TestRunBacklogV2DisabledHasNoStartupEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	cfg := config.Default()
	cfg.StatePath = path
	handled, err := runBacklogV2(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("disabled runtime created state: %v", err)
	}
}

func TestRunBacklogV2WorkerModeDoesNotAcquireCoordinatorAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	cfg := config.Default()
	cfg.StatePath = path
	cfg.BacklogV2.Mode = "worker"
	handled, err := runBacklogV2(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("worker runtime created coordinator state: %v", err)
	}
}

func TestRunBacklogV2RefusesLegacyCoordinatorOverlapBeforeStateOpen(t *testing.T) {
	cfg := config.Default()
	root := setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.Backlog.Enabled = true

	handled, err := runBacklogV2(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !handled || err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	if _, err := os.Stat(cfg.StatePath); !os.IsNotExist(err) {
		t.Fatalf("overlap opened state: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "drop" {
			t.Fatalf("overlap created %q", entry.Name())
		}
	}
}

func TestRunBacklogV2CoordinatorStartsClosedAndAdvancesEpoch(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	path := cfg.StatePath
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.CoordinatorEpoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if epoch <= 1 {
		t.Fatalf("coordinator epoch did not advance: %d", epoch)
	}
	admissions, err := store.LoadQuotaAdmissions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(admissions) != 1 || admissions[0].QuotaPoolID != "codex-main" ||
		admissions[0].Admission != domain.AdmissionClosed {
		t.Fatalf("startup admission = %+v", admissions)
	}
}

func TestRunBacklogV2CoordinatorServesAuthenticatedLocalAdmin(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		done <- err
	}()

	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("admin socket was not created: %s", socketPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := backlogadmin.LocalClient{
		Path: socketPath, MaxResponseBytes: int64(cfg.BacklogV2.MessageLimits.MaxBytes),
		MaxArtifactBytes: int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
		RequestTimeout:   cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	response, err := client.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus,
		Principal: backlogadmin.Principal{ID: "spoofed", Roles: []string{"untrusted"}},
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if response.Status == nil || response.Version != backlogadmin.Version {
		cancel()
		t.Fatalf("response = %+v", response)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not stop")
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("admin socket was not removed: %v", err)
	}
}

func TestRunBacklogV2CoordinatorIngestsLegacyDropWithoutDispatch(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"
	cfg.BacklogV2.Projects = map[string]config.V2Project{
		"steward": {T3Project: "t3-steward development"},
	}
	raw := "---\nproject: t3-steward development\ntitle: compatibility\nimportance: 5\ndifficulty: 5\nmax_turns: 3\ngate: false\n---\nlegacy prompt\n"
	if err := os.WriteFile(filepath.Join(cfg.Backlog.Dir, "legacy.md"), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		done <- err
	}()
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	client := backlogadmin.LocalClient{
		Path: socketPath, MaxResponseBytes: int64(cfg.BacklogV2.MessageLimits.MaxBytes),
		MaxArtifactBytes: int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
		RequestTimeout:   cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	deadline := time.Now().Add(2 * time.Second)
	var response backlogadmin.Response
	for {
		response, err = client.Query(context.Background(), backlogadmin.Query{
			Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkflows,
		})
		if err == nil && len(response.Workflows) == 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("legacy submission was not ingested: response=%+v err=%v", response, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	task := response.Workflows[0].Workflow
	if task.Project != "steward" {
		cancel()
		t.Fatalf("workflow project = %q", task.Project)
	}
	if response.Workflows[0].Run.Progress != "queued" {
		cancel()
		t.Fatalf("run progress = %q", response.Workflows[0].Run.Progress)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not stop")
	}
}

func TestRunBacklogV2CoordinatorAcceptsNativeArchiveSubmissionAndReplay(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		done <- err
	}()
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("admin socket was not created: %s", socketPath)
		}
		time.Sleep(10 * time.Millisecond)
	}

	archive := runtimeSubmissionTar(t)
	client := backlogadmin.LocalClient{
		Path: socketPath, MaxResponseBytes: int64(cfg.BacklogV2.MessageLimits.MaxBytes),
		MaxArtifactBytes:   int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
		MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		RequestTimeout:     cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	request := backlogadmin.LocalSubmissionRequest{IdempotencyKey: "native-request"}
	first, err := client.SubmitArchive(context.Background(), request, bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	replay, err := client.SubmitArchive(context.Background(), request, bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if first.Key != request.IdempotencyKey || first.State != "accepted" || first.Replay ||
		replay.WorkflowID != first.WorkflowID || replay.RunID != first.RunID || !replay.Replay {
		cancel()
		t.Fatalf("submission first=%+v replay=%+v", first, replay)
	}
	response, err := client.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkflows,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if len(response.Workflows) != 1 || response.Workflows[0].Run.ID != first.RunID {
		cancel()
		t.Fatalf("submitted workflows = %+v", response.Workflows)
	}

	definitionRequest := backlogadmin.LocalScheduleDefinitionRequest{
		RequestID: "schedule-definition-1", ID: "nightly", Name: "Nightly",
		WorkflowID: first.WorkflowID, Expression: "0 2 * * *", Timezone: "UTC",
		AfterFailure: domain.ScheduleFailureHold, Enabled: false, Reason: "create schedule",
	}
	schedule, err := client.PutSchedule(
		context.Background(),
		backlogadmin.Principal{ID: "spoofed", Roles: []string{"untrusted"}},
		definitionRequest,
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	scheduleReplay, err := client.PutSchedule(context.Background(), backlogadmin.Principal{}, definitionRequest)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if schedule.Schedule.ID != "nightly" || schedule.Schedule.Revision != 1 || schedule.Replay ||
		scheduleReplay.Schedule != schedule.Schedule || !scheduleReplay.Replay {
		cancel()
		t.Fatalf("schedule first=%+v replay=%+v", schedule, scheduleReplay)
	}
	schedules, err := client.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QuerySchedules,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if len(schedules.Schedules) != 1 || schedules.Schedules[0].Schedule.ID != "nightly" {
		cancel()
		t.Fatalf("schedules = %+v", schedules.Schedules)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not stop")
	}
}

func TestRunBacklogV2CoordinatorReconcilesSchedulesAndAdminCommands(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"
	cfg.BacklogV2.Scheduling.Interval = config.Duration(10 * time.Millisecond)

	created := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	schedule := domain.Schedule{
		ID: "schedule-minute", Name: "minute", Version: 1, WorkflowID: "workflow-1",
		Expression: "* * * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
		Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureNextCycle,
		Enabled: true, Revision: 1, CreatedAt: created, UpdatedAt: created,
	}
	seed, err := sqlite.OpenMigrated(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: backlog.ManifestVersion, Name: "scheduled",
			Class: domain.TaskClassRequired, CreatedAt: created,
		}},
		Schedules: []domain.Schedule{schedule},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: schedule.ID, Version: 1, WorkflowID: schedule.WorkflowID,
			Expression: schedule.Expression, Timezone: schedule.Timezone,
			Overlap: schedule.Overlap, Misfire: schedule.Misfire,
			AfterFailure: schedule.AfterFailure, CreatedAt: created,
		}},
	}); err != nil {
		seed.Close()
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		done <- err
	}()
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("admin socket was not created: %s", socketPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
	client := backlogadmin.LocalClient{
		Path: socketPath, MaxResponseBytes: int64(cfg.BacklogV2.MessageLimits.MaxBytes),
		MaxArtifactBytes: int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
		RequestTimeout:   cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	var schedules backlogadmin.Response
	deadline = time.Now().Add(5 * time.Second)
	for {
		schedules, err = client.Query(context.Background(), backlogadmin.Query{
			Version: backlogadmin.Version, Kind: backlogadmin.QuerySchedules,
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if len(schedules.Schedules) == 1 && schedules.Schedules[0].Schedule.Revision > schedule.Revision {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("schedule occurrence did not reconcile: %+v", schedules.Schedules)
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := client.Mutate(context.Background(), backlogadmin.Mutation{
		Version: backlogadmin.Version, ID: "disable-minute",
		Kind: domain.AdminCommandDisable, ScheduleID: schedule.ID,
		ExpectedRevision: schedules.Schedules[0].Schedule.Revision, Reason: "runtime lifecycle test",
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if response.Command.State != domain.AdminCommandPending {
		cancel()
		t.Fatalf("initial command state = %q", response.Command.State)
	}

	reader, err := sqlite.Open(cfg.StatePath)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer reader.Close()
	deadline = time.Now().Add(5 * time.Second)
	for {
		records, err := reader.LoadCoordinatorRecords(context.Background())
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if len(records.Triggers) >= 1 &&
			len(records.AdminCommands) == 1 &&
			records.AdminCommands[0].State == domain.AdminCommandApplied &&
			len(records.Schedules) == 1 && !records.Schedules[0].Enabled {
			if len(records.Assignments) != 0 {
				cancel()
				t.Fatalf("local lifecycle dispatched assignments: %+v", records.Assignments)
			}
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("local lifecycle did not converge: %+v", records)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not stop")
	}
}

type failingCoordinatorQuotaTicker struct {
	calls *int
}

func (t failingCoordinatorQuotaTicker) Tick(context.Context) (backlog.QuotaBridgeReport, error) {
	*t.calls++
	return backlog.QuotaBridgeReport{}, errors.New("quota unavailable")
}

type recordingCoordinatorScheduleTicker struct {
	calls *int
}

func (t recordingCoordinatorScheduleTicker) Tick(context.Context) (backlog.ScheduleTickReport, error) {
	*t.calls++
	return backlog.ScheduleTickReport{}, nil
}

type recordingCoordinatorPlanningTicker struct {
	calls *int
}

func (t recordingCoordinatorPlanningTicker) Tick(context.Context, backlog.QuotaBridgeReport) (backlog.AssignmentPlanningReport, error) {
	*t.calls++
	return backlog.AssignmentPlanningReport{}, nil
}

type recordingCoordinatorAdminExecutor struct {
	calls *int
}

func (e recordingCoordinatorAdminExecutor) ExecutePendingCommands(context.Context) (backlogadmin.CommandExecutionReport, error) {
	*e.calls++
	return backlogadmin.CommandExecutionReport{}, nil
}

type recordingCoordinatorLegacyTicker struct {
	calls *int
}

func (t recordingCoordinatorLegacyTicker) Tick(context.Context) backlog.LegacySubmissionReport {
	*t.calls++
	return backlog.LegacySubmissionReport{}
}

type recordingCoordinatorWorkerTicker struct {
	calls   *int
	reports *[]backlog.QuotaBridgeReport
}

func (t recordingCoordinatorWorkerTicker) Tick(_ context.Context, report backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	*t.calls++
	*t.reports = append(*t.reports, report)
	return coordinatorWorkerTickReport{}
}

func TestCoordinatorQuotaReconcilerPersistsClosedAdmissionWithoutEvidence(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	store, err := sqlite.OpenMigrated(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reconciler := coordinatorQuotaReconciler{
		store: store,
		bridge: backlog.QuotaBridge{
			Store: store, Pools: coordinatorQuotaPoolBindings(cfg),
			MaxObservationAge: cfg.BacklogV2.Freshness.QuotaMaxAge.D(),
		},
	}
	report, err := reconciler.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Derived) != 1 || report.Derived[0].Admission != domain.AdmissionClosed {
		t.Fatalf("report = %#v", report)
	}
	admissions, err := store.LoadQuotaAdmissions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(admissions) != 1 || admissions[0].Admission != domain.AdmissionClosed {
		t.Fatalf("admissions = %#v", admissions)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.QuotaPools) != 1 ||
		records.QuotaPools[0].ID != report.Pools[0].ID ||
		records.QuotaPools[0].Provider != report.Pools[0].Provider ||
		!slices.Equal(records.QuotaPools[0].ProviderInstanceIDs, report.Pools[0].ProviderInstanceIDs) ||
		records.QuotaPools[0].MaxConcurrent != report.Pools[0].MaxConcurrent ||
		records.QuotaPools[0].Admission != domain.AdmissionClosed {
		t.Fatalf("durable quota pools = %#v, report = %#v", records.QuotaPools, report.Pools)
	}
}
func TestCoordinatorPlannerCommitsOneOfferedAssignmentWithoutDispatch(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	cost := 9.0
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow", Version: 1, Name: "workflow", Project: "project",
			Class: domain.TaskClassRequired, TaskIDs: []string{"task"}, CreatedAt: now,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run", WorkflowID: "workflow", Progress: domain.ProgressActive,
			Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		Tasks: []domain.Task{{
			ID: "task", WorkflowID: "workflow", Name: "task", Class: domain.TaskClassRequired,
			Routes: []domain.ProviderRoute{{
				ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool",
			}},
			Importance: 5, Difficulty: 3, EstimatedCost: &cost, MaxTurns: 2,
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt", WorkflowRunID: "run", TaskID: "task", Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: now,
		}},
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	snapshot := domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch", CoordinatorEpoch: epoch,
		Sequence: 1, Connected: true,
		Inventory: domain.WorkerInventory{
			ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Projects: []domain.WorkerProjectInventory{{
				Name: "project", Available: true, UpdatedAt: now,
			}},
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "codex", Models: []string{"gpt"}, QuotaPoolID: "pool", Available: true,
			}},
			ObservedAt: now,
		},
		ObservedAt: now, ValidUntil: now.Add(time.Hour),
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	quota := backlog.QuotaBridgeReport{
		Pools: []domain.QuotaPool{{
			ID: "pool", Provider: "openai", ProviderInstanceIDs: []string{"codex"},
			Admission: domain.AdmissionOpen, MaxConcurrent: 2,
		}},
		Windows: []backlog.QuotaWindowBudget{{
			QuotaPoolID: "pool", WindowID: "primary", ObservedAt: now,
			Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: now.Add(time.Hour),
		}},
	}
	planner := coordinatorPlanner{
		store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: 5 * time.Minute,
		deadlineRiskWindow: time.Hour, checkpointMargin: 5 * time.Minute,
		now: func() time.Time { return now },
	}
	report, err := planner.Tick(ctx, quota)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Assignments) != 1 || report.Assignments[0].State != domain.AssignmentOffered ||
		report.Assignments[0].Estimate == nil || report.Assignments[0].Estimate.RemainingCost != cost {
		t.Fatalf("planning report = %#v", report)
	}
	replay, err := planner.Tick(ctx, quota)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Assignments) != 0 || len(replay.Plan.Proposals) != 0 {
		t.Fatalf("repeated planning report = %#v", replay)
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Assignments) != 1 || loaded.Attempts[0].AssignmentID != loaded.Assignments[0].ID ||
		loaded.Assignments[0].DispatchState != "" {
		t.Fatalf("durable planning state = %#v", loaded)
	}
}

func TestCoordinatorBoundaryCycleDefersAdminWhenQuotaReconciliationFails(t *testing.T) {
	var quotaCalls, scheduleCalls, planningCalls, adminCalls, legacyCalls, workerCalls int
	var workerReports []backlog.QuotaBridgeReport
	var logs bytes.Buffer
	cycle := coordinatorBoundaryCycle{
		quota:     failingCoordinatorQuotaTicker{calls: &quotaCalls},
		schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
		planning:  recordingCoordinatorPlanningTicker{calls: &planningCalls},
		admin:     recordingCoordinatorAdminExecutor{calls: &adminCalls},
		legacy:    recordingCoordinatorLegacyTicker{calls: &legacyCalls},
		workers:   recordingCoordinatorWorkerTicker{calls: &workerCalls, reports: &workerReports},
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	cycle.Tick(context.Background())
	if quotaCalls != 1 || scheduleCalls != 1 || planningCalls != 0 || adminCalls != 0 || legacyCalls != 1 || workerCalls != 0 {
		t.Fatalf("startup calls quota=%d schedule=%d planning=%d admin=%d legacy=%d workers=%d",
			quotaCalls, scheduleCalls, planningCalls, adminCalls, legacyCalls, workerCalls)
	}
	cycle.TickWithWorkers(context.Background())
	if workerCalls != 1 || len(workerReports) != 1 || len(workerReports[0].Derived) != 0 {
		t.Fatalf("scheduled worker calls=%d reports=%#v", workerCalls, workerReports)
	}
	if !strings.Contains(logs.String(), "admin command execution deferred") {
		t.Fatalf("logs = %q", logs.String())
	}
}

func TestCoordinatorQuotaPoolBindingsAreDeterministicAndDeduplicated(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{
		"zeta":  {Provider: "claude", MaxConcurrent: 1},
		"alpha": {Provider: "codex", MaxConcurrent: 3},
	}
	cfg.BacklogV2.Workers = map[string]config.V2Worker{
		"b": {Providers: map[string]config.V2Provider{
			"shared": {QuotaPool: "alpha"}, "claude": {QuotaPool: "zeta"},
		}},
		"a": {Providers: map[string]config.V2Provider{
			"shared": {QuotaPool: "alpha"}, "other": {QuotaPool: "alpha"},
		}},
	}
	got := coordinatorQuotaPoolBindings(cfg)
	if len(got) != 2 || got[0].ID != "alpha" || got[1].ID != "zeta" ||
		got[0].MaxConcurrent != 3 ||
		!slices.Equal(got[0].ProviderInstanceIDs, []string{"other", "shared"}) {
		t.Fatalf("bindings = %#v", got)
	}
}

func runtimeSubmissionTar(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	files := map[string]string{
		"workflow.yaml":      "version: 2\nname: native\nenvironment: {project: steward}\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n",
		"prompts/inspect.md": "inspect the repository\n",
	}
	for _, name := range []string{"workflow.yaml", "prompts/inspect.md"} {
		raw := []byte(files[name])
		if err := writer.WriteHeader(&tar.Header{
			Name: name, Mode: 0o600, Size: int64(len(raw)), Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
