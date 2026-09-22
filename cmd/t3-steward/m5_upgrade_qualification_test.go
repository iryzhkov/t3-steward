package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

type m5RuntimeDriver struct{}

func (m5RuntimeDriver) Prepare(context.Context, workerproto.ExecutionPackage) (string, error) {
	return "/tmp/m5-workspace", nil
}
func (m5RuntimeDriver) InspectWorkspace(context.Context, workerproto.ExecutionPackage) (string, bool, error) {
	return "", true, nil
}
func (m5RuntimeDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	return backlog.DispatchThreadActive, nil
}
func (m5RuntimeDriver) CreateThread(context.Context, workerproto.ExecutionPackage, string) error {
	return nil
}
func (m5RuntimeDriver) StopThread(context.Context, workerproto.ExecutionPackage) error { return nil }
func (m5RuntimeDriver) Collect(context.Context, workerproto.ExecutionPackage, string) error {
	return nil
}
func (m5RuntimeDriver) CollectFailure(context.Context, workerproto.ExecutionPackage, string, string) error {
	return nil
}
func (m5RuntimeDriver) Settle(context.Context, workerproto.ExecutionPackage) error { return nil }
func (m5RuntimeDriver) Cleanup(context.Context, workerproto.ExecutionPackage, string) error {
	return nil
}
func (m5RuntimeDriver) Warn(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error {
	return nil
}
func (m5RuntimeDriver) Checkpoint(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) (*domain.CheckpointMetadata, error) {
	return nil, nil
}
func (m5RuntimeDriver) Resume(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error {
	return nil
}

type m5NoopTransport struct{}

func (m5NoopTransport) DeliverWorkerCommands(context.Context, domain.WorkerSnapshot, []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error) {
	return nil, nil
}

// The supported upgrade path drives the production activation loop and worker
// reconciliation while every path remains isolated under t.TempDir.
func TestM5SupportedIsolatedUpgradeDrainsSettlesAndResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture := newReloadFixture(t)
	workerID := qualificationWorkerID()
	connectedWorker := fixture.cfg.BacklogV2.Workers[workerID]
	connectedWorker.Connection = "persistent-ssh"
	fixture.cfg.BacklogV2.Workers[workerID] = connectedWorker
	fixture.cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 16 << 20
	fixture.cfg.BacklogV2.Transport.RequestTimeout = config.Duration(time.Minute)
	fixture.cfg.BacklogV2.Freshness.WorkerMaxAge = config.Duration(time.Minute)
	writeReloadConfig(t, fixture.cfg.Path, fixture.cfg)
	epoch, err := fixture.store.AcquireCoordinator(ctx, fixture.cfg.BacklogV2.Coordinator.ID)
	if err != nil {
		t.Fatal(err)
	}
	assignment := fixture.retainAssignment(t, workerID)
	runtime := m5ClaimedRuntime(t, fixture.cfg, assignment, epoch)

	reloads := make(chan os.Signal, 1)
	activated := make(chan config.Config, 8)
	var mu sync.Mutex
	var starts []config.Config
	runner := func(runCtx context.Context, active config.Config, ready func()) error {
		mu.Lock()
		starts = append(starts, active)
		mu.Unlock()
		activated <- active
		ready()
		<-runCtx.Done()
		return nil
	}
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- coordinatorConfigurationActivationLoop(ctx, fixture.cfg, fixture.logger, fixture.store, epoch, fixture.receipts, reloads, runner)
	}()
	initialConfig := receiveM5Activation(t, activated)
	initial := m5Binding(t, initialConfig, workerID)
	if !initial.Inventory.AcceptBacklog {
		t.Fatal("fixture must begin with admission open")
	}

	drainedConfig := cloneReloadConfig(t, initialConfig)
	drainedWorker := drainedConfig.BacklogV2.Workers[workerID]
	drainedWorker.AcceptBacklog = false
	drainedConfig.BacklogV2.Workers[workerID] = drainedWorker
	writeReloadConfig(t, fixture.cfg.Path, drainedConfig)
	reloads <- syscallSIGHUP()
	drainedActive := receiveM5Activation(t, activated)
	drained := m5Binding(t, drainedActive, workerID)
	if drained.Inventory.AcceptBacklog || drained.CatalogRevision != initial.CatalogRevision {
		t.Fatalf("drain replaced execution identity or left admission open: before=%+v after=%+v", initial, drained)
	}

	upgradedConfig := cloneReloadConfig(t, drainedActive)
	project := upgradedConfig.BacklogV2.Projects["steward"]
	project.DefaultRef = "release-candidate"
	upgradedConfig.BacklogV2.Projects["steward"] = project
	writeReloadConfig(t, fixture.cfg.Path, upgradedConfig)
	reloads <- syscallSIGHUP()
	blocked := waitM5Receipt(t, fixture, backlogadmin.ReloadRejected)
	if len(blocked.Blockers) != 1 || blocked.Blockers[0].AssignmentID != assignment.ID {
		t.Fatalf("catalog upgrade did not remain blocked by retained custody: %+v", blocked)
	}
	select {
	case unexpected := <-activated:
		t.Fatalf("rejected reload activated %+v", unexpected.BacklogV2.Projects["steward"])
	case <-time.After(20 * time.Millisecond):
	}

	commands := []domain.WorkerCommand{
		{ID: "prepare-upgrade-custody", Kind: domain.WorkerCommandPrepare},
		{ID: "dispatch-upgrade-custody", Kind: domain.WorkerCommandDispatch},
		{ID: "stop-upgrade-custody", Kind: domain.WorkerCommandStop},
	}
	for index := range commands {
		commands[index].WorkerID = workerID
		commands[index].WorkerEpoch = assignment.WorkerEpoch
		commands[index].CoordinatorEpoch = epoch
		commands[index].AssignmentID = assignment.ID
		commands[index].AssignmentEpoch = assignment.Epoch
		commands[index].CreatedAt = time.Now().UTC()
	}
	if acks, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: commands}); err != nil || len(acks.Acknowledgements) != len(commands) {
		t.Fatalf("worker lifecycle acknowledgements=%+v err=%v", acks, err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Assignments) != 1 || snapshot.Assignments[0].State != domain.AssignmentReleased {
		t.Fatalf("production runtime did not observe released custody: %+v", snapshot.Assignments)
	}
	if err := fixture.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	report, err := (backlog.FleetCoordinator{Store: fixture.store, Now: time.Now}).ReconcileWorkerCommands(ctx, snapshot, m5NoopTransport{})
	if err != nil || len(report.Reconciled) != 1 || report.Reconciled[0].State != domain.AssignmentReleased {
		t.Fatalf("production reconciliation did not commit release: report=%+v err=%v", report, err)
	}
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var detached domain.Attempt
	for _, attempt := range records.Attempts {
		if attempt.ID == assignment.AttemptID {
			detached = attempt
		}
	}
	if detached.AssignmentID != "" || detached.Control != domain.ControlUnassigned {
		t.Fatalf("settled attempt retained execution custody: %+v", detached)
	}

	reloads <- syscallSIGHUP()
	upgradedActive := receiveM5Activation(t, activated)
	upgraded := m5Binding(t, upgradedActive, workerID)
	if upgraded.Inventory.AcceptBacklog || upgraded.CatalogRevision == drained.CatalogRevision {
		t.Fatalf("upgrade did not retain drain and replace catalog: drained=%+v upgraded=%+v", drained, upgraded)
	}
	acceptedUpgrade := waitM5Receipt(t, fixture, backlogadmin.ReloadAccepted)
	upgradeDigest, err := coordinatorConfigurationDigest(upgradedActive.BacklogV2)
	if err != nil || acceptedUpgrade.ConfigurationDigest != upgradeDigest {
		t.Fatalf("accepted upgrade receipt=%+v digest=%q err=%v", acceptedUpgrade, upgradeDigest, err)
	}

	resumedConfig := cloneReloadConfig(t, upgradedActive)
	resumedWorker := resumedConfig.BacklogV2.Workers[workerID]
	resumedWorker.AcceptBacklog = true
	resumedConfig.BacklogV2.Workers[workerID] = resumedWorker
	writeReloadConfig(t, fixture.cfg.Path, resumedConfig)
	reloads <- syscallSIGHUP()
	resumedActive := receiveM5Activation(t, activated)
	resumed := m5Binding(t, resumedActive, workerID)
	if !resumed.Inventory.AcceptBacklog || resumed.CatalogRevision != upgraded.CatalogRevision {
		t.Fatalf("resume replaced catalog/execution epoch or left admission closed: upgraded=%+v resumed=%+v", upgraded, resumed)
	}
	waitM5Receipt(t, fixture, backlogadmin.ReloadAccepted)

	restarted, err := config.LoadFile(fixture.cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	restartedBinding := m5Binding(t, restarted, workerID)
	restartedReceipt, err := readReloadReceipt(fixture.receipts.path)
	if err != nil || restartedReceipt.Outcome != backlogadmin.ReloadAccepted ||
		restartedBinding.CatalogRevision != resumed.CatalogRevision ||
		restartedReceipt.ConfigurationDigest != mustM5Digest(t, restarted) {
		t.Fatalf("restart did not recover accepted effective config/receipt: binding=%+v receipt=%+v err=%v", restartedBinding, restartedReceipt, err)
	}

	cancel()
	if err := <-loopDone; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(starts) != 4 {
		t.Fatalf("activation starts=%d, want initial+drain+upgrade+resume", len(starts))
	}
}

func syscallSIGHUP() os.Signal { return os.Signal(syscall.SIGHUP) }

func receiveM5Activation(t *testing.T, active <-chan config.Config) config.Config {
	t.Helper()
	select {
	case cfg := <-active:
		return cfg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for configuration activation")
		return config.Config{}
	}
}

func waitM5Receipt(t *testing.T, fixture *reloadFixture, outcome string) backlogadmin.ReloadReceipt {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		receipt, err := readReloadReceipt(fixture.receipts.path)
		if err == nil && receipt.Outcome == outcome {
			return receipt
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s reload receipt", outcome)
	return backlogadmin.ReloadReceipt{}
}

func m5Binding(t *testing.T, cfg config.Config, workerID string) workerruntime.WorkerBinding {
	t.Helper()
	binding, err := workerruntime.BuildWorkerBinding(cfg.BacklogV2, workerID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func mustM5Digest(t *testing.T, cfg config.Config) string {
	t.Helper()
	digest, err := coordinatorConfigurationDigest(cfg.BacklogV2)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func m5ClaimedRuntime(t *testing.T, cfg config.Config, assignment domain.Assignment, epoch int64) *workerruntime.Runtime {
	t.Helper()
	now := time.Now().UTC()
	binding := m5Binding(t, cfg, assignment.WorkerID)
	journal, err := workerruntime.OpenJournal(filepath.Join(t.TempDir(), "journal"), assignment.WorkerID, assignment.WorkerEpoch, epoch)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := workerruntime.New(workerruntime.Config{
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		CoordinatorID: cfg.BacklogV2.Coordinator.ID, CoordinatorEpoch: epoch,
		SnapshotTTL: time.Minute, LeaseDuration: time.Hour, MaxPackageBytes: 1 << 20,
		Inventory: binding.Inventory, Now: func() time.Time { return time.Now().UTC() },
		Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}, journal, m5RuntimeDriver{})
	if err != nil {
		t.Fatal(err)
	}
	pkg := workerproto.ExecutionPackage{
		Version: workerproto.ExecutionPackageVersion, ID: "package-upgrade",
		CoordinatorID: cfg.BacklogV2.Coordinator.ID, CoordinatorEpoch: epoch,
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		Identity: workerproto.ExecutionIdentity{
			WorkflowID: "workflow-1", WorkflowRunID: "run-1", TaskID: "implement",
			AttemptID: assignment.AttemptID, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			DispatchToken: "dispatch-upgrade", ThreadID: "thread-upgrade",
		},
		Class:  domain.TaskClassRequired,
		Prompt: workerproto.ArtifactObject{ID: "prompt-upgrade", Path: "prompt/task.md", Kind: "prompt", MediaType: "text/markdown", Size: 1, SHA256: strings.Repeat("a", 64)},
		Route:  domain.ProviderRoute{WorkerID: assignment.WorkerID, ProviderInstanceID: "codex", Model: "model", QuotaPoolID: "pool"},
		Environment: workerproto.EnvironmentReference{
			Type: "fresh", CatalogRevision: binding.CatalogRevision, Project: "steward",
			Scope: "task", SetupProfile: "default",
		},
		Limits:    workerproto.ExecutionLimits{MaxTurns: 1, PrepareTimeout: time.Minute, VerificationTimeout: time.Minute, MaxArtifactBytes: 1024, MaxTotalBytes: 1024},
		CreatedAt: now,
	}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	offered := assignment
	offered.State = domain.AssignmentOffered
	offered.LeaseToken = "lease-upgrade"
	offered.DispatchToken = pkg.Identity.DispatchToken
	offered.ThreadID = pkg.Identity.ThreadID
	offered.LeaseExpiresAt = now.Add(time.Hour)
	if claims, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{{
		Assignment: offered, Package: manifest, ExpiresAt: now.Add(time.Hour),
	}}}); err != nil || len(claims.Claims) != 1 {
		t.Fatalf("claim runtime assignment: claims=%+v err=%v", claims, err)
	}
	return runtime
}
