package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

var runtimeTestNow = time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)

type fakeDriver struct {
	workspace       string
	workspaceReady  bool
	observations    []backlog.DispatchThreadState
	observeErr      error
	createErr       error
	stopErr         error
	collectErr      error
	prepareCalls    int
	createCalls     int
	stopCalls       int
	collectCalls    int
	cleanupCalls    int
	checkpointCalls int
	resumeCalls     int
}

func (d *fakeDriver) Prepare(context.Context, workerproto.ExecutionPackage) (string, error) {
	d.prepareCalls++
	d.workspaceReady = true
	return d.workspace, nil
}
func (d *fakeDriver) InspectWorkspace(context.Context, workerproto.ExecutionPackage) (string, bool, error) {
	return d.workspace, d.workspaceReady, nil
}
func (d *fakeDriver) ObserveThread(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	if d.observeErr != nil {
		return "", d.observeErr
	}
	if len(d.observations) == 0 {
		return backlog.DispatchThreadMissing, nil
	}
	result := d.observations[0]
	d.observations = d.observations[1:]
	return result, nil
}
func (d *fakeDriver) CreateThread(context.Context, workerproto.ExecutionPackage, string) error {
	d.createCalls++
	return d.createErr
}
func (d *fakeDriver) StopThread(context.Context, workerproto.ExecutionPackage) error {
	d.stopCalls++
	return d.stopErr
}
func (d *fakeDriver) Collect(context.Context, workerproto.ExecutionPackage, string) error {
	d.collectCalls++
	return d.collectErr
}
func (d *fakeDriver) Cleanup(context.Context, workerproto.ExecutionPackage, string) error {
	d.cleanupCalls++
	return nil
}
func (d *fakeDriver) Warn(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error {
	return nil
}
func (d *fakeDriver) Checkpoint(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) (*domain.CheckpointMetadata, error) {
	d.checkpointCalls++
	data := []byte("checkpoint")
	sum := sha256.Sum256(data)
	return &domain.CheckpointMetadata{ArtifactID: "checkpoint-1", Path: ".t3/checkpoint.md", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data)), CapturedAt: runtimeTestNow}, nil
}
func (d *fakeDriver) Resume(context.Context, workerproto.ExecutionPackage, domain.ThrottleCommand) error {
	d.resumeCalls++
	return nil
}

func TestRuntimeRestartAtDurableCommandBoundaries(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), observations: []backlog.DispatchThreadState{
		backlog.DispatchThreadMissing, backlog.DispatchThreadActive,
	}}
	runtime := newTestRuntime(t, root, driver)
	offer := testOffer(t)
	claims, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}})
	if err != nil || len(claims.Claims) != 1 {
		t.Fatalf("accept offer: claims=%+v err=%v", claims, err)
	}

	runtime = reopenTestRuntime(t, root, driver)
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare-1")
	acks, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{
		Commands: []domain.WorkerCommand{prepare}, Packages: map[string]workerproto.ExecutionPackageManifest{offer.Assignment.ID: offer.Package},
	})
	if err != nil || !acks.Acknowledgements[0].Accepted || driver.prepareCalls != 1 {
		t.Fatalf("prepare: acks=%+v calls=%d err=%v", acks, driver.prepareCalls, err)
	}
	replayed, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}})
	if err != nil || replayed.Acknowledgements[0] != acks.Acknowledgements[0] || driver.prepareCalls != 1 {
		t.Fatalf("prepare replay: acks=%+v calls=%d err=%v", replayed, driver.prepareCalls, err)
	}

	runtime = reopenTestRuntime(t, root, driver)
	dispatch := testCommand(t, runtime, domain.WorkerCommandDispatch, "dispatch-1")
	acks, err = runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{dispatch}})
	if err != nil || !acks.Acknowledgements[0].Accepted || driver.createCalls != 1 {
		t.Fatalf("dispatch: acks=%+v creates=%d err=%v", acks, driver.createCalls, err)
	}

	runtime = reopenTestRuntime(t, root, driver)
	stop := testCommand(t, runtime, domain.WorkerCommandStop, "stop-1")
	acks, err = runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{stop}})
	if err != nil || !acks.Acknowledgements[0].Accepted || driver.stopCalls != 1 {
		t.Fatalf("stop: acks=%+v stops=%d err=%v", acks, driver.stopCalls, err)
	}

	runtime = reopenTestRuntime(t, root, driver)
	collect := testCommand(t, runtime, domain.WorkerCommandCollect, "collect-1")
	acks, err = runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{collect}})
	if err != nil || !acks.Acknowledgements[0].Accepted || driver.collectCalls != 1 || driver.cleanupCalls != 1 {
		t.Fatalf("collect: acks=%+v collect=%d cleanup=%d err=%v", acks, driver.collectCalls, driver.cleanupCalls, err)
	}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil || snapshot.Assignments[0].State != domain.AssignmentCompleted {
		t.Fatalf("final snapshot: %+v err=%v", snapshot, err)
	}
}

func TestAcceptedCreateSurvivesStoppedProjectionLag(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{
		workspace: filepath.Join(root, "workspace"),
		observations: []backlog.DispatchThreadState{
			backlog.DispatchThreadMissing,
			backlog.DispatchThreadStopped,
		},
	}
	runtime := newClaimedRuntime(t, root, driver)
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}}); err != nil {
		t.Fatal(err)
	}
	dispatch := testCommand(t, runtime, domain.WorkerCommandDispatch, "dispatch")
	acks, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{dispatch}})
	if err != nil || !acks.Acknowledgements[0].Accepted {
		t.Fatalf("dispatch = %+v, err = %v", acks, err)
	}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Assignments[0].Control != domain.ControlRunning {
		t.Fatalf("control = %q, want running", snapshot.Assignments[0].Control)
	}
}

func TestLostAndAmbiguousT3ResponseFailsUnknown(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntime(t, root, driver)
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}}); err != nil {
		t.Fatal(err)
	}
	driver.createErr = errors.New("connection lost")
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadMissing, backlog.DispatchThreadMissing}
	dispatch := testCommand(t, runtime, domain.WorkerCommandDispatch, "dispatch")
	acks, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{dispatch}})
	if err != nil {
		t.Fatal(err)
	}
	if acks.Acknowledgements[0].Accepted {
		t.Fatal("ambiguous dispatch was accepted")
	}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Assignments[0].State != domain.AssignmentUnknown {
		t.Fatalf("assignment state = %q", snapshot.Assignments[0].State)
	}
}

func TestChangedWorkerEpochAndCorruptArtifactFailClosed(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newTestRuntime(t, root, driver)
	offer := testOffer(t)
	offer.Package.SHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err == nil {
		t.Fatal("corrupt package accepted")
	}
	if _, err := OpenJournal(root, "normandy", "worker-2", 9); err == nil {
		t.Fatal("changed worker epoch accepted")
	}
}
func TestLeaseLossStopsAndRequiresReconciliation(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntimeWithClock(t, root, driver, func() time.Time { return now })
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if driver.stopCalls != 1 || snapshot.Assignments[0].State != domain.AssignmentUnknown {
		t.Fatalf("stops=%d snapshot=%+v", driver.stopCalls, snapshot)
	}
}

func TestLeaseRenewalCannotExtendBeyondConfiguredInterval(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	runtime := newClaimedRuntimeWithClock(t, root, &fakeDriver{workspace: filepath.Join(root, "workspace")}, func() time.Time { return now })
	now = now.Add(30 * time.Second)
	renewals, err := runtime.LeaseRenewals()
	if err != nil || len(renewals.Renewals) != 1 {
		t.Fatalf("renewals = %+v, err=%v", renewals, err)
	}
	authorized := renewals.Renewals[0]
	overlong := authorized
	overlong.LeaseExpiresAt = overlong.LeaseExpiresAt.Add(time.Second)
	if err := runtime.ApplyLeaseRenewals(workerproto.LeaseRenewals{Renewals: []domain.AssignmentLeaseRenewal{overlong}}); err == nil ||
		!strings.Contains(err.Error(), "authorized live interval") {
		t.Fatalf("overlong renewal error = %v", err)
	}
	if err := runtime.ApplyLeaseRenewals(workerproto.LeaseRenewals{Renewals: []domain.AssignmentLeaseRenewal{authorized}}); err != nil {
		t.Fatal(err)
	}
}

func TestThrottleCheckpointResumeAndReplay(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntime(t, root, driver)
	prepare := testCommand(t, runtime, domain.WorkerCommandPrepare, "prepare")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{prepare}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	drain := testThrottle(runtime, domain.ThrottleCommandDrain, "drain-1")
	acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{drain})
	if err != nil || !acks[0].Accepted || acks[0].Result != domain.ThrottleResultCheckpointed || acks[0].Checkpoint == nil {
		t.Fatalf("drain: %+v err=%v", acks, err)
	}
	replayed, err := reopenTestRuntime(t, root, driver).DeliverThrottle(context.Background(), []domain.ThrottleCommand{drain})
	if err != nil || replayed[0].AcknowledgedAt != acks[0].AcknowledgedAt || driver.checkpointCalls != 1 {
		t.Fatalf("replay: %+v calls=%d err=%v", replayed, driver.checkpointCalls, err)
	}
	resume := testThrottle(runtime, domain.ThrottleCommandResume, "resume-1")
	resume.WorkspacePath = driver.workspace
	acks, err = runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{resume})
	if err != nil || !acks[0].Accepted || driver.resumeCalls != 1 {
		t.Fatalf("resume: %+v calls=%d err=%v", acks, driver.resumeCalls, err)
	}
}

func TestCancellationWholeCgroupDelegatesDurableStop(t *testing.T) {
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace")}
	runtime := newClaimedRuntime(t, root, driver)
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	stop := testCommand(t, runtime, domain.WorkerCommandStop, "stop")
	if _, err := runtime.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{stop}}); err != nil {
		t.Fatal(err)
	}
	if driver.stopCalls != 1 {
		t.Fatalf("stop calls = %d", driver.stopCalls)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.Attempts["assignment-1"].Phase != PhaseStopped {
		t.Fatalf("phase = %q", state.Attempts["assignment-1"].Phase)
	}
}

func TestObservationKeepsPreDispatchPhasesPreparing(t *testing.T) {
	for _, phase := range []Phase{PhaseClaimed, PhasePreparing, PhasePrepared, PhaseDispatching} {
		t.Run(string(phase), func(t *testing.T) {
			got := observation(AttemptRecord{
				Assignment: domain.Assignment{ID: "assignment-1", Epoch: 1},
				Phase:      phase,
			}, runtimeTestNow)
			if got.State != domain.AssignmentClaimed || got.Control != domain.ControlPreparing {
				t.Fatalf("observation = %#v", got)
			}
		})
	}
}

func newClaimedRuntime(t *testing.T, root string, driver *fakeDriver) *Runtime {
	return newClaimedRuntimeWithClock(t, root, driver, func() time.Time { return runtimeTestNow })
}

func newClaimedRuntimeWithClock(t *testing.T, root string, driver *fakeDriver, now func() time.Time) *Runtime {
	t.Helper()
	runtime := newTestRuntimeWithClock(t, root, driver, now)
	offer := testOffer(t)
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func newTestRuntime(t *testing.T, root string, driver *fakeDriver) *Runtime {
	return newTestRuntimeWithClock(t, root, driver, func() time.Time { return runtimeTestNow })
}

func newTestRuntimeWithClock(t *testing.T, root string, driver *fakeDriver, now func() time.Time) *Runtime {
	t.Helper()
	journal, err := OpenJournal(root, "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(testConfig(now), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func reopenTestRuntime(t *testing.T, root string, driver *fakeDriver) *Runtime {
	t.Helper()
	return newTestRuntime(t, root, driver)
}

func testConfig(now func() time.Time) Config {
	return Config{
		WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorID: "coordinator",
		CoordinatorEpoch: 9, SnapshotTTL: time.Minute, LeaseDuration: 2 * time.Minute,
		MaxPackageBytes: 1 << 20, Now: now,
		Inventory: domain.WorkerInventory{ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: runtimeTestNow},
	}
}

func testOffer(t *testing.T) workerproto.AssignmentOffer {
	t.Helper()
	pkg := testPackage()
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	return workerproto.AssignmentOffer{
		Assignment: domain.Assignment{
			ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
			Route: pkg.Route, State: domain.AssignmentOffered, Epoch: 2,
			LeaseToken: "lease-1", LeaseExpiresAt: runtimeTestNow.Add(2 * time.Minute),
			DispatchToken: "dispatch-1", ThreadID: "thread-1", CreatedAt: runtimeTestNow, UpdatedAt: runtimeTestNow,
		},
		Package: manifest, ExpiresAt: runtimeTestNow.Add(time.Minute),
	}
}

func testPackage() workerproto.ExecutionPackage {
	deadline := runtimeTestNow.Add(time.Hour)
	expiry := runtimeTestNow.Add(2 * time.Hour)
	return workerproto.ExecutionPackage{
		Version: 1, ID: "package-1", CoordinatorID: "coordinator", CoordinatorEpoch: 9,
		WorkerID: "normandy", WorkerEpoch: "worker-1",
		Identity: workerproto.ExecutionIdentity{
			WorkflowID: "workflow-1", WorkflowRunID: "run-1", TaskID: "task-1",
			AttemptID: "attempt-1", AssignmentID: "assignment-1", AssignmentEpoch: 2,
			DispatchToken: "dispatch-1", ThreadID: "thread-1",
		},
		Class:  domain.TaskClassRequired,
		Prompt: testArtifact("prompt-1", "prompt/task.md", "prompt"),
		Route:  domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol", QuotaPoolID: "codex-main"},
		Environment: workerproto.EnvironmentReference{
			CatalogRevision: "catalog-1", Project: "steward", Repository: "example.com/steward",
			Ref: "main", Scope: "task", SetupProfile: "go", T3Project: "development",
		},
		Verification: []string{"go test ./..."}, Deadline: &deadline, ExpiresAt: &expiry,
		Limits:    workerproto.ExecutionLimits{MaxTurns: 12, PrepareTimeout: time.Minute, VerificationTimeout: time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20},
		CreatedAt: runtimeTestNow,
	}
}

func testArtifact(id, path, contents string) workerproto.ArtifactObject {
	sum := sha256.Sum256([]byte(contents))
	return workerproto.ArtifactObject{ID: id, Path: path, Kind: "input", MediaType: "text/markdown", Size: int64(len(contents)), SHA256: hex.EncodeToString(sum[:])}
}

func testCommand(t *testing.T, runtime *Runtime, kind domain.WorkerCommandKind, id string) domain.WorkerCommand {
	t.Helper()
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return domain.WorkerCommand{
		ID: id, Kind: kind, WorkerID: "normandy", WorkerEpoch: "worker-1",
		CoordinatorEpoch: 9, AssignmentID: "assignment-1", AssignmentEpoch: 2,
		ExpectedWorkerSequence: state.Sequence, CreatedAt: runtimeTestNow,
	}
}

func testThrottle(runtime *Runtime, kind domain.ThrottleCommandKind, id string) domain.ThrottleCommand {
	state, _ := runtime.journal.snapshot()
	record := state.Attempts["assignment-1"]
	return domain.ThrottleCommand{
		ID: id, DirectiveID: "directive-1", AttemptID: "attempt-1", AssignmentID: "assignment-1",
		AssignmentEpoch: 2, WorkerID: "normandy", ThreadID: "thread-1",
		WorkspacePath: record.WorkspacePath, Route: testPackage().Route, Kind: kind,
		QuotaPoolID: "codex-main", Reason: "quota", CreatedAt: runtimeTestNow,
	}
}
