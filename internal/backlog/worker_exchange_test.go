package backlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestFleetCoordinatorReconcilesOfferClaimAndDurablePrepare(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	task := testTask("alpha")
	task.Routes = []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"}}
	input := plannerInput([]domain.Task{task}, nil)
	input.Now = coordinatorTestTime
	input.Workflows[0].State.Run.CreatedAt = coordinatorTestTime
	input.Workflows[0].State.Run.UpdatedAt = coordinatorTestTime
	input.Workflows[0].State.Attempts[0].UpdatedAt = coordinatorTestTime
	input.QuotaPools = []domain.QuotaPool{routingPool("pool", 2, 0, "codex")}
	input.RouteEstimates = []RouteEstimate{routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10)}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{input.Workflows[0].Workflow}, WorkflowRuns: []domain.WorkflowRun{input.Workflows[0].State.Run},
		Tasks: []domain.Task{task}, Attempts: input.Workflows[0].State.Attempts, QuotaPools: input.QuotaPools,
	}); err != nil {
		t.Fatal(err)
	}
	first := coordinatorSnapshot(1)
	if err := store.SaveWorkerSnapshot(ctx, first); err != nil {
		t.Fatal(err)
	}
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime }}
	planned, err := coordinator.PlanAndCommit(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Assignments) != 1 || planned.Assignments[0].ThreadID == "" {
		t.Fatalf("planned assignments = %+v", planned.Assignments)
	}

	transport := &exchangeTransport{snapshots: []domain.WorkerSnapshot{first, {
		WorkerID: first.WorkerID, WorkerEpoch: first.WorkerEpoch, CoordinatorEpoch: first.CoordinatorEpoch,
		Sequence: 2, Connected: true, Inventory: first.Inventory,
		Assignments: []domain.WorkerAssignmentObservation{{
			AssignmentID: planned.Assignments[0].ID, AssignmentEpoch: planned.Assignments[0].Epoch,
			State: domain.AssignmentClaimed, Control: domain.ControlPreparing, ThreadID: planned.Assignments[0].ThreadID,
			ObservedAt: coordinatorTestTime.Add(time.Second),
		}},
		ObservedAt: coordinatorTestTime.Add(time.Second), ValidUntil: coordinatorTestTime.Add(time.Minute),
	}}}
	report, err := coordinator.ReconcileWorker(
		ctx, transport, testOfferBuilder{}, WorkerAdmissionPolicyFromQuotaReport(QuotaBridgeReport{Derived: []QuotaPoolAdmissionSnapshot{{
			QuotaPoolID: "pool", Admission: domain.AdmissionOpen,
		}}}), nil, input.QuotaPools, time.Minute, time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Offered) != 1 || len(report.Claimed) != 1 ||
		len(report.Delivery.Pending) != 1 || report.Delivery.Pending[0].Kind != domain.WorkerCommandPrepare ||
		len(report.Delivery.Acknowledgements) != 1 {
		t.Fatalf("exchange report = %+v", report)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 || records.Assignments[0].State != domain.AssignmentClaimed ||
		!records.Assignments[0].LeaseExpiresAt.Equal(coordinatorTestTime.Add(time.Hour)) {
		t.Fatalf("durable assignments = %+v", records.Assignments)
	}
}

func TestLeaseRenewalsForWorkerRequiresLiveDurableAndObservedClaim(t *testing.T) {
	snapshot := coordinatorSnapshot(7)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed,
		ObservedAt: coordinatorTestTime, WorkspacePath: "/tmp/work",
	}}
	assignment := domain.Assignment{
		ID: "assignment-1", Epoch: 1, State: domain.AssignmentClaimed,
		WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		LeaseToken: "lease-1", LeaseExpiresAt: coordinatorTestTime.Add(time.Minute),
	}
	renewals := leaseRenewalsForWorker([]domain.Assignment{assignment}, snapshot, coordinatorTestTime, time.Hour)
	if len(renewals) != 1 || renewals[0].WorkerSequence != snapshot.Sequence ||
		!renewals[0].LeaseExpiresAt.Equal(coordinatorTestTime.Add(time.Hour)) {
		t.Fatalf("renewals = %#v", renewals)
	}
	assignment.State = domain.AssignmentUnknown
	if got := leaseRenewalsForWorker([]domain.Assignment{assignment}, snapshot, coordinatorTestTime, time.Hour); len(got) != 0 {
		t.Fatalf("unknown assignment renewals = %#v", got)
	}
	assignment.State = domain.AssignmentClaimed
	assignment.LeaseExpiresAt = coordinatorTestTime
	if got := leaseRenewalsForWorker([]domain.Assignment{assignment}, snapshot, coordinatorTestTime, time.Hour); len(got) != 0 {
		t.Fatalf("expired assignment renewals = %#v", got)
	}
}

func TestFleetCoordinatorRenewsLeaseAndDeliversDurableThrottle(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot := coordinatorSnapshot(4)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed,
		Control: domain.ControlRunning, ThreadID: "thread-1", WorkspacePath: "/tmp/work",
		ObservedAt: coordinatorTestTime,
	}}
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", AssignmentID: "assignment-1",
		Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, UpdatedAt: coordinatorTestTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		Route: domain.ProviderRoute{WorkerID: snapshot.WorkerID, ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"},
		State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease-1", DispatchToken: "dispatch-1", ThreadID: "thread-1",
		LeaseExpiresAt: coordinatorTestTime.Add(10 * time.Minute), CreatedAt: coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	renewedSnapshot := snapshot
	renewedSnapshot.Sequence++
	renewedSnapshot.ObservedAt = coordinatorTestTime.Add(time.Second)
	renewedSnapshot.ValidUntil = coordinatorTestTime.Add(time.Hour)
	renewedSnapshot.Assignments[0].ObservedAt = renewedSnapshot.ObservedAt
	transport := &exchangeTransport{snapshots: []domain.WorkerSnapshot{snapshot}, renewalSnapshot: &renewedSnapshot}
	directive := domain.ThrottleDirective{
		ID: "directive-1", QuotaPoolID: "pool", AdmissionRevision: 1,
		Severity: domain.ThrottleWarn, Reason: "quota warning", CreatedAt: coordinatorTestTime,
	}
	report, err := (FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime }}).ReconcileWorker(
		ctx, transport, testOfferBuilder{}, WorkerAdmissionPolicy{}, []domain.ThrottleDirective{directive},
		[]domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionOpen, MaxConcurrent: 1}}, time.Minute, time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Renewed) != 1 || len(transport.renewals) != 1 ||
		!report.Renewed[0].LeaseExpiresAt.Equal(coordinatorTestTime.Add(time.Hour)) {
		t.Fatalf("renewal report = %#v renewals=%#v", report.Renewed, transport.renewals)
	}
	if len(transport.throttles) != 1 || transport.throttles[0].Kind != domain.ThrottleCommandWarn {
		t.Fatalf("throttle commands = %#v", transport.throttles)
	}
	records, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Delivery != domain.ThrottleDeliveryAcknowledged || records[0].Result != domain.ThrottleResultWarned {
		t.Fatalf("throttle records = %#v", records)
	}
}

func TestFleetCoordinatorSkipsAutomaticThrottleForForcedAttempt(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	snapshot := coordinatorSnapshot(4)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: "assignment-forced", AssignmentEpoch: 1, State: domain.AssignmentClaimed,
		Control: domain.ControlPreparing, WorkspacePath: "/tmp/work", ObservedAt: coordinatorTestTime,
	}}
	attempt := domain.Attempt{
		ID: "attempt-forced", WorkflowRunID: "run-1", TaskID: "task-1", AssignmentID: "assignment-forced",
		Progress: domain.ProgressActive, Control: domain.ControlPreparing, Revision: 1,
		AdminForceStart: true, UpdatedAt: coordinatorTestTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-forced", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		Route: domain.ProviderRoute{WorkerID: snapshot.WorkerID, ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"},
		State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease-1", DispatchToken: "dispatch-1",
		LeaseExpiresAt: coordinatorTestTime.Add(10 * time.Minute), CreatedAt: coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	directive := domain.ThrottleDirective{
		ID: "directive-closed", QuotaPoolID: "pool", AdmissionRevision: 1,
		Severity: domain.ThrottleStop, Reason: "quota closed", CreatedAt: coordinatorTestTime,
	}
	transport := &exchangeTransport{}
	report := WorkerExchangeReport{}
	err = (FleetCoordinator{}).reconcileWorkerThrottle(
		ctx, store, transport, snapshot, []domain.ThrottleDirective{directive},
		[]domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionClosed, MaxConcurrent: 1}},
		coordinatorTestTime, &report,
	)
	if err != nil {
		t.Fatalf("forced attempt throttle reconciliation: %v", err)
	}
	if len(transport.throttles) != 0 {
		t.Fatalf("forced attempt received automatic throttle: %#v", transport.throttles)
	}
	records, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("forced attempt throttle records = %#v", records)
	}
}

func TestFleetCoordinatorIgnoresHistoricalReleasedAssignmentForThrottle(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	snapshot := coordinatorSnapshot(4)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: "assignment-released", AssignmentEpoch: 5, State: domain.AssignmentUnknown,
		Control: domain.ControlStopped, WorkspacePath: "/tmp/historical", ObservedAt: coordinatorTestTime,
	}}
	attempt := domain.Attempt{
		ID: "attempt-terminal", WorkflowRunID: "run-1", TaskID: "task-1",
		Progress: domain.ProgressSkipped, Control: domain.ControlStopped, Revision: 11,
		UpdatedAt: coordinatorTestTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-released", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch,
		Route:       domain.ProviderRoute{WorkerID: snapshot.WorkerID, ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"},
		State:       domain.AssignmentReleased, Epoch: 5, DispatchToken: "dispatch-old",
		ThreadID: "thread-old", CreatedAt: coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	directive := domain.ThrottleDirective{
		ID: "directive-closed", QuotaPoolID: "pool", AdmissionRevision: 1,
		Severity: domain.ThrottleStop, Reason: "quota closed", CreatedAt: coordinatorTestTime,
	}
	transport := &exchangeTransport{}
	report := WorkerExchangeReport{}
	err = (FleetCoordinator{}).reconcileWorkerThrottle(
		ctx, store, transport, snapshot, []domain.ThrottleDirective{directive},
		[]domain.QuotaPool{{ID: "pool", Admission: domain.AdmissionClosed, MaxConcurrent: 1}},
		coordinatorTestTime, &report,
	)
	if err != nil {
		t.Fatalf("historical assignment throttle reconciliation: %v", err)
	}
	if len(transport.throttles) != 0 {
		t.Fatalf("historical assignment received throttle: %#v", transport.throttles)
	}
	records, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("historical assignment throttle records = %#v", records)
	}
}

type testOfferBuilder struct{}

func (testOfferBuilder) BuildAssignmentOffer(_ context.Context, assignment domain.Assignment, expiresAt time.Time) (workerproto.AssignmentOffer, error) {
	object := workerproto.ArtifactObject{
		ID: "prompt-1", Path: "prompt/task.md", Kind: "prompt", MediaType: "text/markdown",
		Size: 1, SHA256: strings.Repeat("a", 64),
	}
	pkg := workerproto.ExecutionPackage{
		Version: workerproto.ExecutionPackageVersion, ID: "package-" + assignment.ID,
		CoordinatorID: "coordinator", CoordinatorEpoch: 1,
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		Identity: workerproto.ExecutionIdentity{
			WorkflowID: "workflow-1", WorkflowRunID: "run-1", TaskID: "task-1",
			AttemptID: assignment.AttemptID, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			DispatchToken: assignment.DispatchToken, ThreadID: assignment.ThreadID,
		},
		Class: domain.TaskClassRequired, Prompt: object, Route: assignment.Route,
		Environment: workerproto.EnvironmentReference{
			CatalogRevision: "catalog-1", Project: "project-1", Repository: "ssh://git/repository",
			Ref: "main", Scope: "task", SetupProfile: "setup-1", T3Project: "t3-project",
		},
		Verification: []string{"true"},
		Limits: workerproto.ExecutionLimits{
			MaxTurns: 2, PrepareTimeout: time.Minute, VerificationTimeout: time.Minute,
			MaxArtifactBytes: 1024, MaxTotalBytes: 2048,
		},
		CreatedAt: coordinatorTestTime,
	}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	return workerproto.AssignmentOffer{Assignment: assignment, Package: manifest, ExpiresAt: expiresAt}, err
}

type exchangeTransport struct {
	snapshots       []domain.WorkerSnapshot
	renewalSnapshot *domain.WorkerSnapshot
	offers          []workerproto.AssignmentOffer
	commands        []domain.WorkerCommand
	renewals        []domain.AssignmentLeaseRenewal
	throttles       []domain.ThrottleCommand
}

func (t *exchangeTransport) DeliverLeaseRenewals(_ context.Context, renewals []domain.AssignmentLeaseRenewal) (domain.WorkerSnapshot, error) {
	t.renewals = append(t.renewals, renewals...)
	if t.renewalSnapshot != nil {
		return *t.renewalSnapshot, nil
	}
	snapshot := t.snapshots[0]
	t.snapshots = t.snapshots[1:]
	return snapshot, nil
}

func (t *exchangeTransport) DeliverThrottle(_ context.Context, _ domain.WorkerSnapshot, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
	t.throttles = append(t.throttles, commands...)
	acks := make([]domain.ThrottleAcknowledgement, 0, len(commands))
	for _, command := range commands {
		result := domain.ThrottleResultWarned
		acks = append(acks, domain.ThrottleAcknowledgement{CommandID: command.ID, AttemptID: command.AttemptID, Accepted: true, Result: result, AcknowledgedAt: coordinatorTestTime})
	}
	return acks, nil
}

func (t *exchangeTransport) Snapshot(context.Context) (domain.WorkerSnapshot, error) {
	snapshot := t.snapshots[0]
	t.snapshots = t.snapshots[1:]
	return snapshot, nil
}

func (t *exchangeTransport) DeliverOffers(_ context.Context, offers []workerproto.AssignmentOffer) ([]domain.AssignmentClaimRequest, error) {
	t.offers = append(t.offers, offers...)
	claims := make([]domain.AssignmentClaimRequest, 0, len(offers))
	for _, offer := range offers {
		claims = append(claims, domain.AssignmentClaimRequest{
			CoordinatorEpoch: offer.Package.Package.CoordinatorEpoch,
			WorkerID:         offer.Assignment.WorkerID, WorkerEpoch: offer.Assignment.WorkerEpoch,
			AssignmentID: offer.Assignment.ID, AssignmentEpoch: offer.Assignment.Epoch,
			LeaseToken: offer.Assignment.LeaseToken, ClaimedAt: coordinatorTestTime,
			LeaseExpiresAt: offer.Assignment.LeaseExpiresAt,
		})
	}
	return claims, nil
}

func (t *exchangeTransport) DeliverWorkerCommands(_ context.Context, snapshot domain.WorkerSnapshot, commands []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error) {
	t.commands = append(t.commands, commands...)
	acks := make([]domain.WorkerAcknowledgement, 0, len(commands))
	for _, command := range commands {
		acks = append(acks, domain.WorkerAcknowledgement{
			CommandID: command.ID, WorkerID: command.WorkerID, WorkerEpoch: command.WorkerEpoch,
			CoordinatorEpoch: command.CoordinatorEpoch, AssignmentID: command.AssignmentID,
			AssignmentEpoch: command.AssignmentEpoch, WorkerSequence: snapshot.Sequence,
			Accepted: true, AcknowledgedAt: coordinatorTestTime,
		})
	}
	return acks, nil
}
