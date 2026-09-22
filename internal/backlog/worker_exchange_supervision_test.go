package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// activationOfferBuilder packages every assignment as a supervision activation.
// It is the worst case for the exchange gate: a coordinator that has already
// decided to wake an overseer on this worker, so the only thing left between
// the activation and an older worker is the exchange itself.
type activationOfferBuilder struct{}

type failingActivationOfferBuilder struct {
	calls int
	code  string
}

func (b *failingActivationOfferBuilder) BuildAssignmentOffer(context.Context, domain.Assignment, time.Time) (workerproto.AssignmentOffer, error) {
	b.calls++
	return workerproto.AssignmentOffer{}, &ActivationPackageError{Code: b.code, Cause: errors.New("sensitive raw failure")}
}

func (activationOfferBuilder) BuildAssignmentOffer(
	_ context.Context, assignment domain.Assignment, expiresAt time.Time,
) (workerproto.AssignmentOffer, error) {
	object := workerproto.ArtifactObject{
		ID: "prompt-1", Path: "prompt/activation.md", Kind: "prompt", MediaType: "text/markdown",
		Size: 1, SHA256: strings.Repeat("a", 64),
	}
	pkg := workerproto.ExecutionPackage{
		Version: workerproto.ExecutionPackageVersion, ID: "package-" + assignment.ID,
		CoordinatorID: "coordinator", CoordinatorEpoch: 1,
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		Identity: workerproto.ExecutionIdentity{
			WorkflowID: "workflow-1", WorkflowRunID: "run-1", TaskID: "task-overseer",
			AttemptID: assignment.AttemptID, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			DispatchToken: assignment.DispatchToken, ThreadID: assignment.ThreadID,
		},
		Class: domain.TaskClassRequired, Prompt: object, Route: assignment.Route,
		Environment: workerproto.EnvironmentReference{
			Type: "fresh", CatalogRevision: "catalog-1", Project: "project-1",
			Scope: "task", SetupProfile: "supervision",
		},
		RequiredCapabilities: []string{workerproto.CapabilityCampaignSupervision},
		Supervision: &workerproto.SupervisionActivation{
			ActivationID: "activation-1", RunID: "run-1", Epoch: 1, RecordRevision: 1,
			GraphRevision: 1, Principal: "overseer-1", LeaseToken: "lease-1",
			LeaseExpiresAt: coordinatorTestTime.Add(time.Hour), MaxTurns: 3,
			Prompt: "review the analysis gate",
			Actions: []workerproto.SupervisionAction{{
				Name: "decide", Command: []string{"t3-steward", "campaign", "supervision", "decide"},
			}},
		},
		Limits: workerproto.ExecutionLimits{
			MaxTurns: 3, PrepareTimeout: time.Minute, VerificationTimeout: time.Minute,
			MaxArtifactBytes: 1024, MaxTotalBytes: 2048,
		},
		CreatedAt: coordinatorTestTime,
	}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	return workerproto.AssignmentOffer{Assignment: assignment, Package: manifest, ExpiresAt: expiresAt}, err
}

// testActivationAssignment is one assignment bound to the worker
// coordinatorSnapshot describes, with a complete provider route, which the
// execution package validator requires before it will build a manifest.
func testActivationAssignment(id, attemptID, dispatchToken, threadID string) domain.Assignment {
	snapshot := coordinatorSnapshot(1)
	return domain.Assignment{
		ID: id, AttemptID: attemptID, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		Route: domain.ProviderRoute{
			WorkerID: snapshot.WorkerID, ProviderInstanceID: "claudeAgent",
			Model: "claude-fable-5-1", QuotaPoolID: "claude-main",
		},
		State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-1",
		DispatchToken: dispatchToken, ThreadID: threadID,
		LeaseExpiresAt: coordinatorTestTime.Add(10 * time.Minute),
		CreatedAt:      coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
}

func supervisionCapableSnapshot(sequence int64, capable bool) domain.WorkerSnapshot {
	snapshot := coordinatorSnapshot(sequence)
	if capable {
		snapshot.Inventory.Capabilities = append(
			snapshot.Inventory.Capabilities, workerproto.CapabilityCampaignSupervision)
	}
	return snapshot
}

func TestSnapshotActivationIsRefusedBeforeOfferToLegacyWorker(t *testing.T) {
	offer, err := activationOfferBuilder{}.BuildAssignmentOffer(
		context.Background(),
		testActivationAssignment("assignment-evidence", "attempt-evidence", "dispatch-evidence", "thread-evidence"),
		coordinatorTestTime.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	offer.Package.Package.RequiredCapabilities = append(
		offer.Package.Package.RequiredCapabilities,
		workerproto.PackageCapabilitySupervisionEvidence,
	)
	legacy := supervisionCapableSnapshot(1, true)
	if err := requireOfferedCapabilities(offer, legacy); err == nil ||
		!strings.Contains(err.Error(), workerproto.PackageCapabilitySupervisionEvidence) {
		t.Fatalf("legacy worker refusal = %v, want missing evidence capability", err)
	}
	legacy.Inventory.Capabilities = append(
		legacy.Inventory.Capabilities,
		workerproto.PackageCapabilitySupervisionEvidence,
	)
	if err := requireOfferedCapabilities(offer, legacy); err != nil {
		t.Fatalf("upgraded worker refused: %v", err)
	}
}

func TestRequireOfferedCapabilitiesGatesActivationsOnTheDurableInventory(t *testing.T) {
	offer, err := activationOfferBuilder{}.BuildAssignmentOffer(
		context.Background(),
		testActivationAssignment("assignment-1", "attempt-1", "dispatch-1", "thread-1"),
		coordinatorTestTime.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !offer.Package.Package.IsActivation() {
		t.Fatal("the fixture did not build an activation package")
	}

	older := supervisionCapableSnapshot(1, false)
	err = requireOfferedCapabilities(offer, older)
	if err == nil {
		t.Fatal("an activation was offered to a worker that does not advertise the capability")
	}
	for _, fragment := range []string{older.WorkerID, workerproto.CapabilityCampaignSupervision} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("refusal %q does not name %q", err, fragment)
		}
	}

	if err := requireOfferedCapabilities(offer, supervisionCapableSnapshot(1, true)); err != nil {
		t.Fatalf("a capable worker was refused: %v", err)
	}

	// A handshake claim is not evidence: the gate reads only
	// snapshot.Inventory.Capabilities, so a worker whose durable inventory is
	// silent stays refused however its negotiation described itself.
	handshakeOnly := older
	handshakeOnly.Inventory.Capabilities = nil
	if err := requireOfferedCapabilities(offer, handshakeOnly); err == nil {
		t.Fatal("an activation was offered to a worker with no advertised capability at all")
	}

	// An ordinary task package is unaffected: this gate says nothing about
	// work that is not an activation.
	task, err := testOfferBuilder{}.BuildAssignmentOffer(
		context.Background(),
		testActivationAssignment("assignment-2", "attempt-2", "dispatch-2", "thread-2"),
		coordinatorTestTime.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := requireOfferedCapabilities(task, older); err != nil {
		t.Fatalf("an ordinary task offer was refused: %v", err)
	}
}

// reconcileActivationOffer runs one exchange against a worker that either does
// or does not advertise the supervision capability, and reports what the
// exchange did with the single outstanding activation assignment.
func reconcileActivationOffer(t *testing.T, capable bool) (WorkerExchangeReport, *exchangeTransport, []domain.Assignment) {
	report, transport, assignments, _ := reconcileActivationOfferWithBuilder(t, capable, activationOfferBuilder{})
	return report, transport, assignments
}

func reconcileActivationOfferWithBuilder(t *testing.T, capable bool, builder AssignmentOfferBuilder) (WorkerExchangeReport, *exchangeTransport, []domain.Assignment, *sqlite.Store) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	snapshot := supervisionCapableSnapshot(1, capable)
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-overseer", AssignmentID: "assignment-1",
		SupervisionActivationID: "activation-1", SupervisionActivationEpoch: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: coordinatorTestTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		Route: domain.ProviderRoute{
			WorkerID: snapshot.WorkerID, ProviderInstanceID: "claudeAgent",
			Model: "claude-fable-5-1", QuotaPoolID: "claude-main",
		},
		State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-1", DispatchToken: "dispatch-1",
		ThreadID: "thread-1", LeaseExpiresAt: coordinatorTestTime.Add(10 * time.Minute),
		CreatedAt: coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1, GraphRevision: 1, CreatedAt: coordinatorTestTime, UpdatedAt: coordinatorTestTime}},
		Attempts:     []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
		Activations: []domain.Activation{{
			ID: "activation-1", RunID: "run-1", Epoch: 1,
			State: domain.ActivationPendingDispatch, DispatchIdentity: "dispatch-activation-1",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	transport := &exchangeTransport{snapshots: []domain.WorkerSnapshot{snapshot, snapshot}}
	report, err := (FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime }}).ReconcileWorker(
		ctx, transport, builder, WorkerAdmissionPolicy{QuotaChecksDisabled: true},
		nil, nil, time.Minute, time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return report, transport, records.Assignments, store
}

// TestWorkerExchangeWithholdsAnActivationFromAWorkerWithoutTheCapability is the
// compatibility assertion the seam review asks for: the refusal has to happen
// at the exchange, not only at placement. An older worker handed an activation
// would run it as an ordinary task, produce a turn, and let the coordinator try
// to verify a review as task output, which is the outcome the capability was
// introduced to prevent.
func TestWorkerExchangeWithholdsAnActivationFromAWorkerWithoutTheCapability(t *testing.T) {
	report, transport, assignments := reconcileActivationOffer(t, false)
	if len(transport.offers) != 0 {
		t.Fatalf("an activation crossed the wire to an older worker: %#v", transport.offers)
	}
	if len(report.Offered) != 0 || len(report.Claimed) != 0 {
		t.Fatalf("exchange report offered or claimed an activation: %+v", report)
	}
	if len(report.Withheld) != 1 || report.Withheld[0].ID != "assignment-1" {
		t.Fatalf("withheld assignments = %+v", report.Withheld)
	}
	// Withholding is not cancelling. The assignment stays offered, so the same
	// activation reaches the worker as soon as it advertises the capability.
	if len(assignments) != 1 || assignments[0].State != domain.AssignmentOffered {
		t.Fatalf("durable assignments after a withheld activation = %+v", assignments)
	}
}

func TestWorkerExchangePersistsAndSuppressesDeterministicActivationFailure(t *testing.T) {
	builder := &failingActivationOfferBuilder{code: ActivationPackageErrorPromptTooLarge}
	first, _, assignments, store := reconcileActivationOfferWithBuilder(t, true, builder)
	if builder.calls != 1 || len(first.DispatchFailures) != 1 || len(first.Withheld) != 1 {
		t.Fatalf("first exchange calls=%d report=%+v", builder.calls, first)
	}
	if strings.Contains(first.DispatchFailures[0].SafeMessage, "sensitive") {
		t.Fatalf("diagnostic exposed raw failure: %+v", first.DispatchFailures[0])
	}
	snapshot := supervisionCapableSnapshot(3, true)
	snapshot.ObservedAt = coordinatorTestTime.Add(time.Minute)
	snapshot.Inventory.ObservedAt = snapshot.ObservedAt
	snapshot.ValidUntil = snapshot.ObservedAt.Add(time.Hour)
	transport := &exchangeTransport{snapshots: []domain.WorkerSnapshot{snapshot, snapshot}}
	second, err := (FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime.Add(time.Minute) }}).ReconcileWorker(
		context.Background(), transport, builder, WorkerAdmissionPolicy{QuotaChecksDisabled: true},
		nil, nil, time.Minute, time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if builder.calls != 1 {
		t.Fatalf("identical failure rebuilt package %d times, want once", builder.calls)
	}
	if len(second.DispatchFailures) != 1 || len(second.Withheld) != 1 || len(second.Offered) != 0 || len(transport.offers) != 0 {
		t.Fatalf("suppressed exchange = %+v offers=%+v assignments=%+v", second, transport.offers, assignments)
	}
}

func TestWorkerExchangeOffersAnActivationToACapableWorker(t *testing.T) {
	report, transport, assignments := reconcileActivationOffer(t, true)
	if len(transport.offers) != 1 || !transport.offers[0].Package.Package.IsActivation() {
		t.Fatalf("offers delivered = %#v", transport.offers)
	}
	if len(report.Offered) != 1 || len(report.Withheld) != 0 {
		t.Fatalf("exchange report = %+v", report)
	}
	if len(assignments) != 1 || assignments[0].State != domain.AssignmentClaimed {
		t.Fatalf("durable assignments after an offered activation = %+v", assignments)
	}
}
