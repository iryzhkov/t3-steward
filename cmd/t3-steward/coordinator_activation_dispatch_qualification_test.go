package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type reassessmentExchangeTransport struct {
	snapshot domain.WorkerSnapshot
	offers   []workerproto.AssignmentOffer
}

func (t *reassessmentExchangeTransport) WorkerID() string    { return t.snapshot.WorkerID }
func (t *reassessmentExchangeTransport) WorkerEpoch() string { return t.snapshot.WorkerEpoch }
func (t *reassessmentExchangeTransport) Snapshot(context.Context, workerproto.SnapshotRequest) (domain.WorkerSnapshot, error) {
	return t.snapshot, nil
}
func (t *reassessmentExchangeTransport) DeliverOffers(_ context.Context, offers []workerproto.AssignmentOffer) ([]domain.AssignmentClaimRequest, error) {
	t.offers = append(t.offers, offers...)
	claims := make([]domain.AssignmentClaimRequest, 0, len(offers))
	for _, offer := range offers {
		claims = append(claims, domain.AssignmentClaimRequest{
			CoordinatorEpoch: offer.Package.Package.CoordinatorEpoch,
			WorkerID:         offer.Assignment.WorkerID, WorkerEpoch: offer.Assignment.WorkerEpoch,
			AssignmentID: offer.Assignment.ID, AssignmentEpoch: offer.Assignment.Epoch,
			LeaseToken: offer.Assignment.LeaseToken, ClaimedAt: activationLeaseTime.Add(12 * time.Minute),
			LeaseExpiresAt: offer.Assignment.LeaseExpiresAt,
		})
	}
	return claims, nil
}
func (*reassessmentExchangeTransport) DeliverLeaseRenewals(context.Context, []domain.AssignmentLeaseRenewal) (domain.WorkerSnapshot, error) {
	return domain.WorkerSnapshot{}, nil
}
func (*reassessmentExchangeTransport) DeliverThrottle(context.Context, domain.WorkerSnapshot, []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
	return nil, nil
}
func (*reassessmentExchangeTransport) DeliverWorkerCommands(context.Context, domain.WorkerSnapshot, []domain.WorkerCommand) ([]domain.WorkerAcknowledgement, error) {
	return nil, nil
}

func activationQualificationBuilder(t *testing.T, store *sqlite.Store, artifacts *backlog.CoordinatorArtifactStore, principal string) backlog.CoordinatorOfferBuilder {
	t.Helper()
	catalog, err := backlog.NewProjectCatalog(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return backlog.CoordinatorOfferBuilder{
		Store: store, Catalog: catalog, CatalogRevision: "catalog-qualification",
		CoordinatorID: "coordinator-1", CoordinatorEpoch: 1,
		VerificationTimeout: time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20,
		Supervision:        backlog.CoordinatorSupervisionStore{Store: store},
		ActivationEvidence: artifacts, SupervisorPrincipal: principal,
	}
}

func TestFailedActivationPackageSurvivesRestartAndReassessmentDispatchesFreshOffer(t *testing.T) {
	ctx := context.Background()
	var dbPath string
	fixture := newActivationLeaseFixtureWithStore(t, func(path string) (*sqlite.Store, error) {
		dbPath = path
		return sqlite.OpenMigrated(path)
	})
	fixture.coordinator.logger = slog.Default()
	snapshots, err := fixture.store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("worker snapshots = %+v, err=%v", snapshots, err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ObservedAt = snapshot.ObservedAt.Add(time.Second)
	snapshot.Inventory.ObservedAt = snapshot.ObservedAt
	snapshot.Inventory.Capabilities = append(snapshot.Inventory.Capabilities, workerproto.PackageCapabilitySupervisionEvidence)
	fixture.superviseRun(t)
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	before, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if before.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("initial activation = %+v", before.Activation)
	}
	recordsBefore, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	activationAttempts := make(map[string]bool)
	for _, attempt := range recordsBefore.Attempts {
		if attempt.SupervisionActivationID == before.Activation.ID {
			activationAttempts[attempt.ID] = true
		}
	}
	var original domain.Assignment
	for _, assignment := range recordsBefore.Assignments {
		if activationAttempts[assignment.AttemptID] {
			original = assignment
		}
	}
	if original.ID == "" {
		t.Fatal("coordinator did not commit the activation assignment")
	}

	artifactRoot := t.TempDir()
	artifacts := &backlog.CoordinatorArtifactStore{Root: artifactRoot, Catalog: fixture.store}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Artifacts: []domain.Artifact{{
		ID: "artifact-overseer", WorkflowRunID: activationLeaseRun, Kind: domain.ArtifactInput,
		Name: "prompts/overseer.md", MediaType: "text/markdown", Size: 1,
		SHA256: strings.Repeat("0", 64), StoragePath: "prompt.md", Producer: "submission", CreatedAt: fixture.now,
	}}}); err != nil {
		t.Fatal(err)
	}
	failing := activationQualificationBuilder(t, fixture.store, artifacts, strings.Repeat("x", workerproto.SupervisionPromptByteCap))
	firstTransport := &reassessmentExchangeTransport{snapshot: snapshot}
	first, err := (backlog.FleetCoordinator{Store: fixture.store, Now: func() time.Time { return fixture.now }}).ReconcileWorker(
		ctx, firstTransport, failing, backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.DispatchFailures) != 1 || first.DispatchFailures[0].Code != backlog.ActivationPackageErrorPromptTooLarge {
		t.Fatalf("initial exchange = %+v", first)
	}
	retainedBefore, reader, found, err := artifacts.OpenActivationEvidenceSnapshot(ctx, activationLeaseRun, before.Activation.ID)
	if err != nil || !found {
		t.Fatalf("frozen evidence found=%v artifact=%+v err=%v", found, retainedBefore, err)
	}
	_ = reader.Close()

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.OpenMigrated(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = reopened
	fixture.store.SetClock(func() time.Time { return fixture.now })
	fixture.supervision = backlog.CoordinatorSupervisionStore{Store: reopened}
	fixture.coordinator.store = fixture.supervision
	fixture.coordinator.activations.Store = fixture.supervision
	fixture.coordinator.workers = func(ctx context.Context) ([]domain.WorkerSnapshot, error) {
		return fixture.store.LoadWorkerSnapshots(ctx)
	}
	artifacts.Catalog = reopened

	current, err := reopened.ListCurrentActivationDispatchFailures(ctx, activationLeaseRun)
	if err != nil || len(current) != 1 || current[0].ID != first.DispatchFailures[0].ID {
		t.Fatalf("restarted dispatch failure = %+v, err=%v", current, err)
	}
	admin, err := backlogadmin.New(reopened, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	admin.SetSupervisionStore(backlogadmin.CoordinatorSupervisionStore{Store: reopened})
	projection, err := reopened.LoadSupervisionProjection(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	fixture.now = activationLeaseTime.Add(10 * time.Minute)
	if _, err := admin.Supervise(ctx, backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, backlogadmin.SupervisionRequest{
		Version: backlogadmin.SupervisionVersion, Operation: backlogadmin.SupervisionReassess,
		RunID: activationLeaseRun, RequestKey: "repair-package-and-reassess",
		ExpectedRevision: projection.Record.Revision, Reason: "repair the activation package input",
	}); err != nil {
		t.Fatal(err)
	}
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	fixture.now = activationLeaseTime.Add(11 * time.Minute)
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())

	after, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if after.Activation.Epoch != before.Activation.Epoch+1 || after.Activation.ID == before.Activation.ID ||
		after.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("reassessed activation = %+v, want fresh pending dispatch after %+v", after.Activation, before.Activation)
	}
	reassessmentRecords, err := reopened.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reassessmentRecords.Assignments) < 2 {
		t.Fatalf("reassessment assignments = %+v attempts=%+v", reassessmentRecords.Assignments, reassessmentRecords.Attempts)
	}
	repaired := activationQualificationBuilder(t, reopened, artifacts, "campaign-supervisor")
	secondTransport := &reassessmentExchangeTransport{snapshot: snapshot}
	second, err := (backlog.FleetCoordinator{Store: reopened, Now: func() time.Time { return fixture.now }}).ReconcileWorker(
		ctx, secondTransport, repaired, backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondTransport.offers) != 1 || secondTransport.offers[0].Package.Package.Supervision == nil ||
		secondTransport.offers[0].Package.Package.Supervision.ActivationID != after.Activation.ID {
		t.Fatalf("repaired exchange = %+v, offers=%+v", second, secondTransport.offers)
	}
	retainedAfter, nextReader, found, err := artifacts.OpenActivationEvidenceSnapshot(ctx, activationLeaseRun, before.Activation.ID)
	if err != nil || !found {
		t.Fatalf("old evidence after reassessment found=%v artifact=%+v err=%v", found, retainedAfter, err)
	}
	_ = nextReader.Close()
	if retainedAfter.ID != retainedBefore.ID || retainedAfter.SHA256 != retainedBefore.SHA256 || retainedAfter.Size != retainedBefore.Size {
		t.Fatalf("old immutable evidence changed: before=%+v after=%+v", retainedBefore, retainedAfter)
	}
	recordsAfter, err := reopened.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recordsAfter.Tasks) != len(recordsBefore.Tasks) {
		t.Fatalf("task contracts changed: before=%+v after=%+v", recordsBefore.Tasks, recordsAfter.Tasks)
	}
	adminAfter, err := fixture.supervision.LoadSupervisionAdminState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if len(adminAfter.Gates) != 1 || adminAfter.Gates[0].Gate.Definition.ID != activationLeaseGate ||
		adminAfter.Gates[0].Gate.State != domain.GateReadyForReview {
		t.Fatalf("gate contract changed: %+v", adminAfter.Gates)
	}
}
