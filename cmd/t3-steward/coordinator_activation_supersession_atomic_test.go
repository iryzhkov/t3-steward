package main

import (
	"context"
	"errors"
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

// failingActivationCommit stands in for a coordinator that fails, or dies,
// at the moment it raises the activation epoch: every read is the real
// store's, and the commit of the plan never lands.
type failingActivationCommit struct {
	backlog.SupervisionActivationStore
}

var errEpochRaiseLost = errors.New("the coordinator stopped before the activation epoch was raised")

func (failingActivationCommit) CommitSupervisionActivation(context.Context, backlog.SupervisionActivationCommit) error {
	return errEpochRaiseLost
}

// reassessedFailedOffer builds a run whose overseer offer failed to package
// and which an operator has asked to reassess, the state in which the
// coordinator releases the failed offer and raises the activation epoch.
func reassessedFailedOffer(t *testing.T) (*activationLeaseFixture, domain.Activation, domain.Assignment) {
	t.Helper()
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.coordinator.logger = slog.New(slog.DiscardHandler)
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
	if err != nil || before.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("initial activation = %+v, err=%v", before.Activation, err)
	}
	original := activationOffer(t, fixture, before.Activation)
	artifacts := &backlog.CoordinatorArtifactStore{Root: t.TempDir(), Catalog: fixture.store}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Artifacts: []domain.Artifact{{
		ID: "artifact-overseer", WorkflowRunID: activationLeaseRun, Kind: domain.ArtifactInput,
		Name: "prompts/overseer.md", MediaType: "text/markdown", Size: 1,
		SHA256: strings.Repeat("0", 64), StoragePath: "prompt.md", Producer: "submission", CreatedAt: fixture.now,
	}}}); err != nil {
		t.Fatal(err)
	}
	failing := activationQualificationBuilder(t, fixture.store, artifacts, strings.Repeat("x", workerproto.SupervisionPromptByteCap))
	exchange, err := (backlog.FleetCoordinator{Store: fixture.store, Now: func() time.Time { return fixture.now }}).ReconcileWorker(
		ctx, &reassessmentExchangeTransport{snapshot: snapshot}, failing, backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour)
	if err != nil || len(exchange.DispatchFailures) != 1 {
		t.Fatalf("initial exchange = %+v, err=%v", exchange, err)
	}
	admin, err := backlogadmin.New(fixture.store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	admin.SetSupervisionStore(backlogadmin.CoordinatorSupervisionStore{Store: fixture.store})
	projection, err := fixture.store.LoadSupervisionProjection(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	fixture.now = activationLeaseTime.Add(10 * time.Minute)
	if _, err := admin.Supervise(ctx, backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, backlogadmin.SupervisionRequest{
		Version: backlogadmin.SupervisionVersion, Operation: backlogadmin.SupervisionReassess,
		RunID: activationLeaseRun, RequestKey: "reassess-after-package-failure",
		ExpectedRevision: projection.Record.Revision, Reason: "repair the activation package input",
	}); err != nil {
		t.Fatal(err)
	}
	return fixture, before.Activation, original
}

func activationOffer(t *testing.T, fixture *activationLeaseFixture, activation domain.Activation) domain.Assignment {
	t.Helper()
	records, err := fixture.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assignment, found := activationAssignmentOf(records, activation)
	if !found {
		t.Fatalf("activation %s has no assignment", activation.ID)
	}
	return assignment
}

// Releasing a failed overseer offer and raising the activation epoch that
// replaces it are one transaction. They used to be two: the offer was
// released and its attempt ended in one commit, and the epoch was raised in a
// later one after placement, so a coordinator that failed or died in between
// left a released offer under an activation still pending dispatch at the old
// epoch. A failure at the epoch raise must now leave the offer as it was.
func TestFailedEpochRaiseLeavesTheFailedOfferInPlace(t *testing.T) {
	ctx := context.Background()
	fixture, before, original := reassessedFailedOffer(t)
	real := fixture.coordinator.activations.Store
	fixture.coordinator.activations.Store = failingActivationCommit{real}
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	offer := activationOffer(t, fixture, before)
	if state.Activation.Epoch != before.Epoch || offer.State != domain.AssignmentOffered {
		t.Fatalf("after a lost epoch raise: activation epoch %d (was %d), offer %s; want neither changed",
			state.Activation.Epoch, before.Epoch, offer.State)
	}

	// Once the coordinator can commit again, the same reassessment releases
	// the offer and raises the epoch together.
	fixture.coordinator.activations.Store = real
	fixture.now = activationLeaseTime.Add(11 * time.Minute)
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	state, err = fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	offer = activationOffer(t, fixture, before)
	if state.Activation.Epoch != before.Epoch+1 || offer.State != domain.AssignmentReleased || offer.ID != original.ID {
		t.Fatalf("after the reassessment: activation epoch %d (was %d), offer %s %s; want the epoch raised and the offer released",
			state.Activation.Epoch, before.Epoch, offer.ID, offer.State)
	}
}
