package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type custodyDriver struct {
	*fakeDriver
	custodyErr   error
	custodyStops int
}

func (d *custodyDriver) StopPreparation(context.Context, workerproto.ExecutionPackage) error {
	d.custodyStops++
	return d.custodyErr
}

func TestUncertainContainedPreparationNeverExhaustsIntoSuccess(t *testing.T) {
	d := &fakeDriver{prepareErr: fmt.Errorf("%w: lost start reply", ErrContainedCustody)}
	r := newClaimedRuntime(t, t.TempDir(), d)
	for i := 0; i < MaxPrepareAttempts+2; i++ {
		if err := r.prepare(context.Background(), "assignment-1"); !errors.Is(err, ErrContainedCustody) {
			t.Fatalf("prepare: %v", err)
		}
	}
	state, err := r.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	record := state.Attempts["assignment-1"]
	if record.Phase != PhasePreparing || record.PrepareAttempts != 0 {
		t.Fatalf("uncertain preparation became terminal: %+v", record)
	}
	if d.createCalls != 0 || d.stopCalls != 0 {
		t.Fatal("uncertain preparation dispatched or stopped")
	}
}

func TestFailureRetainsAssignmentUntilContainedCustodyConfirmed(t *testing.T) {
	r := newClaimedRuntime(t, t.TempDir(), &fakeDriver{})
	d := &custodyDriver{fakeDriver: &fakeDriver{}, custodyErr: errors.New("verification process still running")}
	r.driver = d
	if err := r.markFailed(context.Background(), "assignment-1", "thread missing"); err == nil {
		t.Fatal("released uncertain custody")
	}
	state, _ := r.journal.snapshot()
	if state.Attempts["assignment-1"].Phase != PhaseClaimed {
		t.Fatal("failure changed phase before stop")
	}
	d.custodyErr = nil
	if err := r.markFailed(context.Background(), "assignment-1", "thread missing"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := r.Snapshot(context.Background())
	if err != nil || snapshot.Assignments[0].State != domain.AssignmentCompleted {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if d.custodyStops != 2 {
		t.Fatalf("stop confirmations=%d", d.custodyStops)
	}
}

func TestPredispatchCancellationRequiresContainedStop(t *testing.T) {
	r := newClaimedRuntime(t, t.TempDir(), &fakeDriver{})
	d := &custodyDriver{fakeDriver: &fakeDriver{}, custodyErr: errors.New("server stop reply lost")}
	r.driver = d
	stop := testCommand(t, r, domain.WorkerCommandStop, "cancel")
	_, err := r.DeliverCommands(context.Background(), workerproto.CommandDelivery{Commands: []domain.WorkerCommand{stop}})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := r.journal.snapshot()
	if state.Attempts["assignment-1"].Phase == PhaseFailed {
		t.Fatal("cancel released live server")
	}
	if d.custodyStops == 0 {
		t.Fatal("cancel did not check custody")
	}
}

func TestSupersededPreparationRequiresContainedStop(t *testing.T) {
	d := &custodyDriver{fakeDriver: &fakeDriver{}, custodyErr: errors.New("prepared server stop reply lost")}
	r := newClaimedRuntime(t, t.TempDir(), d.fakeDriver)
	r.driver = d
	if err := r.markPhase("assignment-1", PhasePrepared, "", "/workspace", ""); err != nil {
		t.Fatal(err)
	}
	superseding := testOffer(t)
	superseding.Assignment.Epoch = 3
	superseding.Package.Package.Identity.AssignmentEpoch = 3
	manifest, err := workerproto.BuildExecutionPackageManifest(superseding.Package.Package)
	if err != nil {
		t.Fatal(err)
	}
	superseding.Package = manifest
	offers := workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{superseding}}
	claims, err := r.AcceptOffers(context.Background(), offers)
	if err != nil || len(claims.Claims) != 0 {
		t.Fatalf("uncertain custody claimed the superseding offer: claims=%+v err=%v", claims, err)
	}
	state, err := r.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Attempts["assignment-1"]; record.Assignment.Epoch != 2 || record.Phase != PhasePrepared {
		t.Fatalf("superseded record replaced before its server stopped: %+v", record)
	}
	d.custodyErr = nil
	if claims, err = r.AcceptOffers(context.Background(), offers); err != nil || len(claims.Claims) != 1 {
		t.Fatalf("confirmed custody withheld the claim: claims=%+v err=%v", claims, err)
	}
	if state, err = r.journal.snapshot(); err != nil {
		t.Fatal(err)
	}
	if record := state.Attempts["assignment-1"]; record.Phase != PhaseClaimed || record.Assignment.Epoch != 3 {
		t.Fatalf("superseding claim not recorded: %+v", record)
	}
	if d.custodyStops != 2 {
		t.Fatalf("stop confirmations=%d", d.custodyStops)
	}
}
