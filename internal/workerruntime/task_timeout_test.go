package workerruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestTaskTimeoutWaitsForContainmentAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	now := runtimeTestNow
	driver := &fakeDriver{stopErr: errors.New("lost stop response")}
	runtime := newTestRuntimeWithClock(t, root, driver, func() time.Time { return now })
	offer := testOffer(t)
	offer.Package.Package.Timeout = time.Minute
	manifest, err := workerproto.BuildExecutionPackageManifest(offer.Package.Package)
	if err != nil {
		t.Fatal(err)
	}
	offer.Package = manifest
	if _, err = runtime.AcceptOffers(ctx, workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err != nil {
		t.Fatal(err)
	}
	id := offer.Assignment.ID
	if err = runtime.markPhase(id, PhaseRunning, "", "/workspace", offer.Package.Package.Identity.ThreadID); err != nil {
		t.Fatal(err)
	}
	now = offer.Package.Package.CreatedAt.Add(2 * time.Minute)
	if err = runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.Attempts[id].Phase != PhaseStopping {
		t.Fatal("ambiguous timeout was terminalized")
	}
	restarted := newTestRuntimeWithClock(t, root, driver, func() time.Time { return now })
	driver.stopErr = nil
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadActive}
	if err = restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = restarted.journal.snapshot()
	if state.Attempts[id].Phase != PhaseStopping {
		t.Fatal("active execution considered stopped")
	}
	driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
	if err = restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ = restarted.journal.snapshot()
	if state.Attempts[id].Phase != PhaseFailed {
		t.Fatal("contained timeout was not failed")
	}
	observed := observation(state.Attempts[id], now)
	if observed.Journal == nil || observed.Journal.Phase != "failed" || observed.Journal.PackageSHA256 != manifest.SHA256 {
		t.Fatalf("journal excerpt=%+v", observed.Journal)
	}
	if driver.createCalls != 0 || driver.collectCalls != 0 {
		t.Fatal("timeout dispatched or collected success")
	}
	ack, err := restarted.executeThrottle(ctx, domain.ThrottleCommand{ID: "late-resume", AssignmentID: id, Kind: domain.ThrottleCommandResume})
	if err != nil || ack.Accepted || driver.resumeCalls != 0 {
		t.Fatalf("expired failed task resumed: %+v %v", ack, err)
	}
}

func TestTaskTimeoutPreventsLateDispatch(t *testing.T) {
	ctx := context.Background()
	now := runtimeTestNow
	driver := &fakeDriver{workspace: "/workspace"}
	runtime := newTestRuntimeWithClock(t, t.TempDir(), driver, func() time.Time { return now })
	offer := testOffer(t)
	offer.Package.Package.Timeout = time.Minute
	manifest, err := workerproto.BuildExecutionPackageManifest(offer.Package.Package)
	if err != nil {
		t.Fatal(err)
	}
	offer.Package = manifest
	if _, err = runtime.AcceptOffers(ctx, workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err != nil {
		t.Fatal(err)
	}
	if err = runtime.markPhase(offer.Assignment.ID, PhasePrepared, "", "/workspace", ""); err != nil {
		t.Fatal(err)
	}
	now = offer.Package.Package.CreatedAt.Add(2 * time.Minute)
	if err = runtime.dispatch(ctx, offer.Assignment.ID); err != nil {
		t.Fatal(err)
	}
	if driver.createCalls != 0 {
		t.Fatal("expired package dispatched")
	}
	state, _ := runtime.journal.snapshot()
	if state.Attempts[offer.Assignment.ID].Phase != PhaseFailed {
		t.Fatal("expired prepared task not failed")
	}
}
