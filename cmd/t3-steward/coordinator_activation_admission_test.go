package main

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestActivationLifecycleReconcilesWhileAdmissionIsClosed(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	activation := fixture.activate(t)
	if activation.LeaseExpiresAt == nil {
		t.Fatal("a confirmed activation has no lease expiry")
	}

	fixture.now = activation.LeaseExpiresAt.Add(time.Second)
	fixture.coordinator.DispatchActivations(context.Background(), backlog.WorkerAdmissionPolicy{})

	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationRevoked {
		t.Fatalf("activation state = %q, want revoked despite closed admission", state.Activation.State)
	}
}

func TestActivationDispatchStillRequiresAdmission(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	fixture.coordinator.DispatchActivations(context.Background(), backlog.WorkerAdmissionPolicy{})

	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != "" && state.Activation.State != domain.ActivationIdle {
		t.Fatalf("activation state = %q, want no dispatch while admission is closed", state.Activation.State)
	}
	if state.Record.ActivationsUsed != 0 {
		t.Fatalf("activations used = %d, want no budget spent while admission is closed", state.Record.ActivationsUsed)
	}
}
