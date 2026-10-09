package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A readiness check observes one repository per task and worker. A worker that
// cannot be reached used to be dialled again for every task, each time over a
// fresh connection and behind the observer's one-probe-at-a-time lock, so a
// five-task campaign paid the same failure five times and the admin request
// outlived its 30 s deadline (2026-10-06). A failed probe is now remembered for
// a short time, like a successful one is for longer.
func TestRepositoryProbeRemembersAnUnreachableWorkerBriefly(t *testing.T) {
	observer := probeObserver(nil)
	dials := 0
	unreachable := observer.dial
	observer.dial = func(ctx context.Context, workerID string) (repositoryProbeClient, func() error, error) {
		dials++
		return unreachable(ctx, workerID)
	}
	now := time.Date(2026, 10, 6, 22, 0, 0, 0, time.UTC)
	observer.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if _, err := observer.ObserveRepository(context.Background(), probeKey("agent-b")); err == nil {
			t.Fatal("an unreachable worker was reported as observed")
		}
	}
	if dials != 1 {
		t.Fatalf("dials = %d, want 1: a remembered failure must not dial again", dials)
	}
	_, err := observer.ObserveRepository(context.Background(), probeKey("agent-b"))
	if err == nil || !strings.Contains(err.Error(), "did not answer") {
		t.Fatalf("a remembered failure must repeat its cause, got %v", err)
	}

	now = now.Add(repositoryProbeFailureTTL + time.Second)
	if _, err := observer.ObserveRepository(context.Background(), probeKey("agent-b")); err == nil {
		t.Fatal("the worker is still unreachable")
	}
	if dials != 2 {
		t.Fatalf("dials = %d, want 2: an expired failure is probed again", dials)
	}
}

// Another worker's failure, or another ref, is not remembered for this key.
func TestRepositoryProbeFailureMemoryIsPerKey(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{"homelab": reachableWorker(t, "homelab")})
	if _, err := observer.ObserveRepository(context.Background(), probeKey("agent-b")); err == nil {
		t.Fatal("agent-b is not reachable in this fixture")
	}
	observation, err := observer.ObserveRepository(context.Background(), probeKey("homelab"))
	if err != nil || observation.Class == "" {
		t.Fatalf("homelab must still be observed: %+v, %v", observation, err)
	}
}

// A caller that gives up is not evidence about the worker: the worker is probed
// again on the next request.
func TestRepositoryProbeDoesNotRememberACallerCancellation(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{"agent-a": blockingProbeClient{}})
	dials := 0
	inner := observer.dial
	observer.dial = func(ctx context.Context, workerID string) (repositoryProbeClient, func() error, error) {
		dials++
		return inner(ctx, workerID)
	}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := observer.ObserveRepository(ctx, probeKey("agent-a"))
		cancel()
		if err == nil {
			t.Fatal("a worker that never answered was reported as observed")
		}
	}
	if dials != 2 {
		t.Fatalf("dials = %d, want 2: the caller's deadline must not be remembered as the worker's failure", dials)
	}
}

// blockingProbeClient never answers until its caller gives up.
type blockingProbeClient struct{}

func (blockingProbeClient) ObserveRepository(ctx context.Context, _ workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error) {
	<-ctx.Done()
	return workerproto.RepositoryObservation{}, ctx.Err()
}

// One worker that accepts a connection and then never answers must not hold a
// readiness check for the transport's whole request timeout.
func TestRepositoryProbeIsBoundedForAWorkerThatNeverAnswers(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{"agent-b": blockingProbeClient{}})
	observer.budget = 50 * time.Millisecond
	start := time.Now()
	_, err := observer.ObserveRepository(context.Background(), probeKey("agent-b"))
	if err == nil {
		t.Fatal("a worker that never answered was reported as observed")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the probe budget's deadline", err)
	}
	if elapsed := time.Since(start); elapsed > testtiming.Bound(2*time.Second) {
		t.Fatalf("probe took %v, want it bounded by its budget", elapsed)
	}
}
