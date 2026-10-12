package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestReadinessReprobesMissingRefAfterFetch(t *testing.T) {
	// Exercise check-first and submission-first so each path must itself refresh.
	for _, path := range []string{"check", "submission"} {
		t.Run(path, func(t *testing.T) {
			runner := &probeProcessRunner{exit: 2, err: &backlog.ProcessExitError{ExitCode: 2}}
			observer := probeObserver(map[string]repositoryProbeClient{
				"homelab": probeWorker(t, "homelab", runner, probeCredentialRef),
			})
			now := probeNow
			observer.now = func() time.Time { return now }
			service, store := probeReadinessService(t, observer, "homelab", "normandy")
			initial := probeViabilityMatrix(t, service)
			if initial.Outcome != backlogadmin.ViabilityImpossible || !probeHasReason(probeCandidate(t, initial, "homelab"), backlogadmin.ReasonRefNotFound) {
				t.Fatalf("fresh missing ref did not refuse: %+v", initial)
			}
			// Another readiness request must obtain a new confirmed refusal.
			before := len(runner.calls)
			again := probeViabilityMatrix(t, service)
			if again.Outcome != backlogadmin.ViabilityImpossible || len(runner.calls)-before != 1 {
				t.Fatal("confirmed missing ref was reused")
			}
			runner.output, runner.exit, runner.err = reachableOutput, 0, nil
			now = now.Add(time.Second) // Still inside TTL; simulate commit becoming available.
			before = len(runner.calls)
			if path == "check" {
				matrix := probeViabilityMatrix(t, service)
				if matrix.Outcome != backlogadmin.ViabilityReady || probeHasReason(probeCandidate(t, matrix, "homelab"), backlogadmin.ReasonRefNotFound) {
					t.Fatalf("check retained stale refusal: %+v", matrix)
				}
			}
			_, err := probeSubmissions(t, service, store).SubmitDirectory(context.Background(), backlog.DirectorySubmission{
				IdempotencyKey: "after-fetch", BundleDir: probeCampaignFixture(t), Principal: "local:1000",
			})
			if err != nil {
				t.Fatalf("submission retained stale refusal: %v", err)
			}
			if len(runner.calls)-before != 1 {
				t.Fatal("expected one fresh probe, followed by cached success")
			}
			if probeWorkflowCount(t, store) != 1 {
				t.Fatal("submission did not create workflow")
			}
			matrix := probeViabilityMatrix(t, service)
			if matrix.Outcome != backlogadmin.ViabilityReady || len(runner.calls)-before != 1 {
				t.Fatal("success was not reused")
			}
			observation, found := observer.cache.Lookup(probeKey("homelab"))
			if !found || !observation.ObservedAt.After(probeNow) {
				t.Fatalf("observation ordering = %+v", observation)
			}
		})
	}
}

type freshnessFailureClient struct{ err error }

func (c freshnessFailureClient) ObserveRepository(context.Context, workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error) {
	return workerproto.RepositoryObservation{}, c.err
}

func TestReadinessMissingRefRefreshFailuresRemainWaiting(t *testing.T) {
	for _, failure := range []struct {
		name      string
		err       error
		transport bool
	}{
		{"timeout", context.DeadlineExceeded, false},
		{"cancelled", context.Canceled, false},
		{"transport", errors.New("worker transport unavailable"), true},
	} {
		t.Run(failure.name, func(t *testing.T) {
			runner := &probeProcessRunner{exit: 2, err: &backlog.ProcessExitError{ExitCode: 2}}
			observer := probeObserver(map[string]repositoryProbeClient{"homelab": probeWorker(t, "homelab", runner, probeCredentialRef)})
			observer.now = func() time.Time { return probeNow }
			service, _ := probeReadinessService(t, observer, "homelab", "normandy")
			if probeViabilityMatrix(t, service).Outcome != backlogadmin.ViabilityImpossible {
				t.Fatal("missing ref was not confirmed")
			}
			attempts := 0
			observer.dial = func(context.Context, string) (repositoryProbeClient, func() error, error) {
				attempts++
				if failure.transport {
					return nil, nil, failure.err
				}
				return freshnessFailureClient{failure.err}, nil, nil
			}
			matrix := probeViabilityMatrix(t, service)
			candidate := probeCandidate(t, matrix, "homelab")
			if attempts == 0 {
				t.Fatal("cached negative prevented refresh")
			}
			if matrix.Outcome != backlogadmin.ViabilityAcceptedWaiting || candidate.Outcome != backlogadmin.ViabilityAcceptedWaiting ||
				probeHasReason(candidate, backlogadmin.ReasonRefNotFound) || !probeHasReason(candidate, backlogadmin.ReasonNetworkUnavailable) {
				t.Fatalf("refresh failure was not unobserved and temporary: %+v", matrix)
			}
			for _, reason := range candidate.Reasons {
				if reason.Permanent {
					t.Fatalf("refresh failure became permanent: %+v", reason)
				}
			}
			if _, found := observer.cache.Lookup(probeKey("homelab")); found {
				t.Fatal("failed refresh claims current evidence")
			}
		})
	}
}

func TestMissingRefStoredWhileObserverWaitsIsNotReused(t *testing.T) {
	runner := &probeProcessRunner{output: reachableOutput}
	observer := probeObserver(map[string]repositoryProbeClient{"homelab": probeWorker(t, "homelab", runner, probeCredentialRef)})
	key := probeKey("homelab")
	observer.cache.Store(backlog.RepositoryProbeObservation{Key: key, Class: backlog.RepositoryRefNotFound})
	lookedUp := make(chan struct{}, 1)
	observer.cache.Now = func() time.Time {
		select {
		case lookedUp <- struct{}{}:
		default:
		}
		return probeNow
	}
	observer.mu.Lock()
	done := make(chan error, 1)
	go func() {
		got, err := observer.ObserveRepository(context.Background(), key)
		if err == nil && got.Class != backlog.RepositoryAuthenticatedOK {
			err = errors.New("waiter reused missing ref")
		}
		done <- err
	}()
	<-lookedUp // First cache lookup precedes acquisition of observer.mu.
	// Store a newly confirmed missing ref while the caller waits for the mutex.
	observer.cache.Store(backlog.RepositoryProbeObservation{Key: key, Class: backlog.RepositoryRefNotFound})
	observer.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 1 {
		t.Fatal("second lookup reused missing evidence")
	}
}
