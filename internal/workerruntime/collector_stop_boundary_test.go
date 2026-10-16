package workerruntime

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCoveredClaudeResumeRequiresAcquisitionAfterDurableStop(t *testing.T) {
	t.Parallel()
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.guard.Config = coveredClaudeConfig()
	old := claudeAnonymousStop(now)
	if err := f.store.SaveBucket(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	// Both windows are complete, below threshold and settled. Their
	// acquisition at now-3m follows the request but precedes the stop ACK.
	saveClaudeAcquisition(t, f, now)
	route := domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1", QuotaPoolID: "claude-main"}
	acquired := now.Add(-3 * time.Minute)
	beforeAcquisition := acquired.Add(-time.Minute)
	afterAcquisition := acquired.Add(2 * time.Minute)
	future := now.Add(time.Minute)
	zero := time.Time{}
	for _, tc := range []struct {
		name        string
		stoppedAt   *time.Time
		wantAllowed bool
	}{
		{"missing stop ACK", nil, false},
		{"zero stop ACK", &zero, false},
		{"future stop ACK", &future, false},
		{"acquisition during drain", &afterAcquisition, false},
		{"acquisition tied with stop ACK", &acquired, false},
		{"acquisition after actual stop", &beforeAcquisition, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pause := LocalThrottleRequest{Bucket: old.Key, RequestedAt: now.Add(-6 * time.Minute), StoppedAt: tc.stoppedAt}
			allowed, why, err := f.guard.ResumeAllowed(context.Background(), pause, route)
			if err != nil || allowed != tc.wantAllowed {
				t.Fatalf("allowed=%v want=%v why=%q err=%v", allowed, tc.wantAllowed, why, err)
			}
		})
	}
}

func TestCoveredClaudeReconcileDoesNotResumeFromAcquisitionDuringDrain(t *testing.T) {
	t.Parallel()
	now := runtimeTestNow
	requestedAt := now
	f := newRecoveryFixture(t, &now)
	f.guard.Config = coveredClaudeConfig()
	old := claudeAnonymousStop(now)
	if err := f.store.SaveBucket(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	runtime, driver := coveredClaudeRuntime(t, f.guard, &now,
		backlog.DispatchThreadActive, backlog.DispatchThreadStopped,
		backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	request := journalRecord(t, runtime).LocalThrottle
	if request == nil || !request.RequestedAt.Equal(requestedAt) || request.StoppedAt != nil {
		t.Fatalf("drain request lacks expected pre-stop boundary: %+v", request)
	}
	// A low acquisition is obtained three minutes after the drain request,
	// before the worker observes the actual stop five minutes after request.
	saveClaudeAcquisition(t, f, requestedAt.Add(6*time.Minute))
	now = requestedAt.Add(5 * time.Minute)
	renewLease(t, runtime, now.Add(time.Hour))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	pause := journalRecord(t, runtime).LocalThrottle
	if pause == nil || pause.StoppedAt == nil || !pause.StoppedAt.Equal(now) {
		t.Fatalf("actual stop was not durably acknowledged: %+v", pause)
	}
	now = requestedAt.Add(6 * time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 || journalRecord(t, runtime).LocalThrottle == nil {
		t.Fatalf("pre-ACK acquisition resumed owned thread: resumes=%d pause=%+v", driver.resumeCalls, journalRecord(t, runtime).LocalThrottle)
	}
	// A later complete acquisition follows the actual stop and settles.
	now = requestedAt.Add(10 * time.Minute)
	saveClaudeAcquisition(t, f, now)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 1 || journalRecord(t, runtime).LocalThrottle != nil {
		t.Fatalf("post-stop verified recovery did not resume: resumes=%d pause=%+v", driver.resumeCalls, journalRecord(t, runtime).LocalThrottle)
	}
	saved, err := f.store.LoadBucket(context.Background(), old.Key)
	if err != nil {
		t.Fatal(err)
	}
	if saved.ProbedAt != nil || saved.Key.AccountID != "" {
		t.Fatalf("recovery inferred a probe or rewrote historical ownership: %+v", saved)
	}
}
