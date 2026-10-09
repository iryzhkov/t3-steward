package workerruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The review reproduction uses the production journal-backed wait fence,
// including the coordinator acknowledgement of the stopped observation.
func TestReviewClosedPoolSurvivesRestart(t *testing.T) {
	for _, admission := range []domain.AdmissionState{domain.AdmissionClosed, domain.AdmissionDraining} {
		t.Run(string(admission), func(t *testing.T) {
			h := runningProviderSession(t, time.Second)
			policy := &workerproto.ProviderResumePolicy{MaxResumes: 3, MaxDelaySeconds: 3600,
				ClosedPools: []workerproto.ClosedQuotaPool{{PoolID: "codex-main", Admission: admission}}}
			h.runtime.config.LiveTaskWait = nil
			if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: policy, ParkedReported: true}); err != nil {
				t.Fatal(err)
			}
			h.driver.failTurn("turn-1", domain.ProviderErrorCapacity)
			reconcileOnce(t, h.runtime)
			snapshot, err := h.runtime.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: policy, ParkedReported: true,
				ObservedWorkerEpoch: snapshot.WorkerEpoch, ObservedSequence: snapshot.Sequence}); err != nil {
				t.Fatal(err)
			}
			reconcileOnce(t, h.runtime)
			h.now = h.now.Add(time.Second)
			reconcileOnce(t, h.runtime)
			if len(h.driver.resumeTokens) != 0 {
				t.Fatal("fixture not waiting")
			}
			h.open(t)
			h.runtime.config.LiveTaskWait = nil
			reconcileOnce(t, h.runtime)
			if len(h.driver.resumeTokens) != 0 {
				t.Fatalf("resumed on still-closed coordinator pool after restart: %d", len(h.driver.resumeTokens))
			}
			if state := h.resume(t); state.State != domain.ProviderResumeQuotaWait || !strings.HasPrefix(state.WaitReason, "quota-closed:") {
				t.Fatalf("state=%+v", state)
			}
			// Only a replacement policy reopening admission permits the same claim.
			if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: 3},
				ParkedReported: true, ObservedWorkerEpoch: snapshot.WorkerEpoch, ObservedSequence: snapshot.Sequence}); err != nil {
				t.Fatal(err)
			}
			reconcileOnce(t, h.runtime)
			if len(h.driver.resumeTokens) != 1 {
				t.Fatalf("resumes after reopening=%d", len(h.driver.resumeTokens))
			}
		})
	}
}

func TestReviewDisabledPolicyCancelsPendingResume(t *testing.T) {
	h := runningProviderSession(t, time.Second)
	h.driver.failTurn("turn-1", domain.ProviderErrorCapacity)
	reconcileOnce(t, h.runtime)
	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: 0}}); err != nil {
		t.Fatal(err)
	}
	// Recheck before the backoff ends, rather than waiting to discover the cap.
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 0 {
		t.Fatalf("sent %d resumes despite coordinator max_resumes=0", len(h.driver.resumeTokens))
	}
	state := h.resume(t)
	record := mustRecord(t, h.runtime, "assignment-1")
	if state.State != domain.ProviderResumeExhausted || state.Budget != 0 || state.ResumeAt != nil ||
		h.driver.collectFailureCalls != 1 || !strings.HasPrefix(record.Failure, ProviderErrorFailure) {
		t.Fatalf("state=%+v record=%+v", state, record)
	}
	h.now = h.now.Add(time.Second)
	h.open(t)
	reconcileOnce(t, h.runtime)
	if len(h.driver.resumeTokens) != 0 {
		t.Fatal("cancelled claim sent after restart")
	}
}

func TestProviderResumePendingClaimUsesCurrentMaximum(t *testing.T) {
	for _, limit := range []int{1, 2, 3} {
		t.Run(string(rune('0'+limit)), func(t *testing.T) {
			h := runningProviderSession(t, time.Second, time.Second, time.Second)
			h.driver.failTurn("turn-1", domain.ProviderErrorCapacity)
			reconcileOnce(t, h.runtime)
			h.now = h.now.Add(time.Second)
			reconcileOnce(t, h.runtime)
			reconcileOnce(t, h.runtime)
			h.driver.failTurn("turn-2", domain.ProviderErrorCapacity)
			reconcileOnce(t, h.runtime)
			if state := h.resume(t); state.Resumes != 2 {
				t.Fatalf("pending state=%+v", state)
			}
			if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: limit}}); err != nil {
				t.Fatal(err)
			}
			// A lower coordinator cap survives reopening the journal too.
			h.open(t)
			h.now = h.now.Add(time.Second)
			reconcileOnce(t, h.runtime)
			state := h.resume(t)
			if state.Budget != limit {
				t.Fatalf("budget=%d want %d", state.Budget, limit)
			}
			if limit < 2 {
				if len(h.driver.resumeTokens) != 1 || state.State != domain.ProviderResumeExhausted || h.driver.collectFailureCalls != 1 {
					t.Fatalf("resumes=%d state=%+v", len(h.driver.resumeTokens), state)
				}
			} else if len(h.driver.resumeTokens) != 2 || state.State != domain.ProviderResumeSent {
				t.Fatalf("resumes=%d state=%+v", len(h.driver.resumeTokens), state)
			}
		})
	}
}

func TestProviderResumePolicyReplacementIsDurableAndOwned(t *testing.T) {
	h := runningProviderSession(t)
	policy := &workerproto.ProviderResumePolicy{MaxResumes: 1, MaxDelaySeconds: 30,
		ClosedPools: []workerproto.ClosedQuotaPool{{PoolID: "codex-main", Admission: domain.AdmissionClosed}}}
	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: policy}); err != nil {
		t.Fatal(err)
	}
	policy.MaxResumes = 3
	policy.ClosedPools[0].PoolID = "changed-by-caller"
	if got := h.runtime.providerResume.Load(); got.MaxResumes != 1 || got.ClosedPools[0].PoolID != "codex-main" {
		t.Fatalf("caller mutated policy: %+v", got)
	}
	h.open(t)
	if got := h.runtime.providerResume.Load(); got == nil || got.MaxResumes != 1 || got.ClosedPools[0].PoolID != "codex-main" {
		t.Fatalf("restored policy=%+v", got)
	}
	// An invalid policy must leave both the live and durable policy unchanged.
	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ProviderResume: &workerproto.ProviderResumePolicy{MaxResumes: -1}}); err == nil {
		t.Fatal("invalid policy accepted")
	}
	h.open(t)
	if got := h.runtime.providerResume.Load(); got == nil || got.MaxResumes != 1 {
		t.Fatalf("invalid replacement changed policy=%+v", got)
	}
	// Absence from a later authenticated request is an explicit replacement;
	// older coordinators must still see no provider report.
	if err := h.runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{}); err != nil {
		t.Fatal(err)
	}
	h.open(t)
	if got := h.runtime.providerResume.Load(); got != nil {
		t.Fatalf("cleared policy restored=%+v", got)
	}
	if got := h.runtime.providerResumeSchedule(); len(got) != 3 {
		t.Fatalf("legacy schedule=%v", got)
	}
}
