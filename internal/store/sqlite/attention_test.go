package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func registerAttentionFixture(t *testing.T, kind domain.AttentionKind) (*Store, domain.Attempt, domain.TaskWait, domain.AttentionDecision) {
	t.Helper()
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	request := taskWaitRegistration(attempt, "attention-1", domain.WakeEach)
	request.Kind = domain.WaitKindAttention
	request.Condition = ""
	request.Attention = &domain.AttentionRequest{
		Kind: kind, Prompt: "May this task continue?", AssignmentID: attempt.AssignmentID,
	}
	wait, err := store.RegisterTaskWait(ctx, request, now)
	if err != nil {
		t.Fatal(err)
	}
	decision := domain.AttentionDecision{
		ID: "decision-1", WaitID: wait.ID, RequestID: wait.RequestID,
		WorkflowRunID: wait.WorkflowRunID, TaskID: wait.TaskID, AttemptID: wait.AttemptID,
		AssignmentID: attempt.AssignmentID, ThreadID: wait.ThreadID,
		RegisteredRevision: wait.RegisteredRevision, Kind: domain.AttentionApprove,
		Reason: "approved by operator",
	}
	return store, attempt, wait, decision
}

func TestAttentionDecisionSurvivesRestartAndResumesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store, attempt, wait, decision := registerAttentionFixture(t, domain.AttentionApproval)
	now := wait.RegisteredAt.Add(time.Minute)
	decided, receipt, err := store.DecideAttention(ctx, decision, "remote:human", now)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != domain.AttentionApplied || decided.Result == nil || decided.Result.Fields["principal"] != "remote:human" {
		t.Fatalf("decision was not durably applied: wait=%+v receipt=%+v", decided, receipt)
	}
	replayed, replayReceipt, err := store.DecideAttention(ctx, decision, "remote:human", now.Add(time.Second))
	if err != nil || replayReceipt.State != receipt.State || replayReceipt.ReceivedAt != receipt.ReceivedAt || replayed.SettledAt == nil {
		t.Fatalf("identical replay changed outcome: wait=%+v receipt=%+v err=%v", replayed, replayReceipt, err)
	}
	changed := decision
	changed.Reason = "different"
	if _, _, err := store.DecideAttention(ctx, changed, "remote:human", now); err == nil || !strings.Contains(err.Error(), "replay changed") {
		t.Fatalf("changed replay accepted: %v", err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now); err == nil {
		t.Fatal("generic settlement synthesized approval")
	}

	path := store.path
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	wakes, err := reopened.WakeTaskWaits(ctx, now.Add(time.Minute))
	if err != nil || len(wakes) != 1 || wakes[0].AttemptID != attempt.ID {
		t.Fatalf("restart wake=%+v err=%v", wakes, err)
	}
	if second, err := reopened.WakeTaskWaits(ctx, now.Add(2*time.Minute)); err != nil || len(second) != 0 {
		t.Fatalf("duplicate wake=%+v err=%v", second, err)
	}
}

func TestAttentionDecisionFencesAndRejectedReceipt(t *testing.T) {
	ctx := context.Background()
	store, _, wait, decision := registerAttentionFixture(t, domain.AttentionApproval)
	defer store.Close()
	now := wait.RegisteredAt.Add(time.Minute)

	stale := decision
	stale.ID = "stale"
	stale.RegisteredRevision++
	if _, _, err := store.DecideAttention(ctx, stale, "remote:human", now); err == nil || !strings.Contains(err.Error(), "execution identity") {
		t.Fatalf("stale decision accepted: %v", err)
	}
	hold := decision
	hold.ID = "hold"
	hold.Kind = domain.AttentionHold
	_, receipt, err := store.DecideAttention(ctx, hold, "remote:human", now)
	if err != nil || receipt.State != domain.AttentionRejected || receipt.Failure == "" {
		t.Fatalf("hold receipt=%+v err=%v", receipt, err)
	}
	live, err := store.ListTaskWaits(ctx)
	if err != nil || len(live) != 1 || !live[0].Live() {
		t.Fatalf("rejected decision disturbed wait: %+v err=%v", live, err)
	}
}
