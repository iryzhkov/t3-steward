package sqlite

import (
	"context"
	"strings"
	"sync"
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
		AssignmentID: attempt.AssignmentID, AssignmentEpoch: wait.Attention.AssignmentEpoch,
		WorkerID: wait.Attention.WorkerID, ThreadID: wait.ThreadID,
		RegisteredRevision: wait.RegisteredRevision, ContentDigest: wait.Attention.ContentDigest,
		DecisionDeadline: wait.Attention.DecisionDeadline, Kind: domain.AttentionApprove,
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

func TestAttentionConcurrentDecisionsChooseOneWinnerAndReplay(t *testing.T) {
	ctx := context.Background()
	store, _, wait, decision := registerAttentionFixture(t, domain.AttentionApproval)
	defer store.Close()
	now := wait.RegisteredAt.Add(time.Minute)
	other := decision
	other.ID = "decision-2"
	other.Reason = "other operator"
	type outcome struct {
		receipt domain.AttentionReceipt
		err     error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, candidate := range []domain.AttentionDecision{decision, other} {
		wg.Add(1)
		go func(candidate domain.AttentionDecision) {
			defer wg.Done()
			<-start
			_, receipt, err := store.DecideAttention(ctx, candidate, "remote:human", now)
			results <- outcome{receipt: receipt, err: err}
		}(candidate)
	}
	close(start)
	wg.Wait()
	close(results)
	applied, rejected := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		switch result.receipt.State {
		case domain.AttentionApplied:
			applied++
		case domain.AttentionRejected:
			rejected++
		}
	}
	if applied != 1 || rejected != 1 {
		t.Fatalf("concurrent decisions applied=%d rejected=%d", applied, rejected)
	}
	_, first, err := store.DecideAttention(ctx, decision, "remote:human", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := store.DecideAttention(ctx, decision, "remote:human", now.Add(2*time.Second))
	if err != nil || first.State != second.State || first.ReceivedAt != second.ReceivedAt {
		t.Fatalf("identical replay mutated receipt: first=%+v second=%+v err=%v", first, second, err)
	}
}

func TestAttentionDeadlineCancellationAndTakeoverFences(t *testing.T) {
	ctx := context.Background()
	t.Run("deadline edge", func(t *testing.T) {
		store, _, wait, decision := registerAttentionFixture(t, domain.AttentionApproval)
		defer store.Close()
		_, receipt, err := store.DecideAttention(ctx, decision, "remote:human", decision.DecisionDeadline)
		if err != nil || receipt.State != domain.AttentionRejected || !strings.Contains(receipt.Failure, "deadline") {
			t.Fatalf("deadline receipt=%+v err=%v", receipt, err)
		}
		listed, _ := store.ListTaskWaits(ctx)
		if len(listed) != 1 || !listed[0].Live() || !listed[0].Deadline.Equal(wait.Deadline) {
			t.Fatalf("deadline rejection changed wait: %+v", listed)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		store, _, wait, decision := registerAttentionFixture(t, domain.AttentionApproval)
		defer store.Close()
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for index := range records.Attempts {
			if records.Attempts[index].ID == wait.AttemptID {
				records.Attempts[index].Progress = domain.ProgressCancelled
				records.Attempts[index].Control = domain.ControlStopped
				records.Attempts[index].Revision++
			}
		}
		if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: records.Attempts}); err != nil {
			t.Fatal(err)
		}
		_, receipt, err := store.DecideAttention(ctx, decision, "remote:human", wait.RegisteredAt.Add(2*time.Second))
		if err != nil || receipt.State != domain.AttentionRejected {
			t.Fatalf("cancelled receipt=%+v err=%v", receipt, err)
		}
	})
	t.Run("assignment takeover", func(t *testing.T) {
		store, _, wait, decision := registerAttentionFixture(t, domain.AttentionApproval)
		defer store.Close()
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for index := range records.Assignments {
			if records.Assignments[index].ID == decision.AssignmentID {
				records.Assignments[index].Epoch++
				records.Assignments[index].WorkerID = "replacement-worker"
			}
		}
		if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: records.Assignments}); err != nil {
			t.Fatal(err)
		}
		_, receipt, err := store.DecideAttention(ctx, decision, "remote:human", wait.RegisteredAt.Add(time.Minute))
		if err != nil || receipt.State != domain.AttentionRejected || !strings.Contains(receipt.Failure, "lost") {
			t.Fatalf("takeover receipt=%+v err=%v", receipt, err)
		}
	})
}

func TestAttentionDecisionPreservesIndependentWaitAndReceiptLifecycle(t *testing.T) {
	ctx := context.Background()
	store, attempt, attention, decision := registerAttentionFixture(t, domain.AttentionApproval)
	defer store.Close()
	independent, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "independent", domain.WakeEach), attention.RegisteredAt)
	if err != nil {
		t.Fatal(err)
	}
	decided, receipt, err := store.DecideAttention(ctx, decision, "remote:human", attention.RegisteredAt.Add(time.Minute))
	if err != nil || receipt.State != domain.AttentionApplied {
		t.Fatalf("decision=%+v receipt=%+v err=%v", decided, receipt, err)
	}
	if len(decided.AttentionReceipts) != 2 ||
		decided.AttentionReceipts[0].State != domain.AttentionReceived ||
		decided.AttentionReceipts[1].State != domain.AttentionApplied {
		t.Fatalf("receipt history=%+v", decided.AttentionReceipts)
	}
	listed, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var independentLive bool
	for _, wait := range listed {
		if wait.ID == independent.ID {
			independentLive = wait.Live()
		}
	}
	if !independentLive {
		t.Fatal("attention decision settled an independent wait")
	}
	wakes, err := store.WakeTaskWaits(ctx, attention.RegisteredAt.Add(2*time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wake=%+v err=%v", wakes, err)
	}
	id := wakes[0].Waits[0].ID
	if ok, err := store.TransitionTaskWake(ctx, id, "pending", "sending", attention.RegisteredAt.Add(3*time.Minute)); err != nil || !ok {
		t.Fatalf("send claim=%v err=%v", ok, err)
	}
	if ok, err := store.TransitionTaskWake(ctx, id, "sending", "delivered", attention.RegisteredAt.Add(4*time.Minute)); err != nil || !ok {
		t.Fatalf("delivery=%v err=%v", ok, err)
	}
	if ok, err := store.TransitionTaskWake(ctx, id, "delivered", "observed", attention.RegisteredAt.Add(5*time.Minute)); err != nil || !ok {
		t.Fatalf("observation=%v err=%v", ok, err)
	}
	listed, err = store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, wait := range listed {
		if wait.ID != attention.ID {
			continue
		}
		states := []domain.AttentionReceiptState{}
		for _, event := range wait.AttentionReceipts {
			states = append(states, event.State)
		}
		want := []domain.AttentionReceiptState{domain.AttentionReceived, domain.AttentionApplied, domain.AttentionDelivered, domain.AttentionObserved}
		if len(states) != len(want) {
			t.Fatalf("lifecycle=%v", states)
		}
		for index := range want {
			if states[index] != want[index] {
				t.Fatalf("lifecycle=%v want=%v", states, want)
			}
		}
	}
}
