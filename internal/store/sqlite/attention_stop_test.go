package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func latestAttentionReceipt(t *testing.T, store *Store, waitID string) domain.AttentionReceipt {
	t.Helper()
	waits, err := store.ListTaskWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, wait := range waits {
		if wait.ID == waitID && len(wait.AttentionReceipts) != 0 {
			return wait.AttentionReceipts[len(wait.AttentionReceipts)-1]
		}
	}
	t.Fatalf("attention wait %q has no receipt", waitID)
	return domain.AttentionReceipt{}
}

func prepareAttentionStopBinding(t *testing.T, store *Store, attempt domain.Attempt, at time.Time) {
	t.Helper()
	ctx := context.Background()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for index := range records.Assignments {
		if records.Assignments[index].ID == attempt.AssignmentID {
			records.Assignments[index].WorkerEpoch = "worker-epoch-1"
			records.Assignments[index].Route = domain.ProviderRoute{WorkerID: "worker", ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool-1"}
		}
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: records.Assignments}); err != nil {
		t.Fatal(err)
	}
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("load binding snapshot: %+v %v", snapshots, err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ObservedAt, snapshot.ValidUntil = at, at.Add(time.Hour)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: attempt.AssignmentID, AssignmentEpoch: 1, State: domain.AssignmentClaimed,
		Control: domain.ControlWaitingExternal, ThreadID: attempt.ThreadID, WorkspacePath: "/workspace", ObservedAt: at,
	}}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
}

func acknowledgeAttentionStop(t *testing.T, store *Store, at time.Time) domain.ThrottleAttemptRecord {
	t.Helper()
	records, err := store.LoadThrottleAttemptRecords(context.Background())
	if err != nil || len(records) != 1 {
		t.Fatalf("load stop record: records=%+v err=%v", records, err)
	}
	record := records[0]
	record.Revision++
	record.Delivery = domain.ThrottleDeliveryAcknowledged
	record.Result = domain.ThrottleResultStopped
	record.Control = domain.ControlDraining
	record.AcknowledgedAt = &at
	record.UpdatedAt = at
	if err := store.CommitThrottleAttemptTransitions(context.Background(), []domain.ThrottleAttemptTransition{{
		ExpectedRevision: record.Revision - 1, Record: record,
	}}); err != nil {
		t.Fatal(err)
	}
	return record
}

func stoppedAttentionSnapshot(t *testing.T, store *Store, record domain.ThrottleAttemptRecord, sequence int64, at time.Time) domain.WorkerSnapshot {
	t.Helper()
	snapshots, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("load worker snapshot: snapshots=%+v err=%v", snapshots, err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence = sequence
	snapshot.ObservedAt = at
	snapshot.ValidUntil = at.Add(time.Hour)
	snapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: record.Command.AssignmentID, AssignmentEpoch: record.Command.AssignmentEpoch,
		State: domain.AssignmentReleased, Control: domain.ControlStopped,
		ThreadID: record.Command.ThreadID, WorkspacePath: record.Command.WorkspacePath, ObservedAt: at,
		Journal: &domain.WorkerJournalExcerpt{Phase: "stopped", ThreadState: "stopped", UpdatedAt: at},
	}}
	return snapshot
}

func TestAttentionStopRemainsUnconfirmedAcrossRestartUntilExactContainment(t *testing.T) {
	ctx := context.Background()
	store, attempt, wait, decision := registerAttentionFixture(t, domain.AttentionDirection)
	decision.Kind, decision.Reason = domain.AttentionStop, "operator stop"
	prepareAttentionStopBinding(t, store, attempt, wait.RegisteredAt.Add(30*time.Second))
	appliedAt := wait.RegisteredAt.Add(time.Minute)
	decided, receipt, err := store.DecideAttentionForCoordinator(ctx, decision, "remote:human", "coord-1", appliedAt)
	if err != nil || receipt.State != domain.AttentionApplied || receipt.CommandID == "" || receipt.CommandDigest == "" {
		t.Fatalf("apply stop: wait=%+v receipt=%+v err=%v", decided, receipt, err)
	}
	stopped := loadAttempt(t, store, attempt.ID)
	if stopped.Progress != domain.ProgressCancelled || stopped.Control != domain.ControlDraining {
		t.Fatalf("logical cancellation claimed containment: %+v", stopped)
	}
	if wakes, err := store.WakeTaskWaits(ctx, appliedAt.Add(time.Second)); err != nil || len(wakes) != 0 {
		t.Fatalf("stop produced a resume wake: %+v err=%v", wakes, err)
	}
	path := store.path
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	records, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil || len(records) != 1 || records[0].Delivery != domain.ThrottleDeliveryPending {
		t.Fatalf("restart lost pending containment: records=%+v err=%v", records, err)
	}
	if got := latestAttentionReceipt(t, store, wait.ID); got.State != domain.AttentionApplied {
		t.Fatalf("unreachable worker fabricated containment: %+v", got)
	}

	ackAt := appliedAt.Add(2 * time.Minute)
	record := acknowledgeAttentionStop(t, store, ackAt)
	if got := latestAttentionReceipt(t, store, wait.ID); got.State != domain.AttentionDelivered || got.ObservedAt != nil {
		t.Fatalf("stop acknowledgement was not delivery-only: %+v", got)
	}
	observedAt := ackAt.Add(time.Minute)
	if err := store.SaveWorkerSnapshot(ctx, stoppedAttentionSnapshot(t, store, record, 3, observedAt)); err != nil {
		t.Fatal(err)
	}
	if got := latestAttentionReceipt(t, store, wait.ID); got.State != domain.AttentionObserved || got.ObservedAt == nil {
		t.Fatalf("positive containment did not advance once: %+v", got)
	}
	recordsAfter, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range recordsAfter.Assignments {
		if assignment.ID == decision.AssignmentID && assignment.State != domain.AssignmentReleased {
			t.Fatalf("observed containment retained live custody: %+v", assignment)
		}
	}
	finalAttempt := loadAttempt(t, store, attempt.ID)
	if finalAttempt.Control != domain.ControlStopped {
		t.Fatalf("observed containment did not stop attempt: %+v", finalAttempt)
	}
	if err := store.SaveWorkerSnapshot(ctx, stoppedAttentionSnapshot(t, store, record, 4, observedAt.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	for _, candidate := range waits {
		if candidate.ID == wait.ID && len(candidate.AttentionReceipts) != 4 {
			t.Fatalf("replayed observation advanced twice: %+v", candidate.AttentionReceipts)
		}
	}
}

func TestAttentionStopEarlierAttemptCustodyBlocksObservedOnly(t *testing.T) {
	ctx := context.Background()
	store, attempt, wait, decision := registerAttentionFixture(t, domain.AttentionDirection)
	defer store.Close()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	earlier := attempt
	earlier.ID, earlier.Number, earlier.AssignmentID = "attempt-earlier", attempt.Number-1, "assign-earlier"
	earlier.Progress, earlier.Control, earlier.Revision = domain.ProgressCancelled, domain.ControlDraining, 1
	earlierAssignment := records.Assignments[0]
	earlierAssignment.ID, earlierAssignment.AttemptID = earlier.AssignmentID, earlier.ID
	earlierAssignment.State, earlierAssignment.Epoch = domain.AssignmentClaimed, 1
	earlierAssignment.LeaseToken, earlierAssignment.DispatchToken = "lease-earlier", "dispatch-earlier"
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{earlier}, Assignments: []domain.Assignment{earlierAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	decision.Kind, decision.Reason = domain.AttentionStop, "stop all task execution"
	prepareAttentionStopBinding(t, store, attempt, wait.RegisteredAt.Add(30*time.Second))
	appliedAt := wait.RegisteredAt.Add(time.Minute)
	if _, receipt, err := store.DecideAttentionForCoordinator(ctx, decision, "remote:human", "coord-1", appliedAt); err != nil || receipt.State != domain.AttentionApplied {
		t.Fatalf("apply stop receipt=%+v err=%v", receipt, err)
	}
	record := acknowledgeAttentionStop(t, store, appliedAt.Add(time.Minute))
	if err := store.SaveWorkerSnapshot(ctx, stoppedAttentionSnapshot(t, store, record, 3, appliedAt.Add(2*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if got := latestAttentionReceipt(t, store, wait.ID); got.State != domain.AttentionDelivered {
		t.Fatalf("earlier attempt custody did not block observed: %+v", got)
	}
	current := loadAttempt(t, store, attempt.ID)
	if current.Control != domain.ControlStopped {
		t.Fatalf("current execution was not contained: %+v", current)
	}
}

func TestAttentionStopWithoutDurableBindingRecordsRejection(t *testing.T) {
	ctx := context.Background()
	store, attempt, wait, decision := registerAttentionFixture(t, domain.AttentionDirection)
	defer store.Close()
	decision.Kind, decision.Reason = domain.AttentionStop, "operator stop"
	_, receipt, err := store.DecideAttentionForCoordinator(
		ctx, decision, "remote:human", "coord-1", wait.RegisteredAt.Add(time.Minute),
	)
	if err != nil || receipt.State != domain.AttentionRejected {
		t.Fatalf("missing binding was not durably rejected: receipt=%+v err=%v", receipt, err)
	}
	if got := latestAttentionReceipt(t, store, wait.ID); got.State != domain.AttentionRejected || got.Failure == "" {
		t.Fatalf("missing binding rejection was not durable: %+v", got)
	}
	current := loadAttempt(t, store, attempt.ID)
	if current.Progress != domain.ProgressWaitingExternal || current.Control != domain.ControlWaitingExternal {
		t.Fatalf("rejected stop mutated execution: %+v", current)
	}
	records, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil || len(records) != 0 {
		t.Fatalf("rejected stop produced a command: records=%+v err=%v", records, err)
	}
}

func TestAttentionStopRacesResumeWithOneAuthorityWinner(t *testing.T) {
	ctx := context.Background()
	store, _, wait, resume := registerAttentionFixture(t, domain.AttentionDirection)
	defer store.Close()
	stop := resume
	stop.ID, stop.Kind, stop.Reason = "decision-stop", domain.AttentionStop, "stop instead"
	prepareAttentionStopBinding(t, store, domain.Attempt{ID: resume.AttemptID, AssignmentID: resume.AssignmentID, ThreadID: resume.ThreadID}, wait.RegisteredAt.Add(30*time.Second))
	type outcome struct {
		receipt domain.AttentionReceipt
		err     error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, candidate := range []domain.AttentionDecision{resume, stop} {
		go func(decision domain.AttentionDecision) {
			<-start
			_, receipt, err := store.DecideAttentionForCoordinator(ctx, decision, "remote:human", "coord-1", wait.RegisteredAt.Add(time.Minute))
			results <- outcome{receipt: receipt, err: err}
		}(candidate)
	}
	close(start)
	applied, rejected := 0, 0
	for range 2 {
		result := <-results
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
		t.Fatalf("resume/stop winners applied=%d rejected=%d", applied, rejected)
	}
}
