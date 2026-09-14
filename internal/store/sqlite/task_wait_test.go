package sqlite

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func taskWaitFixture(t *testing.T) (*Store, domain.Attempt, time.Time) {
	t.Helper()
	store, before, now := sinkStoreFixture(t)
	attempt := before.Attempts[0]
	attempt.Progress = domain.ProgressActive
	attempt.Control = domain.ControlRunning
	attempt.ThreadID = "thread-1"
	attempt.AssignmentID = "assign-1"
	assignment := domain.Assignment{
		ID: "assign-1", AttemptID: attempt.ID, WorkerID: "worker", Epoch: 1,
		State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch",
		ThreadID: "thread-1", CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	return store, attempt, now
}

func taskWaitRegistration(attempt domain.Attempt, requestID string, wake domain.WakeMode) domain.TaskWaitRegistration {
	return domain.TaskWaitRegistration{
		RequestID: requestID, WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
		AttemptID: attempt.ID, ExpectedRevision: uint64(attempt.Revision),
		ThreadID: attempt.ThreadID, Wake: wake, MaxDuration: time.Hour, Name: "ci",
		Condition: "gh run view",
	}
}

func loadAttempt(t *testing.T, store *Store, id string) domain.Attempt {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %q is missing", id)
	return domain.Attempt{}
}

// The whole park-and-resume path: registering parks the attempt without ending
// it, the settled condition resumes the same attempt exactly once, and the
// evidence the resumed turn receives says what happened.
func TestTaskWaitParksAndResumesOneAttemptOnce(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	parked := loadAttempt(t, store, attempt.ID)
	if parked.Progress != domain.ProgressWaitingExternal || parked.Control != domain.ControlWaitingExternal {
		t.Fatalf("attempt was not parked: %q/%q", parked.Progress, parked.Control)
	}
	if parked.Revision != attempt.Revision+1 || parked.CompletedAt != nil {
		t.Fatalf("parking did not fence or did not clear completion: revision=%d completed=%v", parked.Revision, parked.CompletedAt)
	}
	if parked.LastTurnOutcomeMarker != domain.TurnOutcomeWaiting {
		t.Fatalf("the parked turn was not recorded as waiting: %q", parked.LastTurnOutcomeMarker)
	}
	live, err := store.LiveTaskWaitAttempts(ctx)
	if err != nil || live[attempt.ID] != wait.ID {
		t.Fatalf("live waits=%v err=%v", live, err)
	}

	// Nothing resumes while the condition is unsettled.
	if wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Minute)); err != nil || len(wakes) != 0 {
		t.Fatalf("resumed before settlement: %v %v", wakes, err)
	}

	settled := now.Add(10 * time.Minute)
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, ExitCode: 0, Reason: "condition met", Output: "completed",
	}, settled); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, settled)
	if err != nil || len(wakes) != 1 || wakes[0].AttemptID != attempt.ID {
		t.Fatalf("wakes=%v err=%v", wakes, err)
	}
	if prompt := wakes[0].Prompt(); !strings.Contains(prompt, "met") || !strings.Contains(prompt, "completed") {
		t.Fatalf("the resumed turn was given no usable evidence: %q", prompt)
	}
	resumed := loadAttempt(t, store, attempt.ID)
	if resumed.Progress != domain.ProgressActive || resumed.Control != domain.ControlResuming {
		t.Fatalf("attempt did not resume: %q/%q", resumed.Progress, resumed.Control)
	}

	// A second pass, a duplicate tick and a coordinator restart must not wake
	// the attempt again.
	if wakes, err := store.WakeTaskWaits(ctx, settled.Add(time.Minute)); err != nil || len(wakes) != 0 {
		t.Fatalf("second wake=%v err=%v", wakes, err)
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
	if wakes, err := reopened.WakeTaskWaits(ctx, settled.Add(time.Hour)); err != nil || len(wakes) != 0 {
		t.Fatalf("wake repeated after a coordinator restart: %v %v", wakes, err)
	}
	after := loadAttempt(t, reopened, attempt.ID)
	if after.Revision != resumed.Revision {
		t.Fatalf("restart moved the resumed attempt: %d then %d", resumed.Revision, after.Revision)
	}
}

// Registration is idempotent under a repeated request ID and refuses a repeat
// that changed, so a retry after an ambiguous response is safe and a different
// request wearing an old ID is not silently honoured.
func TestTaskWaitRegistrationIsIdempotentAndRefusesChangedReplay(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
	first, err := store.RegisterTaskWait(ctx, request, now)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.RegisterTaskWait(ctx, request, now.Add(time.Minute))
	if err != nil || again.ID != first.ID || again.RegisteredAt != first.RegisteredAt {
		t.Fatalf("replay=%+v err=%v", again, err)
	}
	if parked := loadAttempt(t, store, attempt.ID); parked.Revision != attempt.Revision+1 {
		t.Fatalf("replay moved the attempt again: %d", parked.Revision)
	}
	changed := request
	changed.ThreadID = "another-thread"
	if _, err := store.RegisterTaskWait(ctx, changed, now); !errors.Is(err, domain.ErrTaskWaitReplayChanged) {
		t.Fatalf("changed replay accepted: %v", err)
	}
}

// A terminal attempt refuses a task-bound wait by name, and refusing leaves no
// orphan authority behind: the thread's dispatch token is untouched by the
// refusal, and revoking it is a separate, explicit act.
func TestTaskWaitRefusedOnTerminalAttempt(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	attempt.Progress = domain.ProgressFailed
	attempt.Control = domain.ControlStopped
	attempt.Revision++
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	_, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
	if !errors.Is(err, domain.ErrTaskWaitTerminalAttempt) {
		t.Fatalf("terminal attempt accepted a wait: %v", err)
	}
	if got := err.Error(); got != "attempt is terminal (failed); task-bound waits are refused" {
		t.Fatalf("refusal message is %q", got)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 0 {
		t.Fatalf("a refused registration left a record: %v %v", waits, err)
	}
}

// A registration that names a revision the attempt has moved past is refused
// rather than applied to whatever the attempt has since become.
func TestTaskWaitRefusesStaleAttemptRevision(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
	attempt.Revision += 3
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RegisterTaskWait(ctx, request, now); !errors.Is(err, domain.ErrTaskWaitStaleRevision) {
		t.Fatalf("stale registration accepted: %v", err)
	}
}

// The dangerous interleaving: a wait is registered at about the moment the turn
// ends. Both paths are fenced on the attempt revision, so exactly one commits,
// and the attempt is never both waiting and terminal.
func TestTaskWaitRaceWithTurnCompletionLeavesOneConsistentState(t *testing.T) {
	for _, name := range []string{"wait-first", "completion-first"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			complete := func() error {
				terminal := attempt
				terminal.Revision++
				terminal.Progress = domain.ProgressVerifying
				terminal.Control = domain.ControlStopped
				terminal.LastTurnOutcomeID = "turn-1"
				terminal.LastTurnOutcomeMarker = domain.TurnOutcomeDone
				return store.CommitTurnOutcomeTransitions(ctx, []domain.TurnOutcomeTransition{{
					OutcomeID: "turn-1", ExpectedAttemptRevision: attempt.Revision, Attempt: terminal,
				}})
			}
			register := func() error {
				_, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
				return err
			}
			var first, second func() error
			if name == "wait-first" {
				first, second = register, complete
			} else {
				first, second = complete, register
			}
			if err := first(); err != nil {
				t.Fatalf("the first path lost: %v", err)
			}
			if err := second(); err == nil {
				t.Fatal("both the registration and the completion committed")
			}
			final := loadAttempt(t, store, attempt.ID)
			waiting := final.Progress == domain.ProgressWaitingExternal
			if waiting && final.Progress.Terminal() {
				t.Fatal("the attempt is both waiting and terminal")
			}
			if name == "wait-first" && !waiting {
				t.Fatalf("the winning registration did not park the attempt: %q", final.Progress)
			}
			if name == "completion-first" && waiting {
				t.Fatal("a losing registration parked a completed attempt")
			}
		})
	}
}

// Racing the two paths concurrently against one store: exactly one of them
// commits, whichever the scheduler happens to run first.
func TestTaskWaitConcurrentRaceCommitsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	terminal := attempt
	terminal.Revision++
	terminal.Progress = domain.ProgressVerifying
	terminal.Control = domain.ControlStopped
	terminal.LastTurnOutcomeID = "turn-1"
	terminal.LastTurnOutcomeMarker = domain.TurnOutcomeDone
	errs := make([]error, 2)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		_, errs[0] = store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
	}()
	go func() {
		defer group.Done()
		errs[1] = store.CommitTurnOutcomeTransitions(ctx, []domain.TurnOutcomeTransition{{
			OutcomeID: "turn-1", ExpectedAttemptRevision: attempt.Revision, Attempt: terminal,
		}})
	}()
	group.Wait()
	won := 0
	for _, err := range errs {
		if err == nil {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("%d of the two fenced paths committed: %v", won, errs)
	}
	final := loadAttempt(t, store, attempt.ID)
	if final.Progress == domain.ProgressWaitingExternal && final.Progress.Terminal() {
		t.Fatal("the attempt is both waiting and terminal")
	}
}

// each resumes on the first settlement; all waits for every member.
func TestTaskWaitEachAndAllWakeSemantics(t *testing.T) {
	for _, mode := range []domain.WakeMode{domain.WakeEach, domain.WakeAll} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			first, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", mode), now)
			if err != nil {
				t.Fatal(err)
			}
			parked := loadAttempt(t, store, attempt.ID)
			secondRequest := taskWaitRegistration(parked, "req-2", mode)
			second, err := store.RegisterTaskWait(ctx, secondRequest, now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.SettleTaskWait(ctx, first.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if mode == domain.WakeEach && len(wakes) != 1 {
				t.Fatalf("each did not resume on the first settlement: %v", wakes)
			}
			if mode == domain.WakeAll {
				if len(wakes) != 0 {
					t.Fatalf("all resumed before every member settled: %v", wakes)
				}
				if _, err := store.SettleTaskWait(ctx, second.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(2*time.Minute)); err != nil {
					t.Fatal(err)
				}
				wakes, err = store.WakeTaskWaits(ctx, now.Add(2*time.Minute))
				if err != nil || len(wakes) != 1 || len(wakes[0].Waits) != 2 {
					t.Fatalf("all did not resume once every member settled: %v %v", wakes, err)
				}
			}
		})
	}
}

// A wait that outlives its maximum duration wakes the task with a structured
// timeout, because a parked task holds directory bindings that have no deadline
// of their own and would otherwise block the fleet forever.
func TestTaskWaitExpiryWakesWithStructuredTimeout(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeAll), now)
	if err != nil {
		t.Fatal(err)
	}
	late := wait.Deadline.Add(time.Minute)
	expired, err := store.ExpireTaskWaits(ctx, late)
	if err != nil || len(expired) != 1 {
		t.Fatalf("expiry=%v err=%v", expired, err)
	}
	result := expired[0].Result
	if result == nil || result.Outcome != domain.TaskWaitTimedOut || result.ExitCode != 2 || result.RanFor <= 0 {
		t.Fatalf("timeout evidence is not structured: %+v", result)
	}
	events, err := store.ListTaskWaitReconciliations(ctx)
	if err != nil || len(events) != 1 || events[0].Kind != domain.TaskWaitReconciliationExpired {
		t.Fatalf("expiry was not recorded: %v %v", events, err)
	}
	wakes, err := store.WakeTaskWaits(ctx, late)
	if err != nil || len(wakes) != 1 {
		t.Fatalf("an expired wait did not release its task: %v %v", wakes, err)
	}
	if resumed := loadAttempt(t, store, attempt.ID); resumed.Progress != domain.ProgressActive {
		t.Fatalf("an expired wait left the task parked: %q", resumed.Progress)
	}
	if !strings.Contains(wakes[0].Prompt(), "timed-out") {
		t.Fatalf("the woken task was not told it timed out: %q", wakes[0].Prompt())
	}
}

// A cancelled wait releases its attempt and never retracts an outcome the task
// has already been given.
func TestTaskWaitCancellationReleasesWithoutRetractingEvidence(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet, Reason: "met"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.CancelTaskWait(ctx, wait.ID, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Result.Outcome != domain.TaskWaitMet {
		t.Fatalf("cancellation retracted a settled outcome: %q", cancelled.Result.Outcome)
	}
}

// A worker that collected a parked attempt raced the registration and lost. Its
// assignment is settled, so the execution cannot be resumed. The wake must not
// hand the thread its task back: the authority is revoked and the attempt fails
// honestly instead.
func TestWakeRevokesAuthorityWhenTheExecutionWasAbandoned(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settled := records.Assignments[0]
	settled.State = domain.AssignmentCompleted
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{settled}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Minute))
	if err != nil || len(wakes) != 0 {
		t.Fatalf("an abandoned execution was resumed: %v %v", wakes, err)
	}
	final := loadAttempt(t, store, attempt.ID)
	if final.Progress != domain.ProgressFailed || final.Failure == "" {
		t.Fatalf("the contradiction did not fail the attempt: %+v", final)
	}
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range records.Assignments {
		if assignment.AttemptID == attempt.ID && assignment.DispatchToken != "" {
			t.Fatal("the abandoned thread kept its task authority")
		}
	}
	events, err := store.ListTaskWaitReconciliations(ctx)
	if err != nil || len(events) != 1 || events[0].Kind != domain.TaskWaitReconciliationAuthorityRevoked {
		t.Fatalf("the contradiction was not recorded: %v %v", events, err)
	}
	// A second pass changes nothing further: one revocation, one terminal state.
	if wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Hour)); err != nil || len(wakes) != 0 {
		t.Fatalf("a second pass acted again: %v %v", wakes, err)
	}
	if after := loadAttempt(t, store, attempt.ID); after.Revision != final.Revision {
		t.Fatalf("a second pass moved the terminal attempt: %d then %d", final.Revision, after.Revision)
	}
}

// A thread must lose its task authority before its task is marked terminal.
func TestRevokeTaskAuthorityInvalidatesDispatchBeforeTerminal(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	final, err := store.RevokeTaskAuthority(ctx, attempt.ID, "irreconcilable lifecycle contradiction", now)
	if err != nil {
		t.Fatal(err)
	}
	if !final.Progress.Terminal() || final.Failure == "" {
		t.Fatalf("the attempt did not become terminal: %+v", final)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range records.Assignments {
		if assignment.AttemptID == attempt.ID && assignment.DispatchToken != "" {
			t.Fatal("a terminal task left its thread authorized to act for it")
		}
	}
	events, err := store.ListTaskWaitReconciliations(ctx)
	if err != nil || len(events) != 1 || events[0].Kind != domain.TaskWaitReconciliationAuthorityRevoked {
		t.Fatalf("revocation was not recorded: %v %v", events, err)
	}
	// The revoked attempt refuses a new task-bound wait, so the abandoned
	// thread cannot acquire a fresh reason to keep working.
	if _, err := store.RegisterTaskWait(ctx, taskWaitRegistration(final, "req-after", domain.WakeEach), now); !errors.Is(err, domain.ErrTaskWaitTerminalAttempt) {
		t.Fatalf("a revoked attempt accepted a new wait: %v", err)
	}
}
