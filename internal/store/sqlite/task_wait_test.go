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
		AttemptID: attempt.ID, IssuedRevision: attempt.Revision,
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

// The fence is on the attempt's live turn, not on the revision the task was
// issued with.
//
// An attempt whose revision has merely moved on is what every real task holds,
// because the coordinator advances the attempt after it builds the package, so
// that registration must be accepted. An attempt that has moved on in a way
// that means the turn is gone must still be refused, and so must a thread that
// is not the one the attempt is running.
func TestTaskWaitFencesOnTheLiveTurnNotOnTheIssuedRevision(t *testing.T) {
	t.Run("an advanced revision still parks the attempt", func(t *testing.T) {
		ctx := context.Background()
		store, attempt, now := taskWaitFixture(t)
		request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
		advanced := attempt
		advanced.Revision += 3
		if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{advanced}}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RegisterTaskWait(ctx, request, now); err != nil {
			t.Fatalf("a task holding the revision it was issued could not park: %v", err)
		}
		parked := loadAttempt(t, store, attempt.ID)
		if parked.Progress != domain.ProgressWaitingExternal || parked.Revision != advanced.Revision+1 {
			t.Fatalf("the park did not fence on the live revision: %q at %d", parked.Progress, parked.Revision)
		}
	})

	for name, moveOn := range map[string]func(*domain.Attempt){
		"released back to the queue": func(a *domain.Attempt) {
			a.Progress, a.Control, a.AssignmentID, a.ThreadID = domain.ProgressReady, domain.ControlUnassigned, "", ""
		},
		"its turn ended and is being verified": func(a *domain.Attempt) {
			a.Progress, a.Control = domain.ProgressVerifying, domain.ControlStopped
		},
		"paused by an operator": func(a *domain.Attempt) {
			a.Control = domain.ControlPaused
		},
		"draining under a quota throttle": func(a *domain.Attempt) {
			a.Control = domain.ControlDraining
		},
	} {
		t.Run(name+" is refused", func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
			moved := attempt
			moveOn(&moved)
			moved.Revision++
			if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{moved}}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RegisterTaskWait(ctx, request, now); !errors.Is(err, domain.ErrTaskWaitTurnNotLive) {
				t.Fatalf("an attempt with no live turn accepted a wait: %v", err)
			}
			if waits, err := store.ListTaskWaits(ctx); err != nil || len(waits) != 0 {
				t.Fatalf("a refused registration left a record: %v %v", waits, err)
			}
		})
	}

	t.Run("another thread cannot park this attempt", func(t *testing.T) {
		ctx := context.Background()
		store, attempt, now := taskWaitFixture(t)
		request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
		request.ThreadID = "thread-elsewhere"
		if _, err := store.RegisterTaskWait(ctx, request, now); !errors.Is(err, domain.ErrTaskWaitForeignThread) {
			t.Fatalf("a foreign thread parked the attempt: %v", err)
		}
	})

	t.Run("a revision the coordinator never reached is refused", func(t *testing.T) {
		ctx := context.Background()
		store, attempt, now := taskWaitFixture(t)
		request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
		request.IssuedRevision = attempt.Revision + 10
		if _, err := store.RegisterTaskWait(ctx, request, now); err == nil ||
			!strings.Contains(err.Error(), "ahead of attempt") {
			t.Fatalf("a revision from the future was accepted: %v", err)
		}
	})
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
			assertOneConsistentOutcome(t, store, attempt.ID, name == "wait-first")
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
	assertOneConsistentOutcome(t, store, attempt.ID, errs[0] == nil)
}

// assertOneConsistentOutcome states the invariant the race exists to protect:
// the attempt either parked, with no completion recorded and a live wait
// holding it, or completed, with no wait claiming to hold it. Never a mixture,
// and never a park whose wait does not exist.
func assertOneConsistentOutcome(t *testing.T, store *Store, attemptID string, expectParked bool) {
	t.Helper()
	ctx := context.Background()
	final := loadAttempt(t, store, attemptID)
	live, err := store.LiveTaskWaitAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, held := live[attemptID]
	parked := final.Progress == domain.ProgressWaitingExternal
	if parked != expectParked {
		t.Fatalf("attempt progress %q, parked=%v, want parked=%v", final.Progress, parked, expectParked)
	}
	if parked {
		if !held {
			t.Fatal("the attempt is parked but no live wait holds it")
		}
		if final.Control != domain.ControlWaitingExternal {
			t.Fatalf("a parked attempt has control %q", final.Control)
		}
		if final.CompletedAt != nil || final.LastTurnOutcomeMarker == domain.TurnOutcomeDone {
			t.Fatalf("a parked attempt carries completion evidence: completedAt=%v marker=%q",
				final.CompletedAt, final.LastTurnOutcomeMarker)
		}
		return
	}
	if held {
		t.Fatalf("a live wait holds an attempt that is %q", final.Progress)
	}
	if final.LastTurnOutcomeMarker != domain.TurnOutcomeDone {
		t.Fatalf("the completing turn was not recorded: marker=%q", final.LastTurnOutcomeMarker)
	}
}

// The park outlasts the settlement. Between the moment a wait settles and the
// moment its wake reaches the thread the attempt has no running turn and one is
// coming, so anything that decides whether to collect must still see it parked.
// Reading liveness there is what collected a task mid-park, published a result
// for outputs it had not written, and deleted the identity record the resumed
// turn needed to name itself.
func TestAnAttemptStaysParkedUntilItsWakeReachesTheThread(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	assertParkState := func(stage string, wantLive, wantParked bool) {
		t.Helper()
		live, err := store.LiveTaskWaitAttempts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		parked, err := store.ParkedTaskWaitAttempts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, isLive := live[attempt.ID]
		gotWaitID, isParked := parked[attempt.ID]
		if isLive != wantLive || isParked != wantParked {
			t.Fatalf("%s: live=%v parked=%v, want live=%v parked=%v", stage, isLive, isParked, wantLive, wantParked)
		}
		if isParked && gotWaitID != wait.ID {
			t.Fatalf("%s: the park names wait %q, want %q", stage, gotWaitID, wait.ID)
		}
	}
	assertParkState("registered", true, true)

	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// The condition is decided and nothing has been told yet.
	assertParkState("settled", false, true)

	wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wakes = %+v err=%v", wakes, err)
	}
	// The resumption is committed, and its message has still not been sent.
	assertParkState("woken", false, true)

	if claimed, err := store.TransitionTaskWake(ctx, wait.ID, "pending", "sending", now.Add(2*time.Minute)); err != nil || !claimed {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	// A send that may or may not have landed is not evidence that it did.
	assertParkState("sending", false, true)

	if claimed, err := store.TransitionTaskWake(ctx, wait.ID, "sending", "delivered", now.Add(2*time.Minute)); err != nil || !claimed {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	// The thread has the outcome, so this wait holds nothing any more.
	assertParkState("delivered", false, false)
}

// A wake that cannot reach any turn stops parking the attempt rather than
// holding it forever: an abandoned wake is a decided one.
func TestAnAbandonedWakeStopsParkingTheAttempt(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WakeTaskWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionTaskWake(ctx, wait.ID, "pending", "abandoned", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	parked, err := store.ParkedTaskWaitAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, held := parked[attempt.ID]; held {
		t.Fatalf("an abandoned wake still parks attempt %q", attempt.ID)
	}
}

// A request ID names one park. Replaying it after the wait settled must never
// report that the attempt is parked: the agent would end its turn, the worker
// would collect, and verification would run against outputs never written.
// That is the observed failure rebuilt from a reused ID.
func TestTaskWaitReplayAfterSettlementIsRefused(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
	wait, err := store.RegisterTaskWait(ctx, request, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WakeTaskWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	resumed := loadAttempt(t, store, attempt.ID)
	if resumed.Progress != domain.ProgressActive {
		t.Fatalf("the attempt did not resume: %q", resumed.Progress)
	}

	// The resumed turn keeps the same attempt ID, so every equality check in
	// the replay branch passes. Only the wait's own state distinguishes them.
	_, err = store.RegisterTaskWait(ctx, request, now.Add(2*time.Minute))
	if !errors.Is(err, domain.ErrTaskWaitReplaySettled) {
		t.Fatalf("a replay after settlement was accepted: %v", err)
	}
	if after := loadAttempt(t, store, attempt.ID); after.Progress != domain.ProgressActive || after.Revision != resumed.Revision {
		t.Fatalf("the refused replay changed the attempt: %+v", after)
	}
	live, err := store.LiveTaskWaitAttempts(ctx)
	if err != nil || len(live) != 0 {
		t.Fatalf("a refused replay left a live wait: %v %v", live, err)
	}

	// A new request ID parks it again, which is the supported way to wait twice.
	second := taskWaitRegistration(loadAttempt(t, store, attempt.ID), "req-2", domain.WakeEach)
	if _, err := store.RegisterTaskWait(ctx, second, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if parked := loadAttempt(t, store, attempt.ID); parked.Progress != domain.ProgressWaitingExternal {
		t.Fatalf("a fresh request ID did not park the attempt: %q", parked.Progress)
	}
}

// A live wait whose attempt is no longer parked is a contradiction, so neither
// side is reported as fact.
func TestTaskWaitReplayOnAnUnparkedAttemptIsRefused(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	request := taskWaitRegistration(attempt, "req-1", domain.WakeEach)
	if _, err := store.RegisterTaskWait(ctx, request, now); err != nil {
		t.Fatal(err)
	}
	parked := loadAttempt(t, store, attempt.ID)
	parked.Progress = domain.ProgressActive
	parked.Control = domain.ControlRunning
	parked.Revision++
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{parked}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RegisterTaskWait(ctx, request, now.Add(time.Minute)); !errors.Is(err, domain.ErrTaskWaitReplayNotParked) {
		t.Fatalf("a replay on an unparked attempt was accepted: %v", err)
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

// Mixing the modes on one attempt is defined, not refused: an each wait that
// settles wakes the attempt, and the all waits still live are carried into the
// resumed turn unsettled. Any other reading makes each stop meaning each.
func TestTaskWaitMixedWakeModesWakeOnTheEachSettlement(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	slow, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-all", domain.WakeAll), now)
	if err != nil {
		t.Fatal(err)
	}
	parked := loadAttempt(t, store, attempt.ID)
	urgent, err := store.RegisterTaskWait(ctx, taskWaitRegistration(parked, "req-each", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, urgent.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitFailed, Reason: "the build broke"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Minute))
	if err != nil || len(wakes) != 1 || len(wakes[0].Waits) != 1 || wakes[0].Waits[0].ID != urgent.ID {
		t.Fatalf("the each settlement did not wake the attempt alone: %+v %v", wakes, err)
	}
	if resumed := loadAttempt(t, store, attempt.ID); resumed.Progress != domain.ProgressActive {
		t.Fatalf("the attempt did not resume: %q", resumed.Progress)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, wait := range waits {
		if wait.ID == slow.ID && (wait.Settled() || wait.Woken()) {
			t.Fatal("the unsettled all wait was settled or woken by the each settlement")
		}
	}
}

// all is proven here on its own, rather than by the absence of an each wait.
//
// One attempt holds an all set of two and one each wait. Settling a member of
// the all set moves nothing, because its set is incomplete. Settling the each
// wait resumes the attempt while a member of the all set is still live, and the
// resumed turn is given the outcomes that had settled by then and nothing else.
//
// The distinction matters because "every wait must settle" would pass a test
// that only ever settles a whole all set: it holds the park for the same
// reason. Only an each settlement that resumes an attempt with a live all wait
// beside it separates the two readings.
func TestAllHoldsItsIncompleteSetWhileAnEachWaitStillWakesTheAttempt(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	register := func(requestID string, wake domain.WakeMode) domain.TaskWait {
		t.Helper()
		// Every registration after the first parks an already-parked attempt,
		// which is the shape the fleet registered in and the one that moves the
		// attempt revision under the registration that follows it.
		wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(loadAttempt(t, store, attempt.ID), requestID, wake), now)
		if err != nil {
			t.Fatalf("registering %s: %v", requestID, err)
		}
		return wait
	}
	firstOfSet := register("req-all-1", domain.WakeAll)
	restOfSet := register("req-all-2", domain.WakeAll)
	urgent := register("req-each", domain.WakeEach)

	if _, err := store.SettleTaskWait(ctx, firstOfSet.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, Reason: "the first of the set",
	}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Minute)); err != nil || len(wakes) != 0 {
		t.Fatalf("an incomplete all set resumed the attempt: %v %v", wakes, err)
	}
	if held := loadAttempt(t, store, attempt.ID); held.Progress != domain.ProgressWaitingExternal {
		t.Fatalf("an incomplete all set did not hold the park: %q", held.Progress)
	}

	// The each wait settles as a failure, not a success: an outcome is an
	// outcome, and a failed condition wakes the task with its evidence.
	if _, err := store.SettleTaskWait(ctx, urgent.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitFailed, ExitCode: 2, Reason: "the build broke",
	}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(2*time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("the each settlement did not resume the attempt: %v %v", wakes, err)
	}
	resumed := loadAttempt(t, store, attempt.ID)
	if resumed.Progress != domain.ProgressActive || resumed.Control != domain.ControlResuming {
		t.Fatalf("the attempt did not resume: %q/%q", resumed.Progress, resumed.Control)
	}
	if prompt := wakes[0].Prompt(); !strings.Contains(prompt, "failed") || !strings.Contains(prompt, "the build broke") {
		t.Fatalf("the resumed turn was not told what failed: %q", prompt)
	}
	if len(wakes[0].Waits) != 2 {
		t.Fatalf("the resumed turn was given %d outcomes, want the two that had settled", len(wakes[0].Waits))
	}
	for _, wait := range wakes[0].Waits {
		if wait.ID == restOfSet.ID {
			t.Fatal("an unsettled wait was reported to the resumed turn as an outcome")
		}
	}

	// The rest of the set is carried into the resumed turn exactly as it was:
	// live, unsettled, and still polling.
	waits, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, wait := range waits {
		if wait.ID == restOfSet.ID && (!wait.Live() || wait.Woken()) {
			t.Fatalf("the live member of the all set was settled or woken: %+v", wait)
		}
	}
	// And the attempt it belongs to is running, not parked.
	parked, err := store.LiveTaskWaitAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if waitID, ok := parked[attempt.ID]; ok {
		t.Fatalf("the resumed attempt is reported parked on %q", waitID)
	}

	// When that last wait settles it reaches the turn that is already running,
	// as evidence rather than as a resumption. Silence is not an outcome.
	if _, err := store.SettleTaskWait(ctx, restOfSet.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, Reason: "the rest of the set",
	}, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	late, err := store.WakeTaskWaits(ctx, now.Add(3*time.Minute))
	if err != nil || len(late) != 1 || len(late[0].Waits) != 1 || late[0].Waits[0].ID != restOfSet.ID {
		t.Fatalf("the last settlement was not delivered: %+v %v", late, err)
	}
	if late[0].Resumption() {
		t.Fatal("evidence for a running turn was recorded as a resumption")
	}
	if after := loadAttempt(t, store, attempt.ID); after.Revision != resumed.Revision {
		t.Fatalf("delivering evidence moved the running attempt: %d then %d", resumed.Revision, after.Revision)
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
