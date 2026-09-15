package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Task-bound waits live beside the native node waits of schema 13. They answer
// a different question: a node wait observes another task's outcome, while a
// task-bound wait parks the attempt that registered it.
const coordinatorMigrationV16 = `
CREATE TABLE IF NOT EXISTS coordinator_task_waits(
	id TEXT PRIMARY KEY,
	request_id TEXT NOT NULL UNIQUE,
	attempt_id TEXT NOT NULL,
	thread_id TEXT NOT NULL,
	record TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS coordinator_task_waits_attempt ON coordinator_task_waits(attempt_id);
CREATE TABLE IF NOT EXISTS coordinator_task_wait_events(
	id TEXT PRIMARY KEY,
	attempt_id TEXT NOT NULL,
	record TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS immutable_task_wait_event BEFORE UPDATE ON coordinator_task_wait_events
 BEGIN SELECT RAISE(ABORT,'task wait reconciliation events are immutable'); END;
`

func taskWaitID(requestID string) string { return "tw-" + requestID }

func saveTaskWaitTx(ctx context.Context, tx *sql.Tx, w domain.TaskWait) error {
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO coordinator_task_waits(id,request_id,attempt_id,thread_id,record) VALUES(?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET record=excluded.record`,
		w.ID, w.RequestID, w.AttemptID, w.ThreadID, raw)
	return err
}

// saveAttemptFencedTx writes an attempt only while its stored revision is still
// the one the caller read, and reports a lost race as a task-wait staleness so
// a registering agent sees why it was refused.
//
// Every task-bound wait transition goes through it, so a registration and a
// concurrent turn completion cannot both commit: exactly one wins, and the
// attempt can never be both waiting and terminal.
func saveAttemptFencedTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt, expected int64) error {
	if err := updateAttemptTx(ctx, tx, attempt, expected); err != nil {
		if errors.Is(err, ErrStaleAttemptRevision) {
			return fmt.Errorf("%w: attempt %q is no longer at revision %d",
				domain.ErrTaskWaitStaleRevision, attempt.ID, expected)
		}
		return err
	}
	return nil
}

func recordTaskWaitEventTx(ctx context.Context, tx *sql.Tx, event domain.TaskWaitReconciliation) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT OR IGNORE INTO coordinator_task_wait_events(id,attempt_id,record) VALUES(?,?,?)",
		event.ID, event.AttemptID, raw)
	return err
}

// RegisterTaskWait commits the wait record and the attempt's move to
// waiting-external in one fenced, idempotent transaction.
//
// Registration is the evidence that the current turn is parking rather than
// completing, so it cannot be a second write that a crash could lose. A
// terminal attempt is refused by name: that refusal is what stops a thread
// which has already lost its task authority from quietly acquiring a new
// reason to keep working.
//
// The fence is on the attempt's turn, and it is three separate statements,
// because the failures they catch are different and only one of them used to
// be checked:
//
//   - The attempt must still have a live turn (domain.Attempt.TurnLive). An
//     attempt that is being verified, was released, paused or drained has moved
//     on underneath the registering thread, whatever that thread believes.
//   - The registering thread must be the one the attempt runs on, when the
//     attempt names one at all. A thread may only park its own turn.
//   - The write itself is a compare-and-set against the revision read inside
//     this transaction, so a registration racing a turn completion still loses
//     exactly one of the two: whichever commits second is refused.
//
// What is deliberately not a fence is the revision the task was handed with its
// execution package. The coordinator stamps that when the package is built and
// then advances the attempt itself, when the worker claims the assignment and
// again when it reports the thread running, so the number the task holds is
// always behind by the time the turn starts. Requiring equality with it did not
// protect the attempt from anything: it refused every registration a real task
// could make, and the refusal it produced said "stale revision" for a number
// that was stale the moment it was written.
func (s *Store) RegisterTaskWait(ctx context.Context, request domain.TaskWaitRegistration, now time.Time) (domain.TaskWait, error) {
	var wait domain.TaskWait
	if err := request.Validate(); err != nil {
		return wait, err
	}
	if now.IsZero() {
		return wait, errors.New("task-bound wait registration needs a timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wait, err
	}
	defer tx.Rollback()

	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_task_waits WHERE request_id=?", request.RequestID).Scan(&raw)
	switch {
	case err == nil:
		if err = json.Unmarshal(raw, &wait); err != nil {
			return wait, err
		}
		if wait.AttemptID != request.AttemptID || wait.ThreadID != request.ThreadID ||
			wait.Wake != request.Wake || wait.MaxDuration != request.MaxDuration ||
			wait.WorkflowRunID != request.WorkflowRunID || wait.TaskID != request.TaskID {
			return domain.TaskWait{}, domain.ErrTaskWaitReplayChanged
		}
		// A replay may only report a park that is actually in force. The wait
		// and the attempt are checked separately because they can disagree: a
		// settled wait whose attempt resumed keeps the same attempt ID, so the
		// equality above passes and the caller would be told it is parked while
		// its turn runs on. That is the observed failure rebuilt from a reused
		// request ID.
		if !wait.Live() {
			return domain.TaskWait{}, fmt.Errorf("%w: %s settled as %q",
				domain.ErrTaskWaitReplaySettled, wait.ID, replaySettledOutcome(wait))
		}
		attempt, err := loadAttemptTx(ctx, tx, wait.AttemptID)
		if err != nil {
			return domain.TaskWait{}, err
		}
		if attempt.Progress != domain.ProgressWaitingExternal {
			return domain.TaskWait{}, fmt.Errorf("%w: attempt %q is %q",
				domain.ErrTaskWaitReplayNotParked, attempt.ID, attempt.Progress)
		}
		return wait, tx.Commit()
	case !errors.Is(err, sql.ErrNoRows):
		return wait, err
	}

	attempt, err := loadAttemptTx(ctx, tx, request.AttemptID)
	if err != nil {
		return wait, err
	}
	if attempt.WorkflowRunID != request.WorkflowRunID || attempt.TaskID != request.TaskID {
		return wait, fmt.Errorf("task-bound wait names attempt %q of a different task", request.AttemptID)
	}
	if attempt.Progress.Terminal() {
		return wait, domain.TaskWaitTerminalRefusal(attempt.Progress)
	}
	if !attempt.TurnLive() {
		return wait, fmt.Errorf("%w: attempt %q is %q/%q",
			domain.ErrTaskWaitTurnNotLive, attempt.ID, attempt.Progress, attempt.Control)
	}
	if attempt.ThreadID != "" && attempt.ThreadID != request.ThreadID {
		return wait, fmt.Errorf("%w: attempt %q runs on thread %q, not %q",
			domain.ErrTaskWaitForeignThread, attempt.ID, attempt.ThreadID, request.ThreadID)
	}
	if request.IssuedRevision > attempt.Revision {
		// The coordinator never issued this number: it is ahead of anything the
		// attempt has reached. Something rewrote the identity record, so it is
		// refused rather than read as an unusually fresh one.
		return wait, fmt.Errorf("task-bound wait names attempt revision %d, ahead of attempt %q at revision %d",
			request.IssuedRevision, attempt.ID, attempt.Revision)
	}

	expected := attempt.Revision
	id := taskWaitID(request.RequestID)
	wait = domain.TaskWait{
		ID: id, WorkflowRunID: request.WorkflowRunID, TaskID: request.TaskID,
		AttemptID: request.AttemptID, IssuedRevision: request.IssuedRevision,
		ThreadID: request.ThreadID, Wake: request.Wake, MaxDuration: request.MaxDuration,
		RequestID: request.RequestID, Name: request.Name, Condition: request.Condition,
		RegisteredRevision: expected + 1,
		RegisteredAt:       now.UTC(),
		Deadline:           now.Add(request.MaxDuration).UTC(),
	}
	attempt.Revision++
	attempt.Progress = domain.ProgressWaitingExternal
	attempt.Control = domain.ControlWaitingExternal
	attempt.LastTurnOutcomeID = "turn-outcome:" + id
	attempt.LastTurnOutcomeMarker = domain.TurnOutcomeWaiting
	attempt.CompletedAt = nil
	attempt.UpdatedAt = now.UTC()
	if err = saveAttemptFencedTx(ctx, tx, attempt, expected); err != nil {
		return domain.TaskWait{}, err
	}
	if err = saveTaskWaitTx(ctx, tx, wait); err != nil {
		return domain.TaskWait{}, err
	}
	return wait, tx.Commit()
}

// ListTaskWaits returns every task-bound wait the coordinator owns.
func (s *Store) ListTaskWaits(ctx context.Context) ([]domain.TaskWait, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return nil, err
	}
	return waits, tx.Commit()
}

// LiveTaskWaitAttempts maps every attempt that is parked on a task-bound wait
// to one of the waits parking it. It is what makes a done marker refusable,
// what the worker is told about its own assignments, and what refuses a result
// before any artifact enters coordinator custody.
//
// A live wait is not by itself a park, and reading it as one is what made an
// each wake come undone. Under each the first settlement resumes the attempt
// while the rest of its waits stay live and unsettled, by design: the resumed
// turn is an ordinary running turn that must be observed, collected and
// verified like any other. While "any live wait" stood for "parked", the
// coordinator went on telling the worker that the resumed assignment was
// parked, so the worker put the attempt back into waiting-external the moment
// that turn ended, the result was refused, and the task never finished. From
// outside it looked exactly as if the each settlement had not woken it.
//
// The attempt's own progress is therefore the authority on whether it is
// parked, and the wait records say why. The two are written in one fenced
// transaction at registration, so they cannot disagree about a park that is in
// force; after a wake they disagree on purpose, and this is the side that is
// right. A wait whose attempt has been removed parks nothing.
func (s *Store) LiveTaskWaitAttempts(ctx context.Context) (map[string]string, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return nil, err
	}
	live := make(map[string]string, len(waits))
	for _, wait := range waits {
		if !wait.Live() {
			continue
		}
		if existing, ok := live[wait.AttemptID]; !ok || wait.ID < existing {
			live[wait.AttemptID] = wait.ID
		}
	}
	for attemptID := range live {
		attempt, err := loadAttemptTx(ctx, tx, attemptID)
		if errors.Is(err, sql.ErrNoRows) {
			delete(live, attemptID)
			continue
		}
		if err != nil {
			return nil, err
		}
		if attempt.Progress != domain.ProgressWaitingExternal {
			delete(live, attemptID)
		}
	}
	return live, tx.Commit()
}

// ParkedTaskWaitAttempts maps every attempt a task-bound wait still parks to
// one of the waits parking it. It is what makes collection refusable.
//
// It is not LiveTaskWaitAttempts. An attempt stays parked after its wait
// settles, until the wake carrying that settlement has reached its thread: the
// turn that parked has ended and the resumed turn has not begun, so a collector
// that reads liveness there collects a task that is about to run again.
func (s *Store) ParkedTaskWaitAttempts(ctx context.Context) (map[string]string, error) {
	waits, err := s.ListTaskWaits(ctx)
	if err != nil {
		return nil, err
	}
	parked := make(map[string]string, len(waits))
	for _, wait := range waits {
		if !wait.Parking() {
			continue
		}
		if existing, ok := parked[wait.AttemptID]; !ok || wait.ID < existing {
			parked[wait.AttemptID] = wait.ID
		}
	}
	return parked, nil
}

// RecordTaskWaitReconciliations durably records lifecycle contradictions.
func (s *Store) RecordTaskWaitReconciliations(ctx context.Context, events []domain.TaskWaitReconciliation) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, event := range events {
		if event.ID == "" || event.AttemptID == "" {
			return errors.New("task wait reconciliation needs an identity and an attempt")
		}
		if err := recordTaskWaitEventTx(ctx, tx, event); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ListTaskWaitReconciliations returns the recorded lifecycle contradictions.
func (s *Store) ListTaskWaitReconciliations(ctx context.Context) ([]domain.TaskWaitReconciliation, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	events, err := loadJSON[domain.TaskWaitReconciliation](ctx, tx, "coordinator_task_wait_events")
	if err != nil {
		return nil, err
	}
	return events, tx.Commit()
}

// SettleTaskWait records one immutable outcome. A settled wait is never
// re-settled: the first outcome wins, so duplicate delivery of a check result
// after a restart cannot change what the task is told.
func (s *Store) SettleTaskWait(ctx context.Context, id string, result domain.TaskWaitResult, now time.Time) (domain.TaskWait, error) {
	var wait domain.TaskWait
	if id == "" || result.Outcome == "" || now.IsZero() {
		return wait, errors.New("task-bound wait settlement needs an ID, an outcome and a timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wait, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_task_waits WHERE id=?", id).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return wait, fmt.Errorf("task-bound wait %q is unknown", id)
		}
		return wait, err
	}
	if err = json.Unmarshal(raw, &wait); err != nil {
		return wait, err
	}
	if wait.Settled() {
		return wait, tx.Commit()
	}
	settled := now.UTC()
	if result.ObservedAt.IsZero() {
		result.ObservedAt = settled
	}
	if result.RanFor == 0 {
		result.RanFor = settled.Sub(wait.RegisteredAt)
	}
	wait.Result = &result
	wait.SettledAt = &settled
	if err = saveTaskWaitTx(ctx, tx, wait); err != nil {
		return wait, err
	}
	return wait, tx.Commit()
}

// ExpireTaskWaits enforces every wait's maximum duration.
//
// Directory writer bindings have no deadline by design and are held across a
// wait, so without this the fleet could be blocked by a parked task forever.
// Expiry wakes the task with a structured timeout rather than leaving it, and
// everything it holds, stuck.
func (s *Store) ExpireTaskWaits(ctx context.Context, now time.Time) ([]domain.TaskWait, error) {
	if now.IsZero() {
		return nil, errors.New("task-bound wait expiry needs a timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return nil, err
	}
	var expired []domain.TaskWait
	for _, wait := range waits {
		if !wait.Live() || now.Before(wait.Deadline) {
			continue
		}
		settled := now.UTC()
		wait.Result = &domain.TaskWaitResult{
			Outcome: domain.TaskWaitTimedOut, ExitCode: 2,
			Reason:     fmt.Sprintf("the wait exceeded its maximum duration of %s", wait.MaxDuration),
			RanFor:     settled.Sub(wait.RegisteredAt),
			ObservedAt: settled,
		}
		wait.SettledAt = &settled
		if err = saveTaskWaitTx(ctx, tx, wait); err != nil {
			return nil, err
		}
		if err = recordTaskWaitEventTx(ctx, tx, domain.TaskWaitReconciliation{
			ID: "expiry:" + wait.ID, Kind: domain.TaskWaitReconciliationExpired,
			AttemptID: wait.AttemptID, WaitID: wait.ID, ThreadID: wait.ThreadID,
			Detail:     wait.Result.Reason,
			ObservedAt: settled,
		}); err != nil {
			return nil, err
		}
		expired = append(expired, wait)
	}
	return expired, tx.Commit()
}

// CancelTaskWait settles a wait as cancelled so its attempt can resume. A wait
// that already has an outcome keeps it: cancellation never retracts evidence.
func (s *Store) CancelTaskWait(ctx context.Context, id string, now time.Time) (domain.TaskWait, error) {
	return s.SettleTaskWait(ctx, id, domain.TaskWaitResult{
		Outcome: domain.TaskWaitCancelled, ExitCode: 2, Reason: "the wait was cancelled",
	}, now)
}

// WakeTaskWaits resumes every parked attempt whose wake condition is met, and
// returns the evidence each resumed turn must be given.
//
// each wakes on the first settlement; all waits until every live member of the
// attempt has settled. A wait is marked woken inside the same transaction that
// resumes its attempt, so a coordinator restart, a duplicate tick or a lost
// wake response produce one wake, one resumed turn and one verification.
func (s *Store) WakeTaskWaits(ctx context.Context, now time.Time) ([]domain.TaskWaitWakeContext, error) {
	if now.IsZero() {
		return nil, errors.New("task-bound wait wake needs a timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return nil, err
	}
	byAttempt := make(map[string][]domain.TaskWait)
	for _, wait := range waits {
		byAttempt[wait.AttemptID] = append(byAttempt[wait.AttemptID], wait)
	}
	attemptIDs := make([]string, 0, len(byAttempt))
	for id := range byAttempt {
		attemptIDs = append(attemptIDs, id)
	}
	sort.Strings(attemptIDs)

	var wakes []domain.TaskWaitWakeContext
	for _, attemptID := range attemptIDs {
		ready, ok := taskWaitWakeSet(byAttempt[attemptID])
		if !ok {
			continue
		}
		attempt, err := loadAttemptTx(ctx, tx, attemptID)
		if err != nil {
			return nil, err
		}
		switch {
		case attempt.Progress.Terminal():
			// There is no turn left to tell. The evidence is kept on the wait
			// and the wake is closed so it is not retried forever.
			if err := markTaskWaitsWokenTx(ctx, tx, ready, attempt, "abandoned", false, now); err != nil {
				return nil, err
			}
			if err := recordTaskWaitEventTx(ctx, tx, domain.TaskWaitReconciliation{
				ID: "undelivered:" + ready[0].ID, Kind: domain.TaskWaitReconciliationUndelivered,
				AttemptID: attemptID, WaitID: ready[0].ID, ThreadID: attempt.ThreadID,
				Detail:     fmt.Sprintf("wait settled after attempt %q became %q; the outcome could not be delivered", attemptID, attempt.Progress),
				ObservedAt: now.UTC(),
			}); err != nil {
				return nil, err
			}
			continue
		case attempt.Progress != domain.ProgressWaitingExternal:
			// The attempt is running again: an earlier each settlement already
			// resumed it. This one still has to reach the agent, because a wait
			// that settles and says nothing is exactly the silence this design
			// refuses. It is delivered to the live turn and changes no state.
			if err := markTaskWaitsWokenTx(ctx, tx, ready, attempt, "pending", false, now); err != nil {
				return nil, err
			}
			wakes = append(wakes, domain.TaskWaitWakeContext{
				AttemptID: attemptID, ThreadID: attempt.ThreadID,
				AttemptRevision: attempt.Revision, Waits: ready,
			})
			continue
		}
		if settled, reason, err := taskWaitExecutionAbandonedTx(ctx, tx, attempt); err != nil {
			return nil, err
		} else if settled {
			// The execution that owned this attempt is gone while the attempt is
			// still parked. Resuming would hand a thread back work that no
			// worker is holding, so the contradiction is settled the only honest
			// way: the thread's authority is revoked before the attempt becomes
			// terminal, never after.
			if err := revokeTaskAuthorityTx(ctx, tx, attempt, reason, now); err != nil {
				return nil, err
			}
			if err := markTaskWaitsWokenTx(ctx, tx, ready, attempt, "abandoned", false, now); err != nil {
				return nil, err
			}
			continue
		}
		expected := attempt.Revision
		attempt.Revision++
		attempt.Progress = domain.ProgressActive
		// Resuming, not running: the attempt reacquires its provider slot and
		// executor capacity through the ordinary paths, and the worker moves it
		// to running when it observes the thread active again.
		attempt.Control = domain.ControlResuming
		attempt.LastTurnOutcomeID = ""
		attempt.LastTurnOutcomeMarker = ""
		attempt.UpdatedAt = now.UTC()
		if err := saveAttemptFencedTx(ctx, tx, attempt, expected); err != nil {
			return nil, err
		}
		if err := markTaskWaitsWokenTx(ctx, tx, ready, attempt, "pending", true, now); err != nil {
			return nil, err
		}
		wakes = append(wakes, domain.TaskWaitWakeContext{
			AttemptID: attemptID, ThreadID: attempt.ThreadID,
			AttemptRevision: attempt.Revision, Waits: ready,
		})
	}
	return wakes, tx.Commit()
}

func replaySettledOutcome(wait domain.TaskWait) domain.TaskWaitOutcome {
	if wait.Result == nil {
		return "settled"
	}
	return wait.Result.Outcome
}

func markTaskWaitsWokenTx(ctx context.Context, tx *sql.Tx, waits []domain.TaskWait, attempt domain.Attempt, delivery string, resumption bool, now time.Time) error {
	woken := now.UTC()
	for _, wait := range waits {
		wait.WokenAt = &woken
		wait.Delivery = delivery
		wait.WakeRevision = attempt.Revision
		wait.Resumption = resumption
		if wait.DeliveryID == "" {
			// Derived once and stored, so a retry sends the same command
			// instead of a new one that would start a second turn.
			wait.DeliveryID = fmt.Sprintf("task-wake:%s:%d", wait.ID, attempt.Revision)
		}
		if err := saveTaskWaitTx(ctx, tx, wait); err != nil {
			return err
		}
	}
	return nil
}

// TransitionTaskWake fences wake delivery ownership exactly as node waits do.
// Once sending is durable, a lost reply requires positive observation of the
// delivery ID; its absence never authorizes a second send.
func (s *Store) TransitionTaskWake(ctx context.Context, id, from, to string, now time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_task_waits WHERE id=?", id).Scan(&raw); err != nil {
		return false, err
	}
	var wait domain.TaskWait
	if err = json.Unmarshal(raw, &wait); err != nil {
		return false, err
	}
	if wait.Delivery != from {
		return false, nil
	}
	allowed := wait.Woken() && (from == "pending" && (to == "held" || to == "sending") ||
		from == "held" && (to == "pending" || to == "sending") ||
		(from == "sending" || from == "recovery-required") && (to == "delivered" || to == "recovery-required") ||
		to == "abandoned" && from != "delivered")
	if !allowed {
		return false, fmt.Errorf("invalid task wake transition %s to %s", from, to)
	}
	wait.Delivery = to
	if to == "delivered" {
		delivered := now.UTC()
		wait.DeliveredAt = &delivered
	}
	if err = saveTaskWaitTx(ctx, tx, wait); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// TaskWakesAwaitingDelivery lists the wakes whose message has not reached its
// thread yet, and closes the ones whose turn no longer exists.
//
// A wake is bound to the attempt revision it was committed against. If the
// attempt has moved past it, or become terminal, the message would arrive at a
// turn that did not park and cannot act on it, so the wake is abandoned with a
// durable record instead of being delivered or retried forever.
func (s *Store) TaskWakesAwaitingDelivery(ctx context.Context, now time.Time) ([]domain.TaskWaitWakeContext, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return nil, err
	}
	byAttempt := make(map[string][]domain.TaskWait)
	var order []string
	for _, wait := range waits {
		if !wait.Woken() || wait.Delivery == "delivered" || wait.Delivery == "abandoned" {
			continue
		}
		if _, seen := byAttempt[wait.AttemptID]; !seen {
			order = append(order, wait.AttemptID)
		}
		byAttempt[wait.AttemptID] = append(byAttempt[wait.AttemptID], wait)
	}
	sort.Strings(order)
	pending := make([]domain.TaskWaitWakeContext, 0, len(order))
	for _, attemptID := range order {
		members := byAttempt[attemptID]
		sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
		attempt, err := loadAttemptTx(ctx, tx, attemptID)
		if err != nil {
			return nil, err
		}
		stale := make([]domain.TaskWait, 0, len(members))
		live := make([]domain.TaskWait, 0, len(members))
		for _, wait := range members {
			if attempt.Progress.Terminal() || wait.WakeRevision != attempt.Revision {
				stale = append(stale, wait)
				continue
			}
			live = append(live, wait)
		}
		for _, wait := range stale {
			if wait.Delivery == "sending" || wait.Delivery == "recovery-required" {
				// A message may already exist. Abandoning it here would claim a
				// certainty we do not have, so it stays for observation.
				live = append(live, wait)
				continue
			}
			wait.Delivery = "abandoned"
			if err := saveTaskWaitTx(ctx, tx, wait); err != nil {
				return nil, err
			}
			if err := recordTaskWaitEventTx(ctx, tx, domain.TaskWaitReconciliation{
				ID: "undelivered:" + wait.ID, Kind: domain.TaskWaitReconciliationUndelivered,
				AttemptID: attemptID, WaitID: wait.ID, ThreadID: wait.ThreadID,
				Detail: fmt.Sprintf("wake for attempt revision %d could not be delivered: attempt is %q at revision %d",
					wait.WakeRevision, attempt.Progress, attempt.Revision),
				ObservedAt: now.UTC(),
			}); err != nil {
				return nil, err
			}
		}
		if len(live) == 0 {
			continue
		}
		sort.Slice(live, func(i, j int) bool { return live[i].ID < live[j].ID })
		pending = append(pending, domain.TaskWaitWakeContext{
			AttemptID: attemptID, ThreadID: attempt.ThreadID,
			AttemptRevision: attempt.Revision, Waits: live,
		})
	}
	return pending, tx.Commit()
}

// taskWaitWakeSet applies each and all to one attempt's waits and returns the
// settled, not yet woken waits that justify resuming it.
func taskWaitWakeSet(waits []domain.TaskWait) ([]domain.TaskWait, bool) {
	var ready []domain.TaskWait
	anyEach, anyLive := false, false
	for _, wait := range waits {
		switch {
		case wait.Live():
			anyLive = true
		case !wait.Woken():
			ready = append(ready, wait)
			anyEach = anyEach || wait.Wake == domain.WakeEach
		}
	}
	if len(ready) == 0 {
		return nil, false
	}
	if anyLive && !anyEach {
		return nil, false
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
	return ready, true
}

// RevokeTaskAuthority invalidates the dispatch token that lets a thread claim
// task effects, and only then records the attempt's terminal failure.
//
// The ordering is the whole lesson of the observed failure: a task became
// terminal while its thread was still authorized to act for it, and that thread
// went on to publish a release nobody owned. A thread that keeps talking after
// revocation is refused rather than obeyed.
func (s *Store) RevokeTaskAuthority(ctx context.Context, attemptID, reason string, now time.Time) (domain.Attempt, error) {
	var attempt domain.Attempt
	if attemptID == "" || reason == "" || now.IsZero() {
		return attempt, errors.New("task authority revocation needs an attempt, a reason and a timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return attempt, err
	}
	defer tx.Rollback()
	if attempt, err = loadAttemptTx(ctx, tx, attemptID); err != nil {
		return attempt, err
	}
	if err = revokeTaskAuthorityTx(ctx, tx, attempt, reason, now); err != nil {
		return attempt, err
	}
	if !attempt.Progress.Terminal() {
		completed := now.UTC()
		attempt.Revision++
		attempt.Progress = domain.ProgressFailed
		attempt.Control = domain.ControlStopped
		attempt.Failure = reason
		attempt.CompletedAt = &completed
		attempt.UpdatedAt = completed
	}
	return attempt, tx.Commit()
}

// revokeTaskAuthorityTx invalidates the dispatch token and records why, then
// fails the attempt. The statements run in this order inside one transaction:
// a task must never be terminal while its thread is still authorized to act for
// it, so the revocation is written first and the two commit together.
func revokeTaskAuthorityTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt, reason string, now time.Time) error {
	assignments, err := loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments")
	if err != nil {
		return err
	}
	threadID := attempt.ThreadID
	for _, assignment := range assignments {
		if assignment.AttemptID != attempt.ID || assignment.DispatchToken == "" {
			continue
		}
		if assignment.ThreadID != "" {
			threadID = assignment.ThreadID
		}
		assignment.DispatchToken = ""
		assignment.DispatchError = "task authority revoked: " + reason
		assignment.UpdatedAt = now.UTC()
		raw, err := json.Marshal(assignment)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE coordinator_assignments SET record=? WHERE id=?", raw, assignment.ID); err != nil {
			return err
		}
	}
	if err = recordTaskWaitEventTx(ctx, tx, domain.TaskWaitReconciliation{
		ID: "revocation:" + attempt.ID, Kind: domain.TaskWaitReconciliationAuthorityRevoked,
		AttemptID: attempt.ID, ThreadID: threadID, Detail: reason, ObservedAt: now.UTC(),
	}); err != nil {
		return err
	}
	if attempt.Progress.Terminal() {
		return nil
	}
	expected := attempt.Revision
	completed := now.UTC()
	attempt.Revision++
	attempt.Progress = domain.ProgressFailed
	attempt.Control = domain.ControlStopped
	attempt.Failure = reason
	attempt.CompletedAt = &completed
	attempt.UpdatedAt = completed
	return saveAttemptFencedTx(ctx, tx, attempt, expected)
}

// taskWaitExecutionAbandonedTx reports whether the execution that owned a
// parked attempt has settled underneath it.
//
// A worker that collected a parked attempt raced the registration and lost: the
// coordinator refused its result, but the worker considers the assignment
// finished and will never observe the resumed thread again. Resuming into that
// state would produce exactly the failure this record exists to prevent, a
// thread working with nobody owning what it does.
func taskWaitExecutionAbandonedTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt) (bool, string, error) {
	if attempt.AssignmentID == "" {
		return false, "", nil
	}
	assignment, err := loadAssignmentTx(ctx, tx, attempt.AssignmentID)
	if err != nil {
		return false, "", err
	}
	if assignment.AttemptID != attempt.ID {
		return true, fmt.Sprintf("assignment %q no longer owns parked attempt %q", assignment.ID, attempt.ID), nil
	}
	if assignment.State == domain.AssignmentCompleted || assignment.State == domain.AssignmentReleased {
		return true, fmt.Sprintf("assignment %q settled as %q while attempt %q was parked on a task-bound wait; the execution cannot be resumed",
			assignment.ID, assignment.State, attempt.ID), nil
	}
	return false, "", nil
}
