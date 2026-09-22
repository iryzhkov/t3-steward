package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
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

const coordinatorMigrationV19 = `
CREATE INDEX IF NOT EXISTS coordinator_task_waits_ready
	ON coordinator_task_waits(attempt_id)
	WHERE json_extract(record, '$.settledAt') IS NOT NULL
	  AND json_extract(record, '$.wokenAt') IS NULL;
CREATE INDEX IF NOT EXISTS coordinator_task_waits_live
	ON coordinator_task_waits(attempt_id)
	WHERE json_extract(record, '$.settledAt') IS NULL;
`

func taskWaitID(requestID string) string { return "tw-" + requestID }

func sameAttentionRegistration(stored, requested *domain.AttentionRequest) bool {
	if stored == nil || requested == nil {
		return stored == nil && requested == nil
	}
	return stored.Kind == requested.Kind && stored.Prompt == requested.Prompt &&
		stored.AssignmentID == requested.AssignmentID
}

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
			wait.WorkflowRunID != request.WorkflowRunID || wait.TaskID != request.TaskID ||
			wait.Kind.OrShell() != request.Kind.OrShell() || !sameAttentionRegistration(wait.Attention, request.Attention) {
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
	if request.Wake == domain.WakeAll {
		// A task-bound all set is the attempt's live all waits, and it is all
		// local kinds or all coordinator kinds: the two sides settle on
		// different hosts and a mixed set is not guaranteed by the contract.
		if err := refuseMixedTaskWaitSetTx(ctx, tx, request); err != nil {
			return wait, err
		}
	}
	if request.Kind.Coordinator() {
		// A coordinator kind is checked against the records it will be settled
		// from, so a condition that can never settle, or already holds, is
		// refused before anything is parked, as a local check would be.
		if err := validateStructuredRegistrationTx(ctx, tx, &request, now); err != nil {
			return wait, err
		}
	}
	if request.Attention != nil {
		assignment, err := loadAssignmentTx(ctx, tx, request.Attention.AssignmentID)
		if err != nil {
			return wait, err
		}
		if attempt.AssignmentID != assignment.ID || assignment.AttemptID != attempt.ID ||
			assignment.State != domain.AssignmentClaimed {
			return wait, errors.New("attention request assignment no longer owns the live attempt")
		}
		request.Attention.AssignmentEpoch = assignment.Epoch
		request.Attention.WorkerID = assignment.WorkerID
		request.Attention.ContentDigest = domain.AttentionRequestContentDigest(request.Attention.Kind, request.Attention.Prompt)
		request.Attention.DecisionDeadline = now.Add(request.MaxDuration).UTC()
	}

	expected := attempt.Revision
	id := taskWaitID(request.RequestID)
	wait = domain.TaskWait{
		ID: id, WorkflowRunID: request.WorkflowRunID, TaskID: request.TaskID,
		AttemptID: request.AttemptID, IssuedRevision: request.IssuedRevision,
		ThreadID: request.ThreadID, Wake: request.Wake, MaxDuration: request.MaxDuration,
		RequestID: request.RequestID, Name: request.Name, Condition: request.Condition,
		Kind: request.Kind.OrShell(), OrTimeout: request.OrTimeout, Node: request.Node, Quota: request.Quota, Attention: request.Attention,
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

// refuseMixedTaskWaitSetTx refuses a --wake all registration whose kind is
// on the other side (local or coordinator) from a live all wait of the same
// attempt, naming both members.
func refuseMixedTaskWaitSetTx(ctx context.Context, tx *sql.Tx, request domain.TaskWaitRegistration) error {
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return err
	}
	for _, wait := range waits {
		if wait.AttemptID != request.AttemptID || !wait.Live() || wait.Wake != domain.WakeAll {
			continue
		}
		if wait.Kind.OrShell().Coordinator() == request.Kind.OrShell().Coordinator() {
			continue
		}
		return fmt.Errorf("a --wake all set is all local kinds or all coordinator kinds: wait %s (%s, %s) and this %s wait (%s) would mix them; register this one with --wake each, or wait for %s to settle",
			wait.ID, wait.Kind.OrShell(), sideOf(wait.Kind), request.Kind.OrShell(), sideOf(request.Kind), wait.ID)
	}
	return nil
}

func sideOf(kind domain.WaitKind) string {
	if kind.OrShell().Coordinator() {
		return "a coordinator kind"
	}
	return "a local kind"
}

// validateStructuredRegistrationTx checks a coordinator-kind registration
// against the records it will be settled from, canonicalises its target and
// fills the condition text and name when the caller gave none.
func validateStructuredRegistrationTx(ctx context.Context, tx *sql.Tx, request *domain.TaskWaitRegistration, now time.Time) error {
	records, err := nodeStateRecordsTx(ctx, tx)
	if err != nil {
		return err
	}
	switch {
	case request.Node != nil:
		if request.Node.State == "" {
			request.Node.State = domain.NodeStateTerminal
		}
		obs, err := resolveNodeState(*request.Node, records)
		if err != nil {
			return fmt.Errorf("node wait target: %w", err)
		}
		if request.Node.Target.RunID == request.WorkflowRunID && obs.Target.TaskID == domain.SinkTaskID(request.WorkflowRunID) {
			return fmt.Errorf("a task cannot wait for its own run %s to settle: the run cannot settle while this attempt is parked; wait for a sibling task with --node %s/<task>", request.WorkflowRunID, request.WorkflowRunID)
		}
		if obs.Outcome != "" {
			return fmt.Errorf("the condition already holds (%s is %s: %s), so there is nothing to park for", request.Node.Target, obs.Progress, obs.Reason)
		}
		request.Node.Target = obs.Target
		if request.Condition == "" {
			request.Condition = request.Node.String()
		}
	case request.Attention != nil:
		request.Condition = "attention " + string(request.Attention.Kind) + ": " + request.Attention.Prompt
	case request.Quota != nil:
		if request.Quota.Reset && request.Quota.ResetAt == nil {
			resetAt, err := quotaResetAt(*request.Quota, records)
			if err != nil {
				return err
			}
			request.Quota.ResetAt = resetAt
		}
		obs, err := observeQuota(*request.Quota, records, now.UTC())
		if err != nil {
			return err
		}
		if obs.Outcome != "" {
			return fmt.Errorf("the condition already holds (%s), so there is nothing to park for", obs.Reason)
		}
		if request.Condition == "" {
			request.Condition = request.Quota.String()
		}
	default:
		return fmt.Errorf("a %s wait needs its structured condition", request.Kind)
	}
	if request.Name == "" {
		request.Name = request.Condition
	}
	return nil
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
// Two separate defects met in this function and each fix, applied alone,
// reintroduced the other. A wait that is merely live does not park an attempt
// an each settlement has already resumed, because the remaining waits stay live
// by design; reading it as a park made the worker put the resumed turn straight
// back into waiting-external. And an attempt whose progress has already moved on
// is still parked while its wake sits undelivered, because the parked turn has
// ended and the resumed one has not begun.
//
// So the attempt's own progress is the authority while it is parked, and the
// delivery state is the authority once it has been woken. Neither alone is
// enough: progress alone collects a task mid-wake, and wait liveness alone
// re-parks a task that is already running again.
func (s *Store) ParkedTaskWaitAttempts(ctx context.Context) (map[string]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	waits, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return nil, err
	}
	// Collection may query before the delivery runner. Reconcile downgrade
	// ambiguity here too, so apparently delivered per-record rows cannot
	// release the worker's collection fence.
	if err := reconcileTaskWakeGroupsTx(ctx, tx, waits, time.Now().UTC()); err != nil {
		return nil, err
	}
	parked := make(map[string]string, len(waits))
	waking := make(map[string]bool, len(waits))
	for _, wait := range waits {
		if !wait.Parking() {
			continue
		}
		if existing, ok := parked[wait.AttemptID]; !ok || wait.ID < existing {
			parked[wait.AttemptID] = wait.ID
		}
		// A wake that has not reached the thread keeps the attempt parked on
		// its own, whatever its progress now says.
		if wait.Woken() {
			waking[wait.AttemptID] = true
		}
	}
	for attemptID := range parked {
		if waking[attemptID] {
			continue
		}
		attempt, err := loadAttemptTx(ctx, tx, attemptID)
		if errors.Is(err, sql.ErrNoRows) {
			delete(parked, attemptID)
			continue
		}
		if err != nil {
			return nil, err
		}
		if attempt.Progress != domain.ProgressWaitingExternal {
			delete(parked, attemptID)
		}
	}
	return parked, tx.Commit()
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
	if wait.Kind == domain.WaitKindAttention {
		return wait, errors.New("attention waits may only be settled by an authenticated attention decision")
	}
	if wait, err = settleTaskWaitTx(ctx, tx, wait, result, now); err != nil {
		return wait, err
	}
	return wait, tx.Commit()
}

// settleTaskWaitTx records one immutable outcome inside a transaction; a
// wait that is already settled is returned as it is.
func settleTaskWaitTx(ctx context.Context, tx *sql.Tx, wait domain.TaskWait, result domain.TaskWaitResult, now time.Time) (domain.TaskWait, error) {
	if wait.Settled() {
		return wait, nil
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
	return wait, saveTaskWaitTx(ctx, tx, wait)
}

// DecideAttention is the in-process compatibility entry point. Network-facing
// callers use DecideAttentionForCoordinator so the trusted server identity is
// bound into stop commands rather than accepted from request JSON.
func (s *Store) DecideAttention(ctx context.Context, decision domain.AttentionDecision, requestedBy string, now time.Time) (domain.TaskWait, domain.AttentionReceipt, error) {
	return s.DecideAttentionForCoordinator(ctx, decision, requestedBy, "coordinator", now)
}

// DecideAttentionForCoordinator records an authenticated response against the
// exact parked execution identity. Receipt facts and settlement are committed
// together. Resume/approval use the ordinary wake transaction; stop instead
// commits a cancellation barrier and durable hard-stop intent atomically.
func (s *Store) DecideAttentionForCoordinator(ctx context.Context, decision domain.AttentionDecision, requestedBy, coordinatorID string, now time.Time) (domain.TaskWait, domain.AttentionReceipt, error) {
	var wait domain.TaskWait
	var receipt domain.AttentionReceipt
	if err := decision.Validate(); err != nil {
		return wait, receipt, err
	}
	if requestedBy == "" || coordinatorID == "" || now.IsZero() {
		return wait, receipt, errors.New("attention decision needs an authenticated principal, coordinator and timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wait, receipt, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_task_waits WHERE id=?", decision.WaitID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return wait, receipt, fmt.Errorf("attention wait %q is unknown", decision.WaitID)
		}
		return wait, receipt, err
	}
	if err = json.Unmarshal(raw, &wait); err != nil {
		return wait, receipt, err
	}
	var replay *domain.AttentionReceipt
	for index := range wait.AttentionReceipts {
		prior := &wait.AttentionReceipts[index]
		if prior.Decision.ID != decision.ID {
			continue
		}
		if !reflect.DeepEqual(prior.Decision, decision) || prior.RequestedBy != requestedBy {
			return wait, receipt, errors.New("attention decision replay changed immutable content or principal")
		}
		replay = prior
	}
	if replay != nil {
		return wait, *replay, tx.Commit()
	}
	if wait.Kind != domain.WaitKindAttention || wait.Attention == nil {
		return wait, receipt, errors.New("attention decision names a wait that is not an attention request")
	}
	request := wait.Attention
	if wait.RequestID != decision.RequestID || wait.WorkflowRunID != decision.WorkflowRunID ||
		wait.TaskID != decision.TaskID || wait.AttemptID != decision.AttemptID ||
		wait.ThreadID != decision.ThreadID || wait.RegisteredRevision != decision.RegisteredRevision ||
		request.AssignmentID != decision.AssignmentID || request.AssignmentEpoch != decision.AssignmentEpoch ||
		request.WorkerID != decision.WorkerID || request.ContentDigest != decision.ContentDigest ||
		!request.DecisionDeadline.Equal(decision.DecisionDeadline) || !wait.Deadline.Equal(decision.DecisionDeadline) {
		return wait, receipt, errors.New("attention decision execution identity or immutable request content does not match the registered request")
	}
	attempt, err := loadAttemptTx(ctx, tx, wait.AttemptID)
	if err != nil {
		return wait, receipt, err
	}
	assignment, err := loadAssignmentTx(ctx, tx, decision.AssignmentID)
	if err != nil {
		return wait, receipt, err
	}
	runs, err := loadJSON[domain.WorkflowRun](ctx, tx, "coordinator_workflow_runs")
	if err != nil {
		return wait, receipt, err
	}
	tasks, err := loadJSON[domain.Task](ctx, tx, "coordinator_tasks")
	if err != nil {
		return wait, receipt, err
	}
	runCurrent, taskCurrent, workflowID := false, false, ""
	for _, run := range runs {
		if run.ID == decision.WorkflowRunID && !run.Progress.Terminal() {
			runCurrent, workflowID = true, run.WorkflowID
		}
	}
	for _, task := range tasks {
		if task.ID == decision.TaskID && task.WorkflowID == workflowID && workflowID != "" {
			taskCurrent = true
		}
	}
	fenceCurrent := runCurrent && taskCurrent && wait.Live() &&
		attempt.WorkflowRunID == wait.WorkflowRunID && attempt.TaskID == wait.TaskID &&
		attempt.AssignmentID == assignment.ID && attempt.AssignmentID == decision.AssignmentID &&
		attempt.ThreadID == wait.ThreadID && attempt.Revision >= wait.RegisteredRevision &&
		attempt.Progress == domain.ProgressWaitingExternal && attempt.Control == domain.ControlWaitingExternal &&
		assignment.AttemptID == attempt.ID && assignment.WorkerID == decision.WorkerID &&
		assignment.Epoch == decision.AssignmentEpoch && assignment.State == domain.AssignmentClaimed
	received := now.UTC()
	receivedReceipt := domain.AttentionReceipt{
		Decision: decision, RequestedBy: requestedBy, State: domain.AttentionReceived, ReceivedAt: received,
	}
	wait.AttentionReceipts = append(wait.AttentionReceipts, receivedReceipt)
	reject := func(failure string) (domain.TaskWait, domain.AttentionReceipt, error) {
		rejected := receivedReceipt
		rejected.State, rejected.Failure = domain.AttentionRejected, failure
		wait.AttentionReceipts = append(wait.AttentionReceipts, rejected)
		if saveErr := saveTaskWaitTx(ctx, tx, wait); saveErr != nil {
			return wait, rejected, saveErr
		}
		return wait, rejected, tx.Commit()
	}
	if !now.Before(decision.DecisionDeadline) {
		return reject("attention decision deadline has expired")
	}
	if !fenceCurrent {
		return reject("attention request lost its current run, task, attempt, assignment, worker or thread fence")
	}
	for _, prior := range wait.AttentionReceipts[:len(wait.AttentionReceipts)-1] {
		if prior.State == domain.AttentionApplied || prior.State == domain.AttentionDelivered || prior.State == domain.AttentionObserved {
			return reject(fmt.Sprintf("attention wait already has applied decision %s", prior.Decision.ID))
		}
	}
	allowed := decision.Kind == domain.AttentionApprove && request.Kind == domain.AttentionApproval ||
		decision.Kind == domain.AttentionResume && request.Kind == domain.AttentionDirection ||
		decision.Kind == domain.AttentionStop
	if !allowed {
		return reject("the requested decision is not supported for this attention request")
	}
	if decision.Kind == domain.AttentionStop {
		snapshot, found, bindingErr := loadWorkerSnapshotTx(ctx, tx, assignment.WorkerID)
		if bindingErr != nil {
			return wait, receivedReceipt, bindingErr
		}
		if !found || snapshot.WorkerEpoch != assignment.WorkerEpoch || snapshot.CoordinatorEpoch < 1 {
			return reject("attention stop has no durable worker/coordinator epoch binding")
		}
		exactWorkspace := false
		for _, observation := range snapshot.Assignments {
			if observation.AssignmentID == assignment.ID && observation.AssignmentEpoch == assignment.Epoch &&
				observation.ThreadID == assignment.ThreadID && observation.WorkspacePath != "" {
				exactWorkspace = true
				break
			}
		}
		if !exactWorkspace || assignment.Route.QuotaPoolID == "" {
			return reject("attention stop has no exact workspace and route observation")
		}
		return applyAttentionStopTx(ctx, tx, wait, attempt, assignment, decision, receivedReceipt, requestedBy, coordinatorID, now)
	}
	applied := received
	receipt = receivedReceipt
	receipt.State, receipt.AppliedAt = domain.AttentionApplied, &applied
	wait.AttentionReceipts = append(wait.AttentionReceipts, receipt)
	result := domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, ExitCode: 0, Reason: decision.Reason, ObservedAt: received,
		Fields: map[string]string{
			"decision": string(decision.Kind), "decision-id": decision.ID, "principal": requestedBy,
			"request": decision.RequestID, "run": decision.WorkflowRunID, "task": decision.TaskID,
			"attempt": decision.AttemptID, "assignment": decision.AssignmentID,
			"assignment-epoch": fmt.Sprint(decision.AssignmentEpoch), "worker": decision.WorkerID,
			"thread": decision.ThreadID, "revision": fmt.Sprint(decision.RegisteredRevision),
			"content-digest": decision.ContentDigest, "deadline": decision.DecisionDeadline.Format(time.RFC3339Nano),
		},
	}
	if wait, err = settleTaskWaitTx(ctx, tx, wait, result, now); err != nil {
		return wait, receipt, err
	}
	return wait, receipt, tx.Commit()
}

func applyAttentionStopTx(
	ctx context.Context,
	tx *sql.Tx,
	wait domain.TaskWait,
	attempt domain.Attempt,
	assignment domain.Assignment,
	decision domain.AttentionDecision,
	received domain.AttentionReceipt,
	requestedBy, coordinatorID string,
	now time.Time,
) (domain.TaskWait, domain.AttentionReceipt, error) {
	snapshot, found, err := loadWorkerSnapshotTx(ctx, tx, assignment.WorkerID)
	if err != nil {
		return wait, received, err
	}
	if !found || snapshot.WorkerEpoch != assignment.WorkerEpoch || snapshot.CoordinatorEpoch < 1 {
		return wait, received, errors.New("attention stop has no durable worker/coordinator epoch binding")
	}
	workspace := ""
	for _, observation := range snapshot.Assignments {
		if observation.AssignmentID == assignment.ID && observation.AssignmentEpoch == assignment.Epoch &&
			observation.ThreadID == assignment.ThreadID {
			workspace = observation.WorkspacePath
			break
		}
	}
	if workspace == "" || assignment.Route.QuotaPoolID == "" {
		return wait, received, errors.New("attention stop has no exact workspace and route observation")
	}
	directiveID := attentionStopStableID("attention-stop-directive", decision.ID)
	command := domain.ThrottleCommand{
		ID: attentionStopStableID("attention-stop-command", decision.ID), DirectiveID: directiveID,
		AttemptID: attempt.ID, AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		WorkerID: assignment.WorkerID, ThreadID: assignment.ThreadID, WorkspacePath: workspace,
		Route: assignment.Route, Kind: domain.ThrottleCommandHardStop, QuotaPoolID: assignment.Route.QuotaPoolID,
		Reason: decision.Reason, CreatedAt: now.UTC(),
		AttentionStop: &domain.AttentionStopCommand{
			CoordinatorID: coordinatorID, CoordinatorEpoch: snapshot.CoordinatorEpoch, Principal: requestedBy,
			DecisionID: decision.ID, WaitID: decision.WaitID, RequestID: decision.RequestID,
			WorkflowRunID: decision.WorkflowRunID, TaskID: decision.TaskID,
			RegisteredRevision: decision.RegisteredRevision, AppliedRevision: attempt.Revision + 1,
			RequestDigest: decision.ContentDigest,
		},
	}
	command.AttentionStop.CommandDigest = domain.AttentionStopCommandDigest(command)
	record := domain.ThrottleAttemptRecord{
		DirectiveID: directiveID, AttemptID: attempt.ID, Revision: 1, Command: command,
		Delivery: domain.ThrottleDeliveryPending, Control: domain.ControlDraining, UpdatedAt: now.UTC(),
	}
	rawRecord, err := json.Marshal(record)
	if err != nil {
		return wait, received, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO coordinator_throttle_attempts(directive_id, attempt_id, revision, delivery, record) VALUES (?, ?, ?, ?, ?)`,
		record.DirectiveID, record.AttemptID, record.Revision, record.Delivery, rawRecord); err != nil {
		return wait, received, fmt.Errorf("persist attention stop intent: %w", err)
	}
	next := attempt
	next.Progress, next.Control = domain.ProgressCancelled, domain.ControlDraining
	next.Revision++
	next.AdminForceStart = false
	next.UpdatedAt = now.UTC()
	completed := now.UTC()
	next.CompletedAt = &completed
	if err = updateAdminAttemptTx(ctx, tx, next, attempt.Revision); err != nil {
		return wait, received, err
	}
	applied := now.UTC()
	receipt := received
	receipt.State, receipt.AppliedAt = domain.AttentionApplied, &applied
	receipt.CommandID, receipt.CommandDigest = command.ID, command.AttentionStop.CommandDigest
	wait.AttentionReceipts = append(wait.AttentionReceipts, receipt)
	result := domain.TaskWaitResult{
		Outcome: domain.TaskWaitCancelled, ExitCode: 2, Reason: decision.Reason, ObservedAt: applied,
		Fields: map[string]string{
			"decision": string(decision.Kind), "decision-id": decision.ID, "principal": requestedBy,
			"coordinator": coordinatorID, "coordinator-epoch": fmt.Sprint(snapshot.CoordinatorEpoch),
			"request": decision.RequestID, "run": decision.WorkflowRunID, "task": decision.TaskID,
			"attempt": decision.AttemptID, "assignment": decision.AssignmentID,
			"assignment-epoch": fmt.Sprint(decision.AssignmentEpoch), "worker": decision.WorkerID,
			"thread": decision.ThreadID, "workspace": workspace, "revision": fmt.Sprint(next.Revision),
			"content-digest": decision.ContentDigest, "command": command.ID,
			"command-digest": command.AttentionStop.CommandDigest,
		},
	}
	if wait, err = settleTaskWaitTx(ctx, tx, wait, result, now); err != nil {
		return wait, receipt, err
	}
	return wait, receipt, tx.Commit()
}

func attentionStopStableID(prefix, id string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + id))
	return prefix + "-" + hex.EncodeToString(sum[:12])
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
		if wait.OrTimeout {
			// --or-timeout: the deadline is an expected end of the wait, not a
			// contradiction to record. The outcome still says timed-out so the
			// resumed turn can tell it from the condition being met.
			wait.Result.ExitCode = 0
			wait.Result.Reason = fmt.Sprintf("the deadline of %s passed, which this wait treats as a normal outcome (--or-timeout)", wait.MaxDuration)
		}
		wait.SettledAt = &settled
		if err = saveTaskWaitTx(ctx, tx, wait); err != nil {
			return nil, err
		}
		expired = append(expired, wait)
		if wait.OrTimeout {
			continue
		}
		if err = recordTaskWaitEventTx(ctx, tx, domain.TaskWaitReconciliation{
			ID: "expiry:" + wait.ID, Kind: domain.TaskWaitReconciliationExpired,
			AttemptID: wait.AttemptID, WaitID: wait.ID, ThreadID: wait.ThreadID,
			Detail:     wait.Result.Reason,
			ObservedAt: settled,
		}); err != nil {
			return nil, err
		}
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
type TaskWakeCutoffs struct {
	Worker              map[string]TaskWakeCutoff
	Pool                map[string]TaskWakeCutoff
	AuthorizedWorkers   map[string]TaskWakeWorkerAuthorization
	AdmissionValidAfter time.Time
	// ContendingWorker and ContendingPool restrict this admission pass to wakes
	// sharing at least one named bottleneck. Empty values preserve the normal
	// fleet-wide wake pass.
	ContendingWorker string
	ContendingPool   string
}

type TaskWakeWorkerAuthorization struct {
	WorkerEpoch      string
	SnapshotSequence int64
	CatalogRevision  string
	ValidUntil       time.Time
	Providers        []domain.WorkerProviderInventory
	Projects         []domain.WorkerProjectInventory
}

type TaskWakeCutoff struct {
	ReadyAt   time.Time
	AttemptID string
}

func (s *Store) WakeTaskWaits(ctx context.Context, now time.Time) ([]domain.TaskWaitWakeContext, error) {
	return s.WakeTaskWaitsBefore(ctx, now, TaskWakeCutoffs{})
}

// HasReadyTaskWaits is the cheap coordinator guard for fairness projection.
// WakeTaskWaitsBefore remains authoritative and rechecks the same records in
// its transaction.
func (s *Store) HasReadyTaskWaits(ctx context.Context) (bool, error) {
	var attemptID string
	err := s.db.QueryRowContext(ctx, `SELECT candidate.attempt_id FROM coordinator_task_waits AS candidate
		WHERE json_extract(candidate.record, '$.settledAt') IS NOT NULL
		  AND json_extract(candidate.record, '$.wokenAt') IS NULL
		  AND (json_extract(candidate.record, '$.wake') = 'each' OR NOT EXISTS (
			SELECT 1 FROM coordinator_task_waits AS live
			WHERE live.attempt_id = candidate.attempt_id
			  AND json_extract(live.record, '$.settledAt') IS NULL))
		LIMIT 1`).Scan(&attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// WakeTaskWaitsBefore applies settled wakes in durable settlement order. A
// cutoff keeps a newer wake behind an older ordinary contender sharing its
// worker or quota pool. Empty cutoffs retain legacy behavior only for an
// ungoverned assignment; governed worker-driven wakes deliberately fail closed
// until a coordinator supplies current admission evidence.
func (s *Store) WakeTaskWaitsBefore(ctx context.Context, now time.Time, cutoffs TaskWakeCutoffs) ([]domain.TaskWaitWakeContext, error) {
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
	type readyAttempt struct {
		id      string
		settled time.Time
	}
	var ordered []readyAttempt
	for id, members := range byAttempt {
		ready, ok := taskWaitWakeSet(members)
		if !ok {
			continue
		}
		settled := *ready[0].SettledAt
		for _, member := range ready[1:] {
			if member.SettledAt.Before(settled) {
				settled = *member.SettledAt
			}
		}
		ordered = append(ordered, readyAttempt{id: id, settled: settled})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if !ordered[i].settled.Equal(ordered[j].settled) {
			return ordered[i].settled.Before(ordered[j].settled)
		}
		return ordered[i].id < ordered[j].id
	})

	var wakes []domain.TaskWaitWakeContext
	for _, candidate := range ordered {
		attemptID := candidate.id
		ready, ok := taskWaitWakeSet(byAttempt[attemptID])
		if !ok {
			continue
		}
		attempt, err := loadAttemptTx(ctx, tx, attemptID)
		if err != nil {
			return nil, err
		}
		if !attempt.Progress.Terminal() && (cutoffs.ContendingWorker != "" || cutoffs.ContendingPool != "") {
			if attempt.AssignmentID == "" {
				continue
			}
			scoped, err := loadAssignmentTx(ctx, tx, attempt.AssignmentID)
			if err != nil {
				return nil, err
			}
			if scoped.WorkerID != cutoffs.ContendingWorker && scoped.Route.QuotaPoolID != cutoffs.ContendingPool {
				continue
			}
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
		assignment, err := loadAssignmentTx(ctx, tx, attempt.AssignmentID)
		if err != nil {
			return nil, err
		}
		authorization, authorized := cutoffs.AuthorizedWorkers[assignment.WorkerID]
		if assignment.Route.ProviderInstanceID != "" {
			project, err := frozenTaskWakeProjectTx(ctx, tx, attempt, assignment)
			if err != nil {
				return nil, err
			}
			if !authorized || authorization.WorkerEpoch != assignment.WorkerEpoch ||
				authorization.ValidUntil.Before(now) || !authorizedTaskWakeProject(authorization.Projects, project) {
				continue
			}
			snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, assignment.WorkerID)
			if err != nil {
				return nil, err
			}
			current := exists && snapshot.WorkerEpoch == authorization.WorkerEpoch &&
				snapshot.Sequence == authorization.SnapshotSequence &&
				snapshot.Inventory.CatalogRevision == authorization.CatalogRevision &&
				!snapshot.ValidUntil.Before(now) && authorizedTaskWakeRoute(snapshot.Inventory.Providers, assignment.Route) &&
				authorizedTaskWakeProject(snapshot.Inventory.Projects, project)
			if !current || !authorizedTaskWakeRoute(authorization.Providers, assignment.Route) {
				continue
			}
		}
		newerThan := func(cutoff TaskWakeCutoff) bool {
			return candidate.settled.After(cutoff.ReadyAt) || candidate.settled.Equal(cutoff.ReadyAt) && attemptID >= cutoff.AttemptID
		}
		if cutoff, ok := cutoffs.Worker[assignment.WorkerID]; ok && newerThan(cutoff) {
			continue
		}
		poolID := assignment.Route.QuotaPoolID
		if (cutoffs.ContendingWorker != "" || cutoffs.ContendingPool != "") &&
			assignment.WorkerID != cutoffs.ContendingWorker && poolID != cutoffs.ContendingPool {
			continue
		}
		if cutoff, ok := cutoffs.Pool[poolID]; ok && newerThan(cutoff) {
			continue
		}
		if poolID != "" {
			pools, err := loadJSON[domain.QuotaPool](ctx, tx, "coordinator_quota_pools")
			if err != nil {
				return nil, err
			}
			admissions, err := loadJSON[domain.QuotaAdmissionRecord](ctx, tx, "coordinator_quota_admissions")
			if err != nil {
				return nil, err
			}
			var pool domain.QuotaPool
			foundPool, open := false, false
			for _, current := range pools {
				if current.ID == poolID {
					pool, foundPool = current, true
					break
				}
			}
			if foundPool && !cutoffs.AdmissionValidAfter.IsZero() &&
				(pool.ChecksDisabled || strings.HasSuffix(poolID, "-free")) {
				open = true
			}
			for _, admission := range admissions {
				classAllowed := admission.Admission == domain.AdmissionOpen ||
					(attempt.AdminForceStart && (admission.Admission == domain.AdmissionConstrained || admission.Admission == domain.AdmissionRecovering))
				if admission.QuotaPoolID == poolID && !cutoffs.AdmissionValidAfter.IsZero() && !admission.ObservedAt.Before(cutoffs.AdmissionValidAfter) && classAllowed {
					open = true
				}
			}
			if !foundPool || !open {
				continue
			}
			active := 0
			assignments, err := loadJSON[domain.Assignment](ctx, tx, "coordinator_assignments")
			if err != nil {
				return nil, err
			}
			for _, other := range assignments {
				if other.Route.QuotaPoolID != poolID {
					continue
				}
				owner, err := loadAttemptTx(ctx, tx, other.AttemptID)
				if err != nil {
					return nil, err
				}
				if domain.AssignmentOwnsExecutorCapacity(owner, other) {
					active++
				}
			}
			if pool.MaxConcurrent > 0 && active >= pool.MaxConcurrent {
				continue
			}
		}
		demand, demandKnown := domain.AssignmentExecutorDemand(attempt, assignment)
		if err := requireExecutorCapacityTx(ctx, tx, assignment.WorkerID, now, assignment.WorkerEpoch, demand, demandKnown); err != nil {
			if errors.Is(err, ErrExecutorCapacity) || errors.Is(err, ErrExecutorCapacityEvidence) {
				// Settlement remains durable but undelivered. A later pass
				// retries this same wake after capacity is released.
				continue
			}
			return nil, err
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

func authorizedTaskWakeRoute(providers []domain.WorkerProviderInventory, route domain.ProviderRoute) bool {
	for _, provider := range providers {
		if !provider.Available || provider.InstanceID != route.ProviderInstanceID || provider.QuotaPoolID != route.QuotaPoolID {
			continue
		}
		for _, model := range provider.Models {
			if model == route.Model {
				return true
			}
		}
	}
	return false
}

func authorizedTaskWakeProject(projects []domain.WorkerProjectInventory, project string) bool {
	if project == "" {
		return false
	}
	for _, candidate := range projects {
		if candidate.Name == project && candidate.Available {
			return true
		}
	}
	return false
}

func frozenTaskWakeProjectTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt, assignment domain.Assignment) (string, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id=?", attempt.WorkflowRunID).Scan(&raw); err != nil {
		return "", fmt.Errorf("load wake workflow run %q: %w", attempt.WorkflowRunID, err)
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal(raw, &run); err != nil {
		return "", err
	}
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflows WHERE id=?", run.WorkflowID).Scan(&raw); err != nil {
		return "", fmt.Errorf("load wake workflow %q: %w", run.WorkflowID, err)
	}
	var workflow domain.Workflow
	if err := json.Unmarshal(raw, &workflow); err != nil {
		return "", err
	}
	if assignment.Project != "" && assignment.Project != workflow.Project {
		return "", fmt.Errorf("assignment %q frozen project %q disagrees with workflow %q project %q", assignment.ID, assignment.Project, workflow.ID, workflow.Project)
	}
	return workflow.Project, nil
}

func replaySettledOutcome(wait domain.TaskWait) domain.TaskWaitOutcome {
	if wait.Result == nil {
		return "settled"
	}
	return wait.Result.Outcome
}

func markTaskWaitsWokenTx(ctx context.Context, tx *sql.Tx, waits []domain.TaskWait, attempt domain.Attempt, delivery string, resumption bool, now time.Time) error {
	woken := now.UTC()
	for index, wait := range waits {
		wait.WokenAt = &woken
		wait.Delivery = delivery
		wait.WakeRevision = attempt.Revision
		wait.Resumption = resumption
		if wait.DeliveryID == "" {
			// Every member of this committed set shares one stable message.
			// Per-wait IDs would start a separate turn for each all member.
			wait.DeliveryID = fmt.Sprintf("task-wake:%s:%d", waits[0].ID, attempt.Revision)
		}
		waits[index] = wait
		if err := saveTaskWaitTx(ctx, tx, wait); err != nil {
			return err
		}
	}
	return nil
}

func advanceAttentionReceipt(wait *domain.TaskWait, state domain.AttentionReceiptState, now time.Time) {
	if wait.Attention == nil {
		return
	}
	var latest *domain.AttentionReceipt
	for index := range wait.AttentionReceipts {
		candidate := &wait.AttentionReceipts[index]
		switch candidate.State {
		case domain.AttentionApplied, domain.AttentionDelivered, domain.AttentionObserved:
			latest = candidate
		}
	}
	if latest == nil || latest.State == domain.AttentionObserved ||
		state == domain.AttentionDelivered && latest.State == domain.AttentionDelivered {
		return
	}
	at := now.UTC()
	if state == domain.AttentionObserved && latest.State == domain.AttentionApplied {
		delivered := *latest
		delivered.State, delivered.DeliveredAt = domain.AttentionDelivered, &at
		wait.AttentionReceipts = append(wait.AttentionReceipts, delivered)
		latest = &wait.AttentionReceipts[len(wait.AttentionReceipts)-1]
	}
	next := *latest
	next.State = state
	if state == domain.AttentionDelivered {
		next.DeliveredAt = &at
	} else if state == domain.AttentionObserved {
		if next.DeliveredAt == nil {
			next.DeliveredAt = &at
		}
		next.ObservedAt = &at
	}
	wait.AttentionReceipts = append(wait.AttentionReceipts, next)
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
		(from == "sending" || from == "recovery-required") && (to == "delivered" || to == "observed" || to == "recovery-required") ||
		from == "delivered" && to == "observed" ||
		to == "abandoned" && from != "delivered" && from != "observed")
	if !allowed {
		return false, fmt.Errorf("invalid task wake transition %s to %s", from, to)
	}
	// One committed wake set owns one message. Claim and settle every member
	// atomically, including recovery after an uncertain send.
	members, err := loadJSON[domain.TaskWait](ctx, tx, "coordinator_task_waits")
	if err != nil {
		return false, err
	}
	var group []domain.TaskWait
	for _, member := range members {
		sameDelivery := wait.DeliveryID != "" && member.DeliveryID == wait.DeliveryID &&
			member.AttemptID == wait.AttemptID && member.ThreadID == wait.ThreadID &&
			member.WakeRevision == wait.WakeRevision
		if member.ID != wait.ID && !sameDelivery {
			continue
		}
		if member.Delivery != from {
			return false, nil
		}
		group = append(group, member)
		member.Delivery = to
		if to == "delivered" {
			delivered := now.UTC()
			member.DeliveredAt = &delivered
			advanceAttentionReceipt(&member, domain.AttentionDelivered, now)
		}
		if to == "observed" {
			observed := now.UTC()
			member.DeliveryObservedAt = &observed
			advanceAttentionReceipt(&member, domain.AttentionObserved, now)
		}
		if err = saveTaskWaitTx(ctx, tx, member); err != nil {
			return false, err
		}
	}
	if len(group) > 1 {
		if to == "sending" {
			if err := recordTaskWaitEventTx(ctx, tx, taskWakeGroupClaim(group, now)); err != nil {
				return false, err
			}
		} else if to == "delivered" || to == "observed" {
			claimed, err := taskWakeGroupClaimedTx(ctx, tx, group)
			if err != nil || !claimed {
				return false, err
			}
		}
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
	if err := reconcileTaskWakeGroupsTx(ctx, tx, waits, now); err != nil {
		return nil, err
	}
	byAttempt := make(map[string][]domain.TaskWait)
	var order []string
	for _, wait := range waits {
		if !wait.Woken() || wait.Delivery == "observed" || wait.Delivery == "abandoned" ||
			wait.Delivery == "delivered" && wait.Attention == nil {
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
			if wait.Delivery == "sending" || wait.Delivery == "recovery-required" || wait.Delivery == "manual-recovery-required" || wait.Delivery == "delivered" {
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
		assignment, err := loadAssignmentTx(ctx, tx, attempt.AssignmentID)
		if err != nil {
			return nil, err
		}
		pending = append(pending, domain.TaskWaitWakeContext{
			WorkerID:  assignment.WorkerID,
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
