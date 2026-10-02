package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// An ask is a task-bound wait of kind ask. It lives in coordinator_task_waits
// like every other task-bound wait, so it parks, wakes, expires and is listed
// by the same code; what is particular to it is how it may be settled. It is
// never settled by a check result: only by an answer (AnswerAsk), by its
// deadline (ExpireTaskWaits, through applyAskDeadline) or by a cancellation.

func sameAskRegistration(stored, requested *domain.AskRequest) bool {
	if stored == nil || requested == nil {
		return stored == nil && requested == nil
	}
	return stored.SameQuestion(*requested)
}

// applyAskDeadline settles an expiring ask. With a declared default it records
// the default as the answer and reports true; with --on-deadline fail it leaves
// the timed-out result, reworded to say there was no answer, and reports false
// so the expiry is recorded as the contradiction it is.
func applyAskDeadline(wait *domain.TaskWait, settled time.Time) bool {
	if wait.Ask.Requires == domain.AskRequiresApprover {
		// Registration refuses a default on an approver ask; a record that
		// carries one anyway is never default-settled, because a default is an
		// answer the approver did not give.
		wait.Result.Reason = fmt.Sprintf("no approver answer before the deadline of %s; an approver ask times out unanswered", wait.MaxDuration)
		wait.Result.Fields = map[string]string{"answer": "none"}
		return false
	}
	if wait.Ask.OnDeadline != domain.AskDeadlineDefault || len(wait.Ask.Default) == 0 {
		wait.Result.Reason = fmt.Sprintf("no answer before the deadline of %s; the ask was registered with --on-deadline fail", wait.MaxDuration)
		wait.Result.Fields = map[string]string{"answer": "none"}
		return false
	}
	answer := domain.AskAnswer{
		Schema: domain.AskAnswerSchema, AskID: wait.ID, WorkflowRunID: wait.WorkflowRunID, TaskID: wait.TaskID,
		Question: wait.Ask.Question, Options: append([]string(nil), wait.Ask.Default...),
		Source: domain.AskSourceDeadlineDefault, AnsweredAt: settled,
	}
	wait.AskAnswer = &answer
	wait.Result = &domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, ExitCode: 0,
		Reason:     fmt.Sprintf("no answer before the deadline of %s; the declared default applies", wait.MaxDuration),
		Fields:     map[string]string{"answer": answer.Summary(), "source": string(answer.Source)},
		RanFor:     settled.Sub(wait.RegisteredAt),
		ObservedAt: settled,
	}
	return true
}

// askAnswerWorkspaceTx finds the workspace whose ask-answer.json an ask's
// wake prepares: the one the attempt's worker last reported for its
// assignment. It is looked up for every wake that carries an ask, answered or
// not, because an unanswered one must remove an earlier ask's file. An unknown
// workspace is not an error: the wake says the file could not be prepared.
func askAnswerWorkspaceTx(ctx context.Context, tx *sql.Tx, assignment domain.Assignment, waits []domain.TaskWait) (string, error) {
	asked := false
	for _, wait := range waits {
		if wait.Ask != nil {
			asked = true
		}
	}
	if !asked || assignment.WorkerID == "" {
		return "", nil
	}
	snapshot, found, err := loadWorkerSnapshotTx(ctx, tx, assignment.WorkerID)
	if err != nil || !found {
		return "", err
	}
	for _, observation := range snapshot.Assignments {
		if observation.AssignmentID == assignment.ID {
			return observation.WorkspacePath, nil
		}
	}
	return "", nil
}

// RecordAskRelay records the relay thread of an ask as the steward of the
// asking attempt's worker moves it through opening, open, archived or failed.
//
// The thread is fixed by the first record: the steward derives its ID before
// creating it, records opening, and only then creates it, so a crash between
// the two leaves the ID that may exist rather than a second thread. A record
// naming another thread is refused, as is any relay for an approver ask, a
// relay from a worker that does not hold the attempt's assignment, and opening
// a relay for an ask that has already settled. Archiving is allowed after
// settlement, which is when it normally happens.
func (s *Store) RecordAskRelay(ctx context.Context, waitID string, relay domain.AskRelay, now time.Time) (domain.TaskWait, error) {
	var wait domain.TaskWait
	if waitID == "" || relay.ThreadID == "" || now.IsZero() {
		return wait, errors.New("an ask relay record needs the ask, the thread and a timestamp")
	}
	switch relay.State {
	case domain.AskRelayOpening, domain.AskRelayOpen, domain.AskRelayArchived, domain.AskRelayFailed:
	default:
		return wait, fmt.Errorf("ask relay state %q is not opening, open, archived or failed", relay.State)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wait, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_task_waits WHERE id=?", waitID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return wait, fmt.Errorf("ask %q is unknown", waitID)
		}
		return wait, err
	}
	if err = json.Unmarshal(raw, &wait); err != nil {
		return wait, err
	}
	if wait.Kind != domain.WaitKindAsk || wait.Ask == nil {
		return wait, fmt.Errorf("wait %q is not an ask", waitID)
	}
	if wait.Ask.Requires == domain.AskRequiresApprover {
		return wait, errors.New("an ask that requires the approver has no relay thread: it is answered only through the signed approver path")
	}
	attempt, err := loadAttemptTx(ctx, tx, wait.AttemptID)
	if err != nil {
		return wait, err
	}
	assignment, err := loadAssignmentTx(ctx, tx, attempt.AssignmentID)
	if err != nil {
		return wait, err
	}
	if relay.WorkerID == "" {
		// Only an in-process caller reaches here without a worker: a
		// single-host steward, which is not a named worker. A remote steward
		// always names its worker; the service refuses one that does not.
		relay.WorkerID = assignment.WorkerID
	}
	if assignment.WorkerID != relay.WorkerID {
		return wait, fmt.Errorf("worker %q does not run the asking attempt; %q does", relay.WorkerID, assignment.WorkerID)
	}
	prior := wait.Ask.Relay
	if prior != nil && prior.ThreadID != relay.ThreadID {
		return wait, fmt.Errorf("ask %s already has relay thread %s", wait.ID, prior.ThreadID)
	}
	if prior != nil && prior.State == relay.State {
		return wait, tx.Commit()
	}
	if prior != nil && (prior.State == domain.AskRelayArchived || prior.State == domain.AskRelayFailed) {
		return wait, fmt.Errorf("relay thread %s is already %s", prior.ThreadID, prior.State)
	}
	if prior != nil && prior.State == domain.AskRelayOpen && relay.State == domain.AskRelayOpening {
		// A relay never moves back: an open thread whose start is reported
		// again, late, stays open.
		return wait, tx.Commit()
	}
	if wait.Settled() && (relay.State == domain.AskRelayOpening || relay.State == domain.AskRelayOpen) {
		return wait, fmt.Errorf("%w; no relay thread is opened for it", domain.ErrAskSettled)
	}
	record := relay
	record.UpdatedAt = now.UTC()
	if record.State == domain.AskRelayArchived {
		archived := now.UTC()
		record.ArchivedAt = &archived
	}
	wait.Ask.Relay = &record
	if err := saveTaskWaitTx(ctx, tx, wait); err != nil {
		return wait, err
	}
	return wait, tx.Commit()
}

// AskRelayWork lists the asks whose relay thread the steward of workerID has
// something to do for: an open ask with no relay yet or one still opening or
// open, and a settled ask whose relay thread has not been archived. Approver
// asks have no relay and are never listed. An empty workerID lists every
// worker's, for a single-host deployment whose steward is not a worker.
func (s *Store) AskRelayWork(ctx context.Context, workerID string) ([]domain.TaskWait, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT record FROM coordinator_task_waits
		WHERE json_extract(record, '$.kind') = 'ask'
		  AND COALESCE(json_extract(record, '$.ask.requires'), '') <> 'approver'
		  AND (json_extract(record, '$.settledAt') IS NULL
		       OR json_extract(record, '$.ask.relay.state') IN ('opening', 'open'))
		  AND COALESCE(json_extract(record, '$.ask.relay.state'), '') NOT IN ('archived', 'failed')
		ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var candidates []domain.TaskWait
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var wait domain.TaskWait
		if err := json.Unmarshal(raw, &wait); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, wait)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var work []domain.TaskWait
	for _, wait := range candidates {
		if workerID != "" {
			attempt, err := loadAttemptTx(ctx, tx, wait.AttemptID)
			if err != nil {
				return nil, err
			}
			assignment, err := loadAssignmentTx(ctx, tx, attempt.AssignmentID)
			if err != nil || assignment.WorkerID != workerID {
				continue
			}
		}
		work = append(work, wait)
	}
	return work, tx.Commit()
}

// AnswerAsk records the answer to an ask and settles it, in one transaction.
//
// The answer names the ask by AskID and carries what was chosen, the free text,
// where it came from and, for a T3 answer, the relay thread it was given in.
// Everything else in the stored ask-answer/v1 document is filled in here from
// the coordinator's own record, so a caller cannot restate the question.
//
// The same answer replayed returns the settled ask unchanged; a different one
// is refused, as is any answer to an ask that already settled by its deadline
// or by cancellation. An approver ask accepts only an answer from the CLI whose
// frame was verified as a configured approver's; approver reports exactly that.
func (s *Store) AnswerAsk(ctx context.Context, answer domain.AskAnswer, principal string, approver bool, now time.Time) (domain.TaskWait, error) {
	var wait domain.TaskWait
	if answer.AskID == "" || principal == "" || now.IsZero() {
		return wait, errors.New("an ask answer needs the ask ID, an authenticated principal and a timestamp")
	}
	if answer.Source != domain.AskSourceT3 && answer.Source != domain.AskSourceCLI {
		return wait, fmt.Errorf("an ask answer comes from t3 or cli, not %q", answer.Source)
	}
	if answer.Source == domain.AskSourceCLI && answer.ThreadID != "" {
		return wait, errors.New("a CLI answer names no thread")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wait, err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_task_waits WHERE id=?", answer.AskID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return wait, fmt.Errorf("ask %q is unknown", answer.AskID)
		}
		return wait, err
	}
	if err = json.Unmarshal(raw, &wait); err != nil {
		return wait, err
	}
	if wait.Kind != domain.WaitKindAsk || wait.Ask == nil {
		return wait, fmt.Errorf("wait %q is a %s wait, not an ask", wait.ID, wait.Kind.OrShell())
	}
	// Authority is checked before anything else, a replay included: a caller
	// that could not have given the answer learns nothing by repeating it.
	if wait.Ask.Requires == domain.AskRequiresApprover {
		if answer.Source == domain.AskSourceT3 {
			return wait, fmt.Errorf("%w: an answer given in T3 is refused", domain.ErrAskApproverRequired)
		}
		if !approver {
			return wait, fmt.Errorf("%w: answer it with a client configured as an approver (t3-steward --config <approver config> ask answer %s ...)",
				domain.ErrAskApproverRequired, wait.ID)
		}
	}
	if answer.Source == domain.AskSourceT3 {
		if wait.Ask.Relay == nil || wait.Ask.Relay.ThreadID == "" || wait.Ask.Relay.ThreadID != answer.ThreadID {
			return wait, fmt.Errorf("%w: a T3 answer must come from the ask's own relay thread; %q is not it", domain.ErrAskAnswerRefused, answer.ThreadID)
		}
	}
	answer.Options, answer.FreeText = domain.NormalizeAnswer(answer.Options, answer.FreeText)
	if prior := wait.AskAnswer; prior != nil {
		priorOptions, priorText := domain.NormalizeAnswer(prior.Options, prior.FreeText)
		if prior.Source == answer.Source && prior.ThreadID == answer.ThreadID && priorText == answer.FreeText &&
			reflect.DeepEqual(priorOptions, answer.Options) {
			return wait, tx.Commit()
		}
		return wait, fmt.Errorf("%w (%s, from %s at %s)", domain.ErrAskAlreadyAnswered,
			prior.Summary(), prior.Source, prior.AnsweredAt.UTC().Format(time.RFC3339))
	}
	if wait.Live() && !now.Before(wait.Deadline) {
		// The deadline passed before the expiry pass reached this ask. The
		// expiry policy is applied here, in the same transaction, so a late
		// answer can never win over the default or the failure the task was
		// promised; the refusal below then reports what it became.
		if wait, err = expireTaskWaitTx(ctx, tx, wait, now); err != nil {
			return wait, err
		}
		if err = tx.Commit(); err != nil {
			return wait, err
		}
		return wait, fmt.Errorf("%w as %s: the answer arrived after the deadline", domain.ErrAskSettled, wait.Result.Outcome)
	}
	if wait.Settled() {
		outcome := domain.TaskWaitOutcome("settled")
		if wait.Result != nil {
			outcome = wait.Result.Outcome
		}
		return wait, fmt.Errorf("%w as %s", domain.ErrAskSettled, outcome)
	}
	if err := wait.Ask.CheckAnswer(answer.Options, answer.FreeText); err != nil {
		return wait, fmt.Errorf("%w: %w", domain.ErrAskAnswerRefused, err)
	}
	attempt, err := loadAttemptTx(ctx, tx, wait.AttemptID)
	if err != nil {
		return wait, err
	}
	if attempt.Progress.Terminal() {
		return wait, fmt.Errorf("%w: the task that asked is no longer waiting: its attempt is %s", domain.ErrAskAnswerRefused, attempt.Progress)
	}
	answered := now.UTC()
	record := domain.AskAnswer{
		Schema: domain.AskAnswerSchema, AskID: wait.ID, WorkflowRunID: wait.WorkflowRunID, TaskID: wait.TaskID,
		Question: wait.Ask.Question, Options: answer.Options, FreeText: answer.FreeText,
		Source: answer.Source, AnsweredBy: principal, ThreadID: answer.ThreadID, AnsweredAt: answered,
	}
	wait.AskAnswer = &record
	reason := "answered from the CLI by " + principal
	if answer.Source == domain.AskSourceT3 {
		reason = "answered in T3 on relay thread " + answer.ThreadID
	}
	wait, err = settleTaskWaitTx(ctx, tx, wait, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, ExitCode: 0, Reason: reason,
		Fields: map[string]string{"answer": record.Summary(), "source": string(record.Source)},
	}, answered)
	if err != nil {
		return wait, err
	}
	return wait, tx.Commit()
}
