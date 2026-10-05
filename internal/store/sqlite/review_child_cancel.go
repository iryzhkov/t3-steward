package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// ReviewChildCancellationResult reports durable custody, never a worker stop
// acknowledgement or review acceptance. Quiescence uses the existing predicate.
type ReviewChildCancellationResult struct {
	Status            string // no-action, stop-requested, quiescent
	Reason            string
	CancelledAttempts []string
	WorkerStopPending bool
}

// ReconcileReviewChildCancellation is a trusted internal boundary with no
// runtime hook. Only original stored authority and checkpoint are accepted.
// All observations and effects serialize under the SQLite writer.
func (s *Store) ReconcileReviewChildCancellation(ctx context.Context, expected review.FrozenAuthority, allocated review.CheckpointAuthority) (ReviewChildCancellationResult, error) {
	var zero ReviewChildCancellationResult
	expected, err := expected.Canonical()
	if err != nil {
		return zero, err
	}
	tx, err := s.reviewAuthorityWriteTx(ctx, expected.Parent)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback()
	f, cp, round, err := reviewChildAuthorityTx(ctx, tx, expected, allocated)
	if err != nil {
		return zero, err
	}
	var authorityRun, authorityTask string
	if err = tx.QueryRowContext(ctx, "SELECT run_id,task_id FROM coordinator_review_authorities WHERE id=?", f.Key()).Scan(&authorityRun, &authorityTask); err != nil {
		return zero, err
	}
	if authorityRun != f.Parent.RunID || authorityTask != f.Parent.TaskID {
		return zero, ErrReviewAuthorityIdentity
	}
	var indexedRoundRevision int64
	if err = tx.QueryRowContext(ctx, "SELECT revision FROM coordinator_review_rounds WHERE id=?", cp.RoundID).Scan(&indexedRoundRevision); err != nil {
		return zero, err
	}
	if round.Revision < 1 || indexedRoundRevision != round.Revision {
		return zero, ErrReviewRoundConflict
	}
	receipt, err := loadReviewJSONTx[ReviewMaterialization](ctx, tx, "SELECT record FROM coordinator_review_materializations WHERE checkpoint_id=? AND workflow_id=? AND run_id=?", cp.Key(), review.ChildWorkflowID(cp), cp.RoundID)
	if err != nil {
		return zero, err
	}
	if !reflect.DeepEqual(receipt.Authority, f) || receipt.Checkpoint != cp ||
		receipt.Graph.Run.ID != cp.RoundID || receipt.Graph.Workflow.ID != review.ChildWorkflowID(cp) {
		return zero, ErrReviewMaterialization
	}
	if err = validateReviewChildTx(ctx, tx, receipt); err != nil {
		return zero, err
	}
	if len(receipt.Graph.Tasks) == 0 || receipt.Graph.Tasks[0].Deadline == nil {
		return zero, ErrReviewMaterialization
	}
	deadline := *receipt.Graph.Tasks[0].Deadline
	if deadline.IsZero() || !round.Deadline.Equal(deadline) {
		return zero, ErrReviewMaterialization
	}
	for _, task := range receipt.Graph.Tasks {
		if task.Deadline == nil || !task.Deadline.Equal(deadline) {
			return zero, ErrReviewMaterialization
		}
	}
	parent, latest, err := reviewParentAttemptSnapshotTx(ctx, tx, f.Parent)
	if err != nil {
		return zero, err
	}
	owners, err := validateReviewChildAttemptsTx(ctx, tx, receipt.Graph)
	if err != nil {
		return zero, err
	}
	attempts := map[string]domain.Attempt{}
	parentIDs := map[string]domain.Attempt{}
	err = scanReviewChildRows[domain.Attempt](ctx, tx, "SELECT id,workflow_run_id,task_id,record FROM coordinator_attempts", 3, append(append([]string{}, reviewChildAttemptKeys...), "assignmentId", "threadId", "progress", "control", "completedAt"), func(i []string, a domain.Attempt) error {
		if _, ok := owners[a.ID]; ok {
			if !reviewCancellationExecutionCoherent(a) {
				return ErrReviewMaterialization
			}
			attempts[a.ID] = a
		}
		if a.WorkflowRunID == f.Parent.RunID && a.TaskID == f.Parent.TaskID {
			if !reviewCancellationExecutionCoherent(a) || a.SupervisionActivationID != "" || a.SupervisionActivationEpoch != 0 {
				return ErrReviewAuthorityIdentity
			}
			parentIDs[a.ID] = a
		}
		return nil
	})
	if err != nil {
		return zero, err
	}
	assignments, err := reviewCancellationAssignmentsTx(ctx, tx, attempts, parentIDs, f.Parent.AssignmentID)
	if err != nil {
		return zero, err
	}
	if err = reviewCancellationParentBindings(f.Parent, parentIDs, assignments); err != nil {
		return zero, err
	}
	for _, assignment := range assignments {
		owner, ok := attempts[assignment.AttemptID]
		if !ok {
			continue
		}
		if assignment.Project != "" && assignment.Project != f.Parent.Repository {
			return zero, ErrReviewMaterialization
		}
		for _, task := range receipt.Graph.Tasks {
			if task.ID != owner.TaskID {
				continue
			}
			if len(task.Routes) != 1 || assignment.Route.ProviderInstanceID != task.Routes[0].ProviderInstanceID || assignment.Route.Model != task.Routes[0].Model {
				return zero, ErrReviewMaterialization
			}
		}
	}
	reason, err := reviewCancellationParentTx(ctx, tx, f.Parent, parent, latest, assignments)
	if err != nil {
		return zero, err
	}
	now := s.now().UTC()
	if !deadline.After(now) {
		reason = "review deadline reached"
	}
	if reason == "" {
		return ReviewChildCancellationResult{Status: "no-action"}, tx.Commit()
	}
	result := ReviewChildCancellationResult{Status: "stop-requested", Reason: reason}
	ids := make([]string, 0, len(attempts))
	for id := range attempts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	cancelledTasks := map[string]bool{}
	for _, id := range ids {
		a := attempts[id]
		if a.Progress.Terminal() {
			continue
		}
		// Tokens are revoked first, retaining the legacy lookup index exactly as
		// RevokeTaskAuthority does. Authorization always reads the runtime record.
		for key, assignment := range assignments {
			if assignment.AttemptID != id {
				continue
			}
			next := assignment
			if next.DispatchToken != "" {
				next.DispatchToken = ""
				next.DispatchError = "task authority revoked: " + reason
				next.UpdatedAt = now
				if err = updateReviewCancellationAssignmentTx(ctx, tx, assignment, next); err != nil {
					return zero, err
				}
				assignment = next
			}
			if assignment.State == domain.AssignmentOffered {
				// Offered is never claimed. A started attempt with an offer is corrupt,
				// rather than evidence that its worker can safely be released.
				if !reviewCancellationUnstarted(a) ||
					assignment.DispatchState != "" || assignment.DispatchRevision != 0 || assignment.DispatchConfirmedAt != nil {
					return zero, ErrReviewMaterialization
				}
				next = assignment
				next.State = domain.AssignmentReleased
				next.LeaseExpiresAt = time.Time{}
				next.UpdatedAt = now
				if err = updateReviewCancellationAssignmentTx(ctx, tx, assignment, next); err != nil {
					return zero, err
				}
				assignment = next
			}
			assignments[key] = assignment
		}
		previous := a.Revision
		a.Revision++
		a.Progress = domain.ProgressCancelled
		a.Control = domain.ControlStopped
		a.Failure = ""
		a.CompletedAt = &now
		a.UpdatedAt = now
		if err = saveAttemptFencedTx(ctx, tx, a, previous); err != nil {
			return zero, err
		}
		auditID := "review-child-cancelled:" + cp.Key() + ":" + id
		if _, err = insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
			ID: auditID, Kind: "attempt-cancelled", WorkflowRunID: a.WorkflowRunID,
			TaskID: a.TaskID, AttemptID: id, TargetType: domain.AdminTargetAttempt, TargetID: id,
			Actor: "coordinator", Reason: reason, CreatedAt: now,
			Detail: nativeAuditDetail{ExpectedRevision: previous, Revision: a.Revision, IdempotencyIdentity: auditID, Outcome: string(domain.ProgressCancelled)},
		}); err != nil {
			return zero, err
		}
		attempts[id] = a
		cancelledTasks[a.TaskID] = true
		result.CancelledAttempts = append(result.CancelledAttempts, id)
	}
	roundRevision := round.Revision
	for _, member := range round.Reviewers {
		if member.State != "pending" || !cancelledTasks[member.TaskID] {
			continue
		}
		// A terminal latest execution remains collector-owned even if a stranded
		// older execution of the same task needed stopping.
		var latestAttempt domain.Attempt
		for _, a := range attempts {
			if a.TaskID == member.TaskID && a.Number > latestAttempt.Number {
				latestAttempt = a
			}
		}
		newlyCancelled := false
		for _, id := range result.CancelledAttempts {
			if id == latestAttempt.ID {
				newlyCancelled = true
			}
		}
		if !newlyCancelled {
			continue
		}
		state := "failed"
		if reason == "review deadline reached" {
			state = "timed-out"
		}
		if err = round.ApplyResult(member.ID, review.Result{State: state, Failure: reason}, now); err != nil {
			return zero, err
		}
	}
	if round.Revision != roundRevision {
		raw, err := json.Marshal(round)
		if err != nil {
			return zero, err
		}
		updated, err := tx.ExecContext(ctx, "UPDATE coordinator_review_rounds SET revision=?,record=? WHERE id=? AND revision=?", round.Revision, raw, round.ID, roundRevision)
		if err != nil {
			return zero, err
		}
		n, err := updated.RowsAffected()
		if err != nil {
			return zero, err
		}
		if n != 1 {
			return zero, ErrReviewRoundConflict
		}
	}
	childAttempts := make([]domain.Attempt, 0, len(attempts))
	childAssignments := []domain.Assignment{}
	for _, a := range attempts {
		childAttempts = append(childAttempts, a)
	}
	for _, a := range assignments {
		if _, ok := attempts[a.AttemptID]; ok {
			childAssignments = append(childAssignments, a)
		}
	}
	if domain.RunExecutionsQuiescent(cp.RoundID, childAttempts, childAssignments) {
		result.Status = "quiescent"
	} else {
		result.WorkerStopPending = true
	}
	// Existing projection owns sink/run settlement; no cancellation-specific
	// sink grant and no parent wait mutation are manufactured here.
	if err = tx.Commit(); err != nil {
		return zero, err
	}
	return result, nil
}

// Indexed UNION runtime ownership prevents a foreign index hiding an owned
// assignment. Replaced epochs are permitted; inconsistent records are refused.
func reviewCancellationAssignmentsTx(ctx context.Context, tx *sql.Tx, children map[string]domain.Attempt, parents map[string]domain.Attempt, original string) (map[string]domain.Assignment, error) {
	found := map[string]domain.Assignment{}
	seen := map[string]bool{}
	err := scanReviewChildRows[reviewCancellationAssignmentRecord](ctx, tx,
		"SELECT id,attempt_id,worker_id,worker_epoch,assignment_epoch,assignment_state,dispatch_revision,dispatch_state,lease_expires_at,dispatch_token,record FROM coordinator_assignments", 10,
		[]string{"id", "attemptId", "workerId", "workerEpoch", "epoch", "state", "threadId", "route", "dispatchToken", "dispatchState", "dispatchRevision", "dispatchConfirmedAt", "leaseExpiresAt", "project", "executionRole", "activationId", "gateId"},
		func(i []string, record reviewCancellationAssignmentRecord) error {
			a := record.Assignment
			_, ic := children[i[1]]
			_, rc := children[a.AttemptID]
			referenced := i[0] == original || a.ID == original
			referencedAttempts := make([]domain.Attempt, 0, len(children)+len(parents))
			for _, attempt := range children {
				referencedAttempts = append(referencedAttempts, attempt)
			}
			for _, attempt := range parents {
				referencedAttempts = append(referencedAttempts, attempt)
			}
			for _, attempt := range referencedAttempts {
				if attempt.AssignmentID == i[0] || attempt.AssignmentID == a.ID {
					referenced = true
				}
			}
			_, ip := parents[i[1]]
			_, rp := parents[a.AttemptID]
			if !ic && !rc && !ip && !rp && !referenced {
				return nil
			}
			if a.ID == "" || i[0] != a.ID || i[1] != a.AttemptID || i[2] != a.WorkerID || i[3] != a.WorkerEpoch ||
				i[4] != strconv.FormatInt(a.Epoch, 10) || i[5] != string(a.State) || i[6] != strconv.FormatInt(a.DispatchRevision, 10) || i[7] != string(a.DispatchState) || a.Epoch < 1 || seen[a.AttemptID] ||
				(!rc && !rp) {
				return ErrReviewMaterialization
			}
			switch a.State {
			case domain.AssignmentOffered, domain.AssignmentClaimed, domain.AssignmentUnknown, domain.AssignmentReleased, domain.AssignmentCompleted:
			default:
				return ErrReviewMaterialization
			}
			lease, err := time.Parse(time.RFC3339Nano, i[8])
			if i[8] == "" {
				lease = time.Time{}
				err = nil
			}
			if err != nil || !lease.Equal(a.LeaseExpiresAt) {
				return ErrReviewMaterialization
			}
			if a.DispatchToken != "" && i[9] != a.DispatchToken {
				return ErrReviewMaterialization
			}
			if a.WorkerID == "" {
				return ErrReviewMaterialization
			}
			if rc {
				attempt := children[a.AttemptID]
				if attempt.AssignmentID != a.ID && !(attempt.AssignmentID == "" && (a.State == domain.AssignmentReleased || a.State == domain.AssignmentCompleted)) {
					return ErrReviewMaterialization
				}
				if a.ThreadID != "" && attempt.ThreadID != "" && a.ThreadID != attempt.ThreadID {
					return ErrReviewMaterialization
				}
				if a.Route.ProviderInstanceID == "" || a.Route.Model == "" || (a.ExecutionRole != "" && a.ExecutionRole != domain.ExecutionRoleExecutor) || a.ActivationID != "" || a.GateID != "" {
					return ErrReviewMaterialization
				}
				if a.State == domain.AssignmentOffered && (!reviewCancellationUnstarted(attempt) || a.DispatchState != "" || a.DispatchRevision != 0 || a.DispatchConfirmedAt != nil) {
					return ErrReviewMaterialization
				}
			}
			seen[a.AttemptID] = true
			found[a.ID] = a
			return nil
		})
	if err != nil {
		return nil, err
	}
	bound := make(map[string]domain.Attempt, len(children)+len(parents))
	for id, a := range children {
		bound[id] = a
	}
	for id, a := range parents {
		bound[id] = a
	}
	for _, a := range bound {
		if a.AssignmentID != "" {
			if assignment, ok := found[a.AssignmentID]; !ok || assignment.AttemptID != a.ID {
				return nil, ErrReviewMaterialization
			}
		}
	}
	if _, ok := found[original]; !ok {
		return nil, ErrReviewAuthorityIdentity
	}
	return found, nil
}

func updateReviewCancellationAssignmentTx(ctx context.Context, tx *sql.Tx, old, next domain.Assignment) error {
	raw, err := json.Marshal(next)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx,
		"UPDATE coordinator_assignments SET assignment_state=?,lease_expires_at=?,record=? WHERE id=? AND attempt_id=? AND assignment_epoch=? AND assignment_state=?",
		next.State, reviewCancellationTime(next.LeaseExpiresAt), raw, old.ID, old.AttemptID, old.Epoch, old.State)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrReviewMaterialization
	}
	return nil
}

func reviewCancellationParentTx(ctx context.Context, tx *sql.Tx, p review.ParentBinding, a domain.Attempt, latest int, assignments map[string]domain.Assignment) (string, error) {
	if a.Revision < p.IssuedRevision || a.SupervisionActivationID != "" || a.SupervisionActivationEpoch != 0 {
		return "", ErrReviewAuthorityIdentity
	}
	// Scan run/task/workflow through indexed and runtime identities as well.
	var run domain.WorkflowRun
	var task domain.Task
	var workflow domain.Workflow
	nr, nt, nw := 0, 0, 0
	err := scanReviewChildRows[domain.WorkflowRun](ctx, tx, "SELECT id,workflow_id,revision,record FROM coordinator_workflow_runs", 3, []string{"id", "workflowId", "revision", "progress"}, func(i []string, r domain.WorkflowRun) error {
		if i[0] != p.RunID && r.ID != p.RunID {
			return nil
		}
		if i[0] != r.ID || i[1] != r.WorkflowID || i[2] != strconv.FormatInt(r.Revision, 10) || r.Revision < 1 {
			return ErrReviewAuthorityIdentity
		}
		if !reviewCancellationExecutionCoherent(domain.Attempt{Progress: r.Progress}) {
			return ErrReviewAuthorityIdentity
		}
		run = r
		nr++
		return nil
	})
	if err != nil {
		return "", err
	}
	if nr != 1 {
		return "", ErrReviewAuthorityIdentity
	}
	err = scanReviewChildRows[domain.Task](ctx, tx, "SELECT id,workflow_id,record FROM coordinator_tasks", 2, []string{"id", "workflowId", "runId"}, func(i []string, t domain.Task) error {
		if i[0] != p.TaskID && t.ID != p.TaskID {
			return nil
		}
		if i[0] != t.ID || i[1] != t.WorkflowID || t.WorkflowID != run.WorkflowID || t.RunID != "" && t.RunID != p.RunID {
			return ErrReviewAuthorityIdentity
		}
		task = t
		nt++
		return nil
	})
	if err != nil {
		return "", err
	}
	if nt != 1 || task.ID != p.TaskID {
		return "", ErrReviewAuthorityIdentity
	}
	err = scanReviewChildRows[domain.Workflow](ctx, tx, "SELECT id,record FROM coordinator_workflows", 1, []string{"id", "project"}, func(i []string, w domain.Workflow) error {
		if i[0] != run.WorkflowID && w.ID != run.WorkflowID {
			return nil
		}
		if i[0] != w.ID || w.Project != p.Repository {
			return ErrReviewAuthorityIdentity
		}
		workflow = w
		nw++
		return nil
	})
	if err != nil {
		return "", err
	}
	if nw != 1 || workflow.ID != run.WorkflowID {
		return "", ErrReviewAuthorityIdentity
	}
	assignment := assignments[p.AssignmentID]
	if assignment.State == domain.AssignmentOffered || assignment.AttemptID != p.AttemptID || assignment.Epoch < p.AssignmentEpoch || (assignment.Project != "" && assignment.Project != p.Repository) {
		return "", ErrReviewAuthorityIdentity
	}
	if assignment.Epoch == p.AssignmentEpoch &&
		(assignment.ThreadID != p.ThreadID || assignment.Route.ProviderInstanceID+"/"+assignment.Route.Model != p.ExecutorRoute) {
		return "", ErrReviewAuthorityIdentity
	}
	if run.Progress.Terminal() {
		return "parent run ended", nil
	}
	if a.Progress.Terminal() {
		return "parent attempt ended", nil
	}
	if a.Number < latest {
		return "parent attempt superseded", nil
	}
	if assignment.Epoch > p.AssignmentEpoch || assignment.State == domain.AssignmentReleased ||
		assignment.State == domain.AssignmentCompleted || assignment.State == domain.AssignmentUnknown {
		return "parent assignment ownership lost", nil
	}
	if assignment.State != domain.AssignmentClaimed || !a.TurnLive() ||
		a.ThreadID != p.ThreadID || a.AssignmentID != p.AssignmentID {
		return "", ErrReviewAuthorityIdentity
	}
	return "", nil
}

// Validate parent history and custody before any disposition shortcut. This observes
// ended/replaced authority; it does not require or grant a live original turn.
func reviewCancellationParentBindings(p review.ParentBinding, parents map[string]domain.Attempt, assignments map[string]domain.Assignment) error {
	for _, a := range parents {
		var owned domain.Assignment
		found := false
		for _, assignment := range assignments {
			if assignment.AttemptID == a.ID {
				owned, found = assignment, true
				break
			}
		}
		if !found {
			if a.ID == p.AttemptID || a.AssignmentID != "" || a.ThreadID != "" {
				return ErrReviewAuthorityIdentity
			}
			continue // A coherent unassigned retry has no custody yet.
		}
		settled := owned.State == domain.AssignmentReleased || owned.State == domain.AssignmentCompleted
		if a.AssignmentID != owned.ID && !(a.AssignmentID == "" && settled) {
			return ErrReviewAuthorityIdentity
		}
		// Unknown-recovery stopped clears both refs only on unstarted released work.
		clearedThread := owned.State == domain.AssignmentReleased && a.AssignmentID == "" && a.ThreadID == "" && reviewCancellationUnstarted(a)
		if a.ID == p.AttemptID {
			if owned.ID != p.AssignmentID || owned.State == domain.AssignmentOffered {
				return ErrReviewAuthorityIdentity
			}
			replacementThread := owned.Epoch > p.AssignmentEpoch && a.ThreadID != "" && a.ThreadID == owned.ThreadID
			if a.ThreadID != p.ThreadID && !clearedThread && !replacementThread {
				return ErrReviewAuthorityIdentity
			}
		} else {
			// Offers reserve a thread and token before dispatch reaches the attempt.
			// The reserved token alone is not evidence that execution started.
			reservedThread := owned.State == domain.AssignmentOffered && a.AssignmentID == owned.ID &&
				a.ThreadID == "" && owned.ThreadID != "" && reviewCancellationUnstarted(a) &&
				owned.DispatchState == "" && owned.DispatchRevision == 0 && owned.DispatchConfirmedAt == nil
			if owned.ThreadID != a.ThreadID && !clearedThread && !reservedThread {
				return ErrReviewAuthorityIdentity
			}
		}
		if owned.Route.ProviderInstanceID == "" || owned.Route.Model == "" ||
			(owned.Project != "" && owned.Project != p.Repository) ||
			(owned.ExecutionRole != "" && owned.ExecutionRole != domain.ExecutionRoleExecutor) ||
			owned.ActivationID != "" || owned.GateID != "" {
			return ErrReviewAuthorityIdentity
		}
		if owned.State == domain.AssignmentOffered && (!reviewCancellationUnstarted(a) || owned.DispatchState != "" || owned.DispatchRevision != 0 || owned.DispatchConfirmedAt != nil) {
			return ErrReviewAuthorityIdentity
		}
	}
	return nil
}

// The cancellation scan extends the existing key ambiguity convention to the
// nested route binding, without changing the global loader or domain decoder.
type reviewCancellationAssignmentRecord struct{ domain.Assignment }

func (r *reviewCancellationAssignmentRecord) UnmarshalJSON(raw []byte) error {
	a, err := decodeReviewChildRecord[domain.Assignment](raw, []string{"route"})
	if err != nil {
		return err
	}
	var fields struct {
		Route json.RawMessage `json:"route"`
	}
	if err = json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields.Route) != 0 && !bytes.Equal(bytes.TrimSpace(fields.Route), []byte("null")) {
		if _, err = decodeReviewChildRecord[domain.ProviderRoute](fields.Route, []string{"workerId", "providerInstanceId", "model", "options", "quotaPoolId"}); err != nil {
			return err
		}
	}
	r.Assignment = a
	return nil
}

// Only recognized runtime states can be cancelled; malformed state is never
// repaired by converting it to a terminal attempt.
func reviewCancellationExecutionCoherent(a domain.Attempt) bool {
	switch a.Progress {
	case domain.ProgressQueued, domain.ProgressBlocked, domain.ProgressReady, domain.ProgressActive, domain.ProgressNeedsInput, domain.ProgressWaitingExternal, domain.ProgressVerifying, domain.ProgressSucceeded, domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSkipped:
	default:
		return false
	}
	switch a.Control {
	case "", domain.ControlUnassigned, domain.ControlPreparing, domain.ControlRunning, domain.ControlDraining, domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlResuming, domain.ControlStopped, domain.ControlWaitingExternal:
	default:
		return false
	}
	return a.Progress.Terminal() || a.CompletedAt == nil
}

func reviewCancellationUnstarted(a domain.Attempt) bool {
	if a.Control != domain.ControlUnassigned {
		return false
	}
	switch a.Progress {
	case domain.ProgressQueued, domain.ProgressReady, domain.ProgressBlocked:
		return true
	}
	return false
}

func reviewCancellationTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
