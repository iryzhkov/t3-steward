package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"reflect"
	"time"
)

type reviewChildCurrent struct {
	checkpoint  review.CheckpointAuthority
	round       review.Round
	attempts    map[string]domain.Attempt
	assignments map[string]domain.Assignment
	reason      string
}

// Shared cancellation validation observes original issued authority and CURRENT
// parent disposition, including complete child history, without requiring a live parent.
func reviewChildCurrentTx(ctx context.Context, tx *sql.Tx, expected review.FrozenAuthority, allocated review.CheckpointAuthority, nowUTC time.Time) (reviewChildCurrent, error) {
	var state reviewChildCurrent
	f, cp, round, err := reviewChildAuthorityTx(ctx, tx, expected, allocated)
	if err != nil {
		return state, err
	}
	var authorityRun, authorityTask string
	if err = tx.QueryRowContext(ctx, "SELECT run_id,task_id FROM coordinator_review_authorities WHERE id=?", f.Key()).Scan(&authorityRun, &authorityTask); err != nil {
		return state, err
	}
	if authorityRun != f.Parent.RunID || authorityTask != f.Parent.TaskID {
		return state, ErrReviewAuthorityIdentity
	}
	var indexedRoundRevision int64
	if err = tx.QueryRowContext(ctx, "SELECT revision FROM coordinator_review_rounds WHERE id=?", cp.RoundID).Scan(&indexedRoundRevision); err != nil {
		return state, err
	}
	if round.Revision < 1 || indexedRoundRevision != round.Revision {
		return state, ErrReviewRoundConflict
	}
	receipt, err := loadReviewJSONTx[ReviewMaterialization](ctx, tx, "SELECT record FROM coordinator_review_materializations WHERE checkpoint_id=? AND workflow_id=? AND run_id=?", cp.Key(), review.ChildWorkflowID(cp), cp.RoundID)
	if errors.Is(err, sql.ErrNoRows) {
		// An allocated checkpoint has no child custody yet. Do not let a missing
		// or corrupt receipt hide a graph that was already materialized.
		var childRows int
		if err := tx.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM coordinator_review_materializations WHERE checkpoint_id=? OR run_id=?) + (SELECT count(*) FROM coordinator_workflow_runs WHERE id=?) + (SELECT count(*) FROM coordinator_workflows WHERE id=?)", cp.Key(), cp.RoundID, cp.RoundID, review.ChildWorkflowID(cp)).Scan(&childRows); err != nil {
			return state, err
		}
		if childRows != 0 || !round.Deadline.IsZero() {
			return state, ErrReviewMaterialization
		}
		reason, err := reviewAllocatedParentDispositionTx(ctx, tx, f.Parent)
		if err != nil {
			return state, err
		}
		return reviewChildCurrent{checkpoint: cp, round: round, reason: reason}, nil
	}
	if err != nil {
		return state, err
	}
	if !reflect.DeepEqual(receipt.Authority, f) || receipt.Checkpoint != cp ||
		receipt.Graph.Run.ID != cp.RoundID || receipt.Graph.Workflow.ID != review.ChildWorkflowID(cp) {
		return state, ErrReviewMaterialization
	}
	if err = validateReviewChildTx(ctx, tx, receipt); err != nil {
		return state, err
	}
	if len(receipt.Graph.Tasks) == 0 || receipt.Graph.Tasks[0].Deadline == nil {
		return state, ErrReviewMaterialization
	}
	deadline := *receipt.Graph.Tasks[0].Deadline
	if deadline.IsZero() || !round.Deadline.Equal(deadline) {
		return state, ErrReviewMaterialization
	}
	for _, task := range receipt.Graph.Tasks {
		if task.Deadline == nil || !task.Deadline.Equal(deadline) {
			return state, ErrReviewMaterialization
		}
	}
	parent, latest, err := reviewParentAttemptSnapshotTx(ctx, tx, f.Parent)
	if err != nil {
		return state, err
	}
	owners, err := validateReviewChildAttemptsTx(ctx, tx, receipt.Graph)
	if err != nil {
		return state, err
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
		return state, err
	}
	assignments, err := reviewCancellationAssignmentsTx(ctx, tx, attempts, parentIDs, f.Parent.AssignmentID)
	if err != nil {
		return state, err
	}
	if err = reviewCancellationParentBindings(f.Parent, parentIDs, assignments); err != nil {
		return state, err
	}
	for _, assignment := range assignments {
		owner, ok := attempts[assignment.AttemptID]
		if !ok {
			continue
		}
		if assignment.Project != "" && assignment.Project != f.Parent.Repository {
			return state, ErrReviewMaterialization
		}
		for _, task := range receipt.Graph.Tasks {
			if task.ID != owner.TaskID {
				continue
			}
			if len(task.Routes) != 1 || assignment.Route.ProviderInstanceID != task.Routes[0].ProviderInstanceID || assignment.Route.Model != task.Routes[0].Model {
				return state, ErrReviewMaterialization
			}
		}
	}
	reason, err := reviewCancellationParentTx(ctx, tx, f.Parent, parent, latest, assignments)
	if err != nil {
		return state, err
	}
	now := nowUTC.UTC()
	if !deadline.After(now) {
		reason = "review deadline reached"
	}
	state = reviewChildCurrent{checkpoint: cp, round: round, attempts: attempts, assignments: assignments, reason: reason}
	return state, nil
}

// Stored receipts are immutable ownership, never a prefix-based classification.
// Both indexed and runtime graph identities must agree before an owner is used.
func storedReviewChildrenTx(ctx context.Context, tx *sql.Tx) ([]ReviewMaterialization, error) {
	rows, err := tx.QueryContext(ctx, "SELECT checkpoint_id,workflow_id,run_id,record FROM coordinator_review_materializations ORDER BY checkpoint_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var receipts []ReviewMaterialization
	for rows.Next() {
		var checkpoint, workflow, run string
		var raw []byte
		if err = rows.Scan(&checkpoint, &workflow, &run, &raw); err != nil {
			return nil, err
		}
		receipt, e := decodeReviewChildRecord[ReviewMaterialization](raw, []string{"Authority", "Checkpoint", "Graph"})
		if e != nil {
			return nil, fmt.Errorf("stored review %s: %w", checkpoint, e)
		}
		if checkpoint != receipt.Checkpoint.Key() || workflow != receipt.Graph.Workflow.ID || run != receipt.Graph.Run.ID || run != receipt.Checkpoint.RoundID || workflow != review.ChildWorkflowID(receipt.Checkpoint) {
			return nil, fmt.Errorf("stored review %s: %w", checkpoint, ErrReviewMaterialization)
		}
		receipts = append(receipts, receipt)
	}
	return receipts, rows.Err()
}

// ReconcileMaterializedReviewChildren includes allocated-only rounds before
// coordinator snapshots. Each round commits under its parent writer lock.
func (s *Store) ReconcileMaterializedReviewChildren(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// A damaged materialization index must not turn child custody into allocation.
	receipts, err := storedReviewChildrenTx(ctx, tx)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("enumerate materialized reviews: %w", err)
	}
	rows, err := tx.QueryContext(ctx, "SELECT c.id,c.authority_id,c.round_id,c.number,c.record,a.record FROM coordinator_review_checkpoints c LEFT JOIN coordinator_review_authorities a ON a.id=c.authority_id ORDER BY c.id")
	if err != nil {
		tx.Rollback()
		return err
	}
	type issued struct {
		f  review.FrozenAuthority
		cp review.CheckpointAuthority
	}
	var checkpoints []issued
	issuedByKey := map[string]issued{}
	for rows.Next() {
		var id, authority, round string
		var number int
		var cpRaw, fRaw []byte
		if err = rows.Scan(&id, &authority, &round, &number, &cpRaw, &fRaw); err != nil {
			break
		}
		var item issued
		item.cp, err = decodeReviewChildRecord[review.CheckpointAuthority](cpRaw, []string{"AuthorityKey", "Checkpoint", "Number", "RoundID"})
		if err != nil {
			break
		}
		item.f, err = decodeReviewChildRecord[review.FrozenAuthority](fRaw, []string{"Parent", "Requirements", "RequirementsDigest"})
		if err != nil {
			break
		}
		if item.cp.Key() != id || item.cp.AuthorityKey != authority || item.cp.RoundID != round || item.cp.Number != number || item.f.Key() != authority {
			err = ErrReviewAuthorityIdentity
			break
		}
		checkpoints = append(checkpoints, item)
		issuedByKey[id] = item
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	tx.Rollback()
	if err != nil {
		return fmt.Errorf("enumerate review checkpoints: %w", err)
	}
	// Every materialization must still have its original checkpoint/authority.
	// Enumeration of checkpoints alone must not silently skip orphan custody.
	for _, receipt := range receipts {
		item, ok := issuedByKey[receipt.Checkpoint.Key()]
		if !ok || item.cp != receipt.Checkpoint || !reflect.DeepEqual(item.f, receipt.Authority) {
			return ErrReviewMaterialization
		}
	}
	for _, item := range checkpoints {
		if _, err = s.ReconcileReviewChildCancellation(ctx, item.f, item.cp); err != nil {
			return fmt.Errorf("reconcile review %s parent %s: %w", item.cp.Key(), item.f.Parent.AttemptID, err)
		}
	}
	return nil
}

// Allocated-only rounds use the same checked parent history and custody
// classification as materialized rounds, without inventing child attempts.
func reviewAllocatedParentDispositionTx(ctx context.Context, tx *sql.Tx, p review.ParentBinding) (string, error) {
	parent, latest, err := reviewParentAttemptSnapshotTx(ctx, tx, p)
	if err != nil {
		return "", err
	}
	parents := map[string]domain.Attempt{}
	err = scanReviewChildRows[domain.Attempt](ctx, tx, "SELECT id,workflow_run_id,task_id,record FROM coordinator_attempts", 3,
		append(append([]string{}, reviewChildAttemptKeys...), "assignmentId", "threadId", "progress", "control", "completedAt"), func(_ []string, a domain.Attempt) error {
			if a.WorkflowRunID == p.RunID && a.TaskID == p.TaskID {
				if !reviewCancellationExecutionCoherent(a) || a.SupervisionActivationID != "" || a.SupervisionActivationEpoch != 0 {
					return ErrReviewAuthorityIdentity
				}
				parents[a.ID] = a
			}
			return nil
		})
	if err != nil {
		return "", err
	}
	assignments, err := reviewCancellationAssignmentsTx(ctx, tx, nil, parents, p.AssignmentID)
	if err != nil {
		return "", err
	}
	if err = reviewCancellationParentBindings(p, parents, assignments); err != nil {
		return "", err
	}
	return reviewCancellationParentTx(ctx, tx, p, parent, latest, assignments)
}

// Retry shape is checked only for validated materialized ownership. Ordinary
// retries retain their existing contract and are never classified by ID spelling.
func (s *Store) reviewRetryAdmissionTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt, next *domain.Attempt) (string, error) {
	refusal, err := s.reviewAttemptAdmissionTx(ctx, tx, attempt)
	if err != nil || refusal != "" {
		return refusal, err
	}
	receipts, err := storedReviewChildrenTx(ctx, tx)
	if err != nil {
		return "", err
	}
	for _, receipt := range receipts {
		owners, err := validateReviewChildAttemptsTx(ctx, tx, receipt.Graph)
		if err != nil {
			return "", err
		}
		if _, owned := owners[attempt.ID]; !owned {
			continue
		}
		latest := 0
		for id, owner := range owners {
			if owner.task != attempt.TaskID {
				continue
			}
			stored, err := loadAttemptTx(ctx, tx, id)
			if err != nil {
				return "", err
			}
			if stored.Number > latest {
				latest = stored.Number
			}
		}
		if next == nil || next.ID == "" || next.ID == attempt.ID || next.WorkflowRunID != attempt.WorkflowRunID || next.TaskID != attempt.TaskID ||
			next.Number != latest+1 || next.Revision != 1 || !reviewCancellationUnstarted(*next) || next.AssignmentID != "" || next.ThreadID != "" ||
			next.SupervisionActivationID != "" || next.SupervisionActivationEpoch != 0 || next.StartedAt != nil || next.CompletedAt != nil ||
			next.CheckpointArtifactID != "" || next.FinalSummaryArtifactID != "" {
			return "malformed review retry identity or execution shape", nil
		}
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM coordinator_attempts WHERE id=?", next.ID).Scan(&count); err != nil {
			return "", err
		}
		if count != 0 {
			return "review retry identity already exists", nil
		}
	}
	return "", nil
}

// CURRENT transaction proof is independent of any preceding automatic pass.
// Empty reason means admitted. Corruption is an error, never an eligibility skip.
func (s *Store) reviewAttemptAdmissionTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt) (string, error) {
	receipts, err := storedReviewChildrenTx(ctx, tx)
	if err != nil {
		return "", err
	}
	owned := false
	reason := ""
	for _, receipt := range receipts {
		current, e := reviewChildCurrentTx(ctx, tx, receipt.Authority, receipt.Checkpoint, s.now())
		if e != nil {
			return "", fmt.Errorf("review admission %s: %w", receipt.Checkpoint.Key(), e)
		}
		if _, ok := current.attempts[attempt.ID]; !ok {
			continue
		}
		if owned {
			return "", ErrReviewMaterialization
		}
		owned = true
		reason = current.reason
		if reason == "" {
			run, e := loadReviewJSONTx[domain.WorkflowRun](ctx, tx, "SELECT record FROM coordinator_workflow_runs WHERE id=?", current.checkpoint.RoundID)
			if e != nil {
				return "", e
			}
			if run.Progress.Terminal() || run.Sink != nil && run.Sink.Progress.Terminal() {
				reason = "review child run settled"
			}
			if current.round.Combined != "pending" || current.round.Terminal() {
				reason = "review round settled"
			}
			for _, member := range current.round.Reviewers {
				if member.TaskID == attempt.TaskID {
					if member.State != "pending" {
						reason = "review member settled"
					}
				}
			}
		}
	}
	return reason, nil
}
