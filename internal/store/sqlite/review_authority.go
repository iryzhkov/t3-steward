package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

const coordinatorMigrationV32 = `
CREATE TABLE IF NOT EXISTS coordinator_review_authorities (
 id TEXT PRIMARY KEY,
 run_id TEXT NOT NULL,
 task_id TEXT NOT NULL,
 record TEXT NOT NULL,
 UNIQUE(run_id,task_id)
);
CREATE TABLE IF NOT EXISTS coordinator_review_checkpoints (
 id TEXT PRIMARY KEY,
 authority_id TEXT NOT NULL REFERENCES coordinator_review_authorities(id),
 checkpoint_id TEXT NOT NULL,
 number INTEGER NOT NULL CHECK(number>0),
 round_id TEXT NOT NULL UNIQUE REFERENCES coordinator_review_rounds(id),
 record TEXT NOT NULL,
 UNIQUE(authority_id,checkpoint_id),
 UNIQUE(authority_id,number)
);
CREATE TRIGGER IF NOT EXISTS immutable_review_authority_update BEFORE UPDATE ON coordinator_review_authorities
 BEGIN SELECT RAISE(ABORT,'review authority is immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_review_authority_delete BEFORE DELETE ON coordinator_review_authorities
 BEGIN SELECT RAISE(ABORT,'review authority is immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_review_checkpoint_update BEFORE UPDATE ON coordinator_review_checkpoints
 BEGIN SELECT RAISE(ABORT,'review checkpoint is immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_review_checkpoint_delete BEFORE DELETE ON coordinator_review_checkpoints
 BEGIN SELECT RAISE(ABORT,'review checkpoint is immutable'); END;
`

var (
	ErrReviewAuthorityConflict = errors.New("frozen review authority or checkpoint changed")
	ErrReviewAuthorityIdentity = errors.New("review parent identity is not current and live")
	ErrReviewAuthorityLimit    = errors.New("frozen review round limit exhausted")
)

// reviewAuthorityWriteTx acquires SQLite's writer before reading the allocation
// count. Independent connections serialize rather than upgrading stale snapshots.
// The no-op parent write and attempt CAS share the allocation transaction.
func (s *Store) reviewAuthorityWriteTx(ctx context.Context, p review.ParentBinding) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE coordinator_workflow_runs SET revision=revision WHERE id=?", p.RunID); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}
func loadReviewJSONTx[T any](ctx context.Context, tx *sql.Tx, query string, args ...any) (T, error) {
	var value T
	var raw []byte
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return value, err
	}
	err := json.Unmarshal(raw, &value)
	return value, err
}
func reviewParentCurrentTx(ctx context.Context, tx *sql.Tx, p review.ParentBinding, initial bool) error {
	attempt, err := reviewParentAttemptTx(ctx, tx, p)
	if err != nil {
		return err
	}
	if attempt.ID != p.AttemptID || attempt.WorkflowRunID != p.RunID || attempt.TaskID != p.TaskID || !attempt.TurnLive() ||
		attempt.ThreadID != p.ThreadID || attempt.AssignmentID != p.AssignmentID || attempt.SupervisionActivationID != "" ||
		attempt.Revision < p.IssuedRevision || initial && attempt.Revision != p.IssuedRevision {
		return ErrReviewAuthorityIdentity
	}
	assignment, err := loadAssignmentTx(ctx, tx, p.AssignmentID)
	if err != nil {
		return err
	}
	if assignment.ID != p.AssignmentID || assignment.AttemptID != p.AttemptID || assignment.Epoch != p.AssignmentEpoch || assignment.State != domain.AssignmentClaimed ||
		assignment.ThreadID != p.ThreadID || assignment.Route.ProviderInstanceID+"/"+assignment.Route.Model != p.ExecutorRoute {
		return ErrReviewAuthorityIdentity
	}
	run, err := loadReviewJSONTx[domain.WorkflowRun](ctx, tx, "SELECT record FROM coordinator_workflow_runs WHERE id=?", p.RunID)
	if err != nil {
		return err
	}
	if run.ID != p.RunID || run.Progress.Terminal() {
		return ErrReviewAuthorityIdentity
	}
	projection, err := loadWorkflowProjectionTx(ctx, tx, p.RunID)
	var task domain.Task
	if err == nil {
		for _, candidate := range projection.Tasks {
			if candidate.ID == p.TaskID {
				task = candidate
			}
		}
	}
	if err != nil {
		return err
	}
	if task.ID != p.TaskID || task.WorkflowID != run.WorkflowID || task.RunID != "" && task.RunID != p.RunID {
		return ErrReviewAuthorityIdentity
	}
	workflow, err := loadReviewJSONTx[domain.Workflow](ctx, tx, "SELECT record FROM coordinator_workflows WHERE id=?", run.WorkflowID)
	if err != nil {
		return err
	}
	if workflow.ID != run.WorkflowID || workflow.Project != p.Repository {
		return ErrReviewAuthorityIdentity
	}
	return updateAttemptTx(ctx, tx, attempt, attempt.Revision)
}
func expectedReviewAuthorityTx(ctx context.Context, tx *sql.Tx, expected review.FrozenAuthority) (review.FrozenAuthority, error) {
	stored, err := loadReviewJSONTx[review.FrozenAuthority](ctx, tx, "SELECT record FROM coordinator_review_authorities WHERE id=?", expected.Key())
	if err != nil {
		return stored, err
	}
	canonical, err := stored.Canonical()
	if err != nil {
		return review.FrozenAuthority{}, err
	}
	if !reflect.DeepEqual(canonical, expected) {
		return review.FrozenAuthority{}, ErrReviewAuthorityConflict
	}
	return canonical, nil
}

// FreezeReviewAuthority accepts ONLY a trusted coordinator admission snapshot.
// No executor/manifest/CLI path calls it. Replay cannot replace risk or identity,
// including with a rehashed weaker policy or a new parent attempt.
func (s *Store) FreezeReviewAuthority(ctx context.Context, expected review.FrozenAuthority) (review.FrozenAuthority, error) {
	return s.freezeReviewAuthority(ctx, expected, false)
}
func (s *Store) FreezeDeclaredReviewAuthority(ctx context.Context, expected review.FrozenAuthority) (review.FrozenAuthority, error) {
	return s.freezeReviewAuthority(ctx, expected, true)
}
func (s *Store) freezeReviewAuthority(ctx context.Context, expected review.FrozenAuthority, declared bool) (review.FrozenAuthority, error) {
	expected, err := expected.Canonical()
	if err != nil {
		return review.FrozenAuthority{}, err
	}
	tx, err := s.reviewAuthorityWriteTx(ctx, expected.Parent)
	if err != nil {
		return review.FrozenAuthority{}, err
	}
	defer tx.Rollback()
	if declared {
		if err := validateDeclaredAuthorityTx(ctx, tx, expected); err != nil {
			return review.FrozenAuthority{}, err
		}
	}
	stored, err := expectedReviewAuthorityTx(ctx, tx, expected)
	initial := errors.Is(err, sql.ErrNoRows)
	if err != nil && !initial {
		return review.FrozenAuthority{}, err
	}
	if err := reviewParentCurrentTx(ctx, tx, expected.Parent, initial); err != nil {
		return review.FrozenAuthority{}, err
	}
	if initial {
		raw, err := json.Marshal(expected)
		if err != nil {
			return review.FrozenAuthority{}, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_review_authorities(id,run_id,task_id,record) VALUES(?,?,?,?)", expected.Key(), expected.Parent.RunID, expected.Parent.TaskID, raw); err != nil {
			return review.FrozenAuthority{}, err
		}
		stored = expected
	}
	if err := tx.Commit(); err != nil {
		return review.FrozenAuthority{}, err
	}
	return stored, nil
}

// AllocateReviewCheckpoint atomically reserves a numbered round and pending
// member identities from STORED requirements. It does not submit/park a child.
func (s *Store) AllocateReviewCheckpoint(ctx context.Context, expected review.FrozenAuthority, checkpoint review.Checkpoint) (review.CheckpointAuthority, error) {
	expected, err := expected.Canonical()
	if err != nil {
		return review.CheckpointAuthority{}, err
	}
	if err := checkpoint.Validate(); err != nil {
		return review.CheckpointAuthority{}, err
	}
	tx, err := s.reviewAuthorityWriteTx(ctx, expected.Parent)
	if err != nil {
		return review.CheckpointAuthority{}, err
	}
	defer tx.Rollback()
	frozen, err := expectedReviewAuthorityTx(ctx, tx, expected)
	if err != nil {
		return review.CheckpointAuthority{}, err
	}
	if err := reviewParentCurrentTx(ctx, tx, frozen.Parent, false); err != nil {
		return review.CheckpointAuthority{}, err
	}
	a := review.CheckpointAuthority{AuthorityKey: frozen.Key(), Checkpoint: checkpoint}
	stored, err := loadReviewJSONTx[review.CheckpointAuthority](ctx, tx, "SELECT record FROM coordinator_review_checkpoints WHERE id=?", a.Key())
	if err == nil {
		if stored.AuthorityKey != a.AuthorityKey || stored.Checkpoint != checkpoint {
			return review.CheckpointAuthority{}, ErrReviewAuthorityConflict
		}
		if err := tx.Commit(); err != nil {
			return review.CheckpointAuthority{}, err
		}
		return stored, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return review.CheckpointAuthority{}, err
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM coordinator_review_checkpoints WHERE authority_id=?", frozen.Key()).Scan(&count); err != nil {
		return review.CheckpointAuthority{}, err
	}
	if count >= frozen.Requirements.RoundLimit {
		return review.CheckpointAuthority{}, ErrReviewAuthorityLimit
	}
	a.Number = count + 1
	a.RoundID = a.Key()
	round := review.Round{ID: a.RoundID, WorkflowRunID: a.RoundID, Risk: frozen.Requirements.Risk, TemplateVersion: review.TemplateVersion, BaseCommit: frozen.Parent.BaseCommit, HeadCommit: checkpoint.HeadCommit, InputManifestDigest: checkpoint.InputDigest}
	for _, m := range frozen.Requirements.Members {
		round.Reviewers = append(round.Reviewers, m.Reviewer(a.MemberTaskID(m.ID)))
	}
	if err := round.Initialize(s.now()); err != nil {
		return review.CheckpointAuthority{}, err
	}
	raw, err := json.Marshal(round)
	if err != nil {
		return review.CheckpointAuthority{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_review_rounds(id,revision,record) VALUES(?,?,?)", round.ID, round.Revision, raw); err != nil {
		return review.CheckpointAuthority{}, err
	}
	raw, err = json.Marshal(a)
	if err != nil {
		return review.CheckpointAuthority{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_review_checkpoints(id,authority_id,checkpoint_id,number,round_id,record) VALUES(?,?,?,?,?,?)", a.Key(), a.AuthorityKey, checkpoint.ID, a.Number, a.RoundID, raw); err != nil {
		return review.CheckpointAuthority{}, err
	}
	if err := tx.Commit(); err != nil {
		return review.CheckpointAuthority{}, err
	}
	return a, nil
}

// CheckReviewAuthorityEvidence loads one coherent durable snapshot. trustedHead
// must come from a future coordinator completion probe, NEVER executor files.
// This internal read does not accept a task, grant a waiver, or establish HEAD.
func (s *Store) CheckReviewAuthorityEvidence(ctx context.Context, expected review.FrozenAuthority, checkpoint review.Checkpoint, trustedHead string) (string, error) {
	expected, err := expected.Canonical()
	if err != nil {
		return "", err
	}
	if err := checkpoint.Validate(); err != nil {
		return "", err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	frozen, err := expectedReviewAuthorityTx(ctx, tx, expected)
	if err != nil {
		return "", err
	}
	key := review.CheckpointAuthority{AuthorityKey: frozen.Key(), Checkpoint: checkpoint}
	a, err := loadReviewJSONTx[review.CheckpointAuthority](ctx, tx, "SELECT record FROM coordinator_review_checkpoints WHERE id=?", key.Key())
	if err != nil {
		return "", err
	}
	if a.Checkpoint != checkpoint {
		return "", ErrReviewAuthorityConflict
	}
	round, err := loadReviewJSONTx[review.Round](ctx, tx, "SELECT record FROM coordinator_review_rounds WHERE id=?", a.RoundID)
	if err != nil {
		return "", err
	}
	verdict, err := review.ValidateBoundEvidence(frozen, a, round, trustedHead)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return verdict, nil
}
