package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

const coordinatorMigrationV34 = `
CREATE TABLE IF NOT EXISTS coordinator_review_materializations (
 checkpoint_id TEXT PRIMARY KEY REFERENCES coordinator_review_checkpoints(id),
 workflow_id TEXT NOT NULL UNIQUE REFERENCES coordinator_workflows(id),
 run_id TEXT NOT NULL UNIQUE REFERENCES coordinator_workflow_runs(id),
 record TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS immutable_review_materialization_update BEFORE UPDATE ON coordinator_review_materializations
 BEGIN SELECT RAISE(ABORT,'review materialization is immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_review_materialization_delete BEFORE DELETE ON coordinator_review_materializations
 BEGIN SELECT RAISE(ABORT,'review materialization is immutable'); END;
`

var ErrReviewMaterialization = errors.New("review child materialization conflict or incomplete graph")

// ReviewMaterialization is the original immutable creation receipt, not a
// current execution snapshot. Graph records are retained for corruption checks.
type ReviewMaterialization struct {
	Authority  review.FrozenAuthority
	Checkpoint review.CheckpointAuthority
	Graph      review.ChildGraph
}

// MaterializeReviewChild is an internal trusted preparation boundary. SQL checks
// metadata/content identities only; PrepareReviewChild's verified immutable file
// custody is a prerequisite. No public submission or production caller uses it.
func (s *Store) MaterializeReviewChild(ctx context.Context, expected review.FrozenAuthority, allocated review.CheckpointAuthority, prepared review.ChildPreparation) (ReviewMaterialization, error) {
	var zero ReviewMaterialization
	expected, err := expected.Canonical()
	if err != nil {
		return zero, err
	}
	// A bound replay is genuinely read-only, including after parent termination.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return zero, err
	}
	receipt, found, err := reviewChildReplayTx(ctx, tx, expected, allocated, prepared)
	tx.Rollback()
	if err != nil || found {
		return receipt, err
	}

	tx, err = s.reviewAuthorityWriteTx(ctx, expected.Parent)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback()
	// Recheck under the writer lock: another connection may have won.
	receipt, found, err = reviewChildReplayTx(ctx, tx, expected, allocated, prepared)
	if err != nil {
		return zero, err
	}
	if found {
		return receipt, tx.Commit()
	}
	frozen, cp, round, err := reviewChildAuthorityTx(ctx, tx, expected, allocated)
	if err != nil {
		return zero, err
	}
	if err := reviewParentCurrentTx(ctx, tx, frozen.Parent, false); err != nil {
		return zero, err
	}
	if round.Combined != "pending" || !round.UpdatedAt.Equal(round.CreatedAt) || !round.Deadline.IsZero() || round.Revision != 1 || round.ReplyText != "" || round.Terminal() {
		return zero, ErrReviewMaterialization
	}
	for _, m := range round.Reviewers {
		if m.State != "pending" || m.Verdict != nil || m.Failure != "" || m.ReviewMD != "" || len(m.VerdictJSON) != 0 || m.ReviewAvailable || m.VerdictAvailable {
			return zero, ErrReviewMaterialization
		}
	}
	graph, err := prepared.Build(frozen, cp, round.CreatedAt)
	if err != nil {
		return zero, err
	}
	if !prepared.Deadline().After(s.now()) {
		return zero, ErrReviewMaterialization
	}
	// Reject orphan/colliding initial graphs rather than adopting them.
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM coordinator_graph_revisions WHERE run_id=?", cp.RoundID).Scan(&count); err != nil {
		return zero, err
	}
	if count != 0 {
		return zero, ErrReviewMaterialization
	}
	if err := insertReviewChildTx(ctx, tx, graph); err != nil {
		return zero, err
	}
	if err := bindNodeEdgesTx(ctx, tx); err != nil {
		return zero, err
	}
	if err := ensureInitialGraphsTx(ctx, tx); err != nil {
		return zero, err
	}
	round.Deadline = prepared.Deadline()
	round.Revision++
	round.UpdatedAt = s.now()
	raw, err := json.Marshal(round)
	if err != nil {
		return zero, err
	}
	result, err := tx.ExecContext(ctx, "UPDATE coordinator_review_rounds SET revision=?,record=? WHERE id=? AND revision=1", round.Revision, raw, round.ID)
	if err != nil {
		return zero, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return zero, err
	}
	if changed != 1 {
		return zero, ErrReviewMaterialization
	}
	receipt = ReviewMaterialization{Authority: frozen, Checkpoint: cp, Graph: graph}
	if err := upsertJSON(ctx, tx, "review materialization", cp.Key(),
		"INSERT INTO coordinator_review_materializations(checkpoint_id,workflow_id,run_id,record) VALUES(?,?,?,?)",
		[]any{cp.Key(), graph.Workflow.ID, graph.Run.ID}, receipt); err != nil {
		return zero, err
	}
	if err := validateReviewChildTx(ctx, tx, receipt); err != nil {
		return zero, err
	}
	return receipt, tx.Commit()
}

func reviewChildAuthorityTx(ctx context.Context, tx *sql.Tx, expected review.FrozenAuthority, allocated review.CheckpointAuthority) (review.FrozenAuthority, review.CheckpointAuthority, review.Round, error) {
	f, err := expectedReviewAuthorityTx(ctx, tx, expected)
	if err != nil {
		return f, review.CheckpointAuthority{}, review.Round{}, err
	}
	cp, err := loadReviewJSONTx[review.CheckpointAuthority](ctx, tx, "SELECT record FROM coordinator_review_checkpoints WHERE id=? AND authority_id=? AND round_id=? AND number=?", allocated.Key(), f.Key(), allocated.RoundID, allocated.Number)
	if err != nil {
		return f, cp, review.Round{}, err
	}
	if cp != allocated || cp.AuthorityKey != f.Key() || cp.RoundID != cp.Key() || cp.Number < 1 || cp.Number > f.Requirements.RoundLimit {
		return f, cp, review.Round{}, ErrReviewMaterialization
	}
	round, err := loadReviewJSONTx[review.Round](ctx, tx, "SELECT record FROM coordinator_review_rounds WHERE id=?", cp.RoundID)
	if err != nil {
		return f, cp, round, err
	}
	if round.ID != cp.RoundID || round.WorkflowRunID != cp.RoundID || round.Risk != f.Requirements.Risk || round.TemplateVersion != review.TemplateVersion ||
		round.BaseCommit != f.Parent.BaseCommit || round.HeadCommit != cp.Checkpoint.HeadCommit || round.InputManifestDigest != cp.Checkpoint.InputDigest || len(round.Reviewers) != len(f.Requirements.Members) || round.CreatedAt.IsZero() {
		return f, cp, round, ErrReviewMaterialization
	}
	for i, m := range f.Requirements.Members {
		got := round.Reviewers[i]
		if got.ID != m.ID || got.TaskID != cp.MemberTaskID(m.ID) || got.Role != m.Role || got.Route != m.Route || got.ProviderFamily != m.ProviderFamily || got.Tier != m.Tier || got.Required != m.Required {
			return f, cp, round, ErrReviewMaterialization
		}
	}
	return f, cp, round, nil
}

func reviewChildReplayTx(ctx context.Context, tx *sql.Tx, expected review.FrozenAuthority, allocated review.CheckpointAuthority, prepared review.ChildPreparation) (ReviewMaterialization, bool, error) {
	var zero ReviewMaterialization
	// Always compare stored issued authority/checkpoint even on replay.
	f, cp, round, err := reviewChildAuthorityTx(ctx, tx, expected, allocated)
	if err != nil {
		return zero, false, err
	}
	receipt, err := loadReviewJSONTx[ReviewMaterialization](ctx, tx, "SELECT record FROM coordinator_review_materializations WHERE checkpoint_id=? AND workflow_id=? AND run_id=?", cp.Key(), review.ChildWorkflowID(cp), cp.RoundID)
	if errors.Is(err, sql.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, true, err
	}
	graph, err := prepared.Build(f, cp, round.CreatedAt)
	if err != nil {
		return zero, true, err
	}
	if !reflect.DeepEqual(receipt, ReviewMaterialization{Authority: f, Checkpoint: cp, Graph: graph}) || !round.Deadline.Equal(prepared.Deadline()) {
		return zero, true, ErrReviewMaterialization
	}
	if err := validateReviewChildTx(ctx, tx, receipt); err != nil {
		return zero, true, err
	}
	return receipt, true, nil
}

func insertReviewChildTx(ctx context.Context, tx *sql.Tx, g review.ChildGraph) error {
	if err := upsertJSON(ctx, tx, "child workflow", g.Workflow.ID, "INSERT INTO coordinator_workflows(id,record) VALUES(?,?)", []any{g.Workflow.ID}, g.Workflow); err != nil {
		return err
	}
	r := g.Run
	if err := upsertJSON(ctx, tx, "child run", r.ID, "INSERT INTO coordinator_workflow_runs(id,workflow_id,schedule_id,progress,revision,record) VALUES(?,?,?,?,?,?)", []any{r.ID, r.WorkflowID, r.ScheduleID, r.Progress, r.Revision}, r); err != nil {
		return err
	}
	for _, t := range g.Tasks {
		if err := upsertJSON(ctx, tx, "child task", t.ID, "INSERT INTO coordinator_tasks(id,workflow_id,name,record) VALUES(?,?,?,?)", []any{t.ID, t.WorkflowID, t.Name}, t); err != nil {
			return err
		}
	}
	for _, a := range g.Attempts {
		if err := upsertJSON(ctx, tx, "child attempt", a.ID, "INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)", []any{a.ID, a.WorkflowRunID, a.TaskID, a.Number, a.Revision}, a); err != nil {
			return err
		}
	}
	for _, a := range g.Artifacts {
		if err := upsertJSON(ctx, tx, "child artifact", a.ID, "INSERT INTO coordinator_artifacts(id,workflow_run_id,task_id,attempt_id,sha256,record) VALUES(?,?,?,?,?,?)", []any{a.ID, a.WorkflowRunID, a.TaskID, a.AttemptID, a.SHA256}, a); err != nil {
			return err
		}
	}
	return nil
}

func validateReviewChildTx(ctx context.Context, tx *sql.Tx, receipt ReviewMaterialization) error {
	g := receipt.Graph
	w, err := loadReviewJSONTx[domain.Workflow](ctx, tx, "SELECT record FROM coordinator_workflows WHERE id=?", g.Workflow.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(w, g.Workflow) {
		return ErrReviewMaterialization
	}
	r, err := loadReviewJSONTx[domain.WorkflowRun](ctx, tx, "SELECT record FROM coordinator_workflow_runs WHERE id=? AND workflow_id=?", g.Run.ID, g.Workflow.ID)
	if err != nil {
		return err
	}
	if r.ID != g.Run.ID || r.WorkflowID != g.Workflow.ID || r.ScheduleID != "" || r.TriggerID != "" || r.Supervision != nil || r.Revision < 1 || r.GraphRevision != 1 ||
		!r.CreatedAt.Equal(g.Run.CreatedAt) || !reflect.DeepEqual(r.InputArtifactIDs, g.Run.InputArtifactIDs) || r.Sink == nil ||
		r.Sink.ID != g.Run.Sink.ID || r.Sink.Name != g.Run.Sink.Name || r.Sink.GraphRevision != 1 || !reflect.DeepEqual(r.Sink.Needs, g.Run.Sink.Needs) {
		return ErrReviewMaterialization
	}
	graph, err := loadReviewJSONTx[domain.GraphDefinition](ctx, tx, "SELECT record FROM coordinator_graph_revisions WHERE run_id=? AND revision=1", r.ID)
	if err != nil {
		return err
	}
	graphTasks := append([]domain.Task(nil), g.Tasks...)
	sort.Slice(graphTasks, func(i, j int) bool { return graphTasks[i].ID < graphTasks[j].ID })
	if graph.RunID != r.ID || graph.Revision != 1 || graph.Actor != "submission" || graph.Reason != "original retained definition" || graph.RequestID != "initial:"+r.ID ||
		!graph.CreatedAt.Equal(r.CreatedAt) || graph.Digest != domain.GraphDigest(graphTasks) || !reflect.DeepEqual(graph.Tasks, graphTasks) {
		return ErrReviewMaterialization
	}
	if r.Graph != nil && !reflect.DeepEqual(*r.Graph, graph) {
		return ErrReviewMaterialization
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM coordinator_tasks WHERE workflow_id=?", w.ID).Scan(&count); err != nil {
		return err
	}
	if count != len(g.Tasks) {
		return ErrReviewMaterialization
	}
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM coordinator_workflow_runs WHERE workflow_id=?", w.ID).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return ErrReviewMaterialization
	}
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM coordinator_attempts WHERE workflow_run_id=? AND (number<1 OR task_id NOT IN (SELECT id FROM coordinator_tasks WHERE workflow_id=?))", r.ID, w.ID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return ErrReviewMaterialization
	}
	for _, check := range []struct {
		query    string
		expected int
	}{
		{"SELECT count(*) FROM coordinator_graph_revisions WHERE run_id=?", 1},
		{"SELECT count(*) FROM coordinator_attempts WHERE workflow_run_id=? AND number=1", len(g.Attempts)},
		{"SELECT count(*) FROM coordinator_artifacts WHERE workflow_run_id=? AND json_extract(record,'$.kind')='input'", len(g.Artifacts)},
	} {
		if err := tx.QueryRowContext(ctx, check.query, r.ID).Scan(&count); err != nil {
			return err
		}
		if count != check.expected {
			return ErrReviewMaterialization
		}
	}
	for i, t := range g.Tasks {
		actual, err := loadReviewJSONTx[domain.Task](ctx, tx, "SELECT record FROM coordinator_tasks WHERE id=? AND workflow_id=? AND name=?", t.ID, t.WorkflowID, t.Name)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, t) {
			return ErrReviewMaterialization
		}
		initial := g.Attempts[i]
		a, err := loadReviewJSONTx[domain.Attempt](ctx, tx, "SELECT record FROM coordinator_attempts WHERE id=? AND workflow_run_id=? AND task_id=? AND number=1", initial.ID, r.ID, t.ID)
		if err != nil {
			return err
		}
		if a.ID != initial.ID || a.WorkflowRunID != r.ID || a.TaskID != t.ID || a.Number != 1 || a.Revision < 1 || a.SupervisionActivationID != "" || a.SupervisionActivationEpoch != 0 {
			return ErrReviewMaterialization
		}
	}
	for _, a := range g.Artifacts {
		actual, err := loadReviewJSONTx[domain.Artifact](ctx, tx, "SELECT record FROM coordinator_artifacts WHERE id=? AND workflow_run_id=? AND task_id=? AND attempt_id='' AND sha256=?", a.ID, r.ID, a.TaskID, a.SHA256)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, a) {
			return ErrReviewMaterialization
		}
	}
	return nil
}
