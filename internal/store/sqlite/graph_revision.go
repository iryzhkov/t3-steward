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

const coordinatorMigrationV14 = `
CREATE TABLE IF NOT EXISTS coordinator_graph_revisions(run_id TEXT NOT NULL, revision INTEGER NOT NULL, record TEXT NOT NULL, PRIMARY KEY(run_id,revision));
CREATE TABLE IF NOT EXISTS coordinator_graph_requests(id TEXT PRIMARY KEY, actor TEXT NOT NULL, request TEXT NOT NULL, result TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS immutable_graph_revision BEFORE UPDATE ON coordinator_graph_revisions BEGIN SELECT RAISE(ABORT,'graph revision is immutable'); END;
CREATE TRIGGER IF NOT EXISTS immutable_graph_request BEFORE UPDATE ON coordinator_graph_requests BEGIN SELECT RAISE(ABORT,'graph request is immutable'); END;
CREATE TRIGGER IF NOT EXISTS release_clone_source AFTER DELETE ON coordinator_workflow_runs BEGIN DELETE FROM coordinator_retention_pins WHERE owner='clone:'||OLD.id; END;
`

var ErrStaleGraph = errors.New("graph revision or inspected run changed")

func (s *Store) GraphAmendmentReplay(ctx context.Context, request domain.GraphAmendment, actor string) (domain.GraphAmendmentResult, bool, error) {
	var result domain.GraphAmendmentResult
	var storedActor, raw, reply string
	err := s.db.QueryRowContext(ctx, "SELECT actor,request,result FROM coordinator_graph_requests WHERE id=?", request.ID).Scan(&storedActor, &raw, &reply)
	if errors.Is(err, sql.ErrNoRows) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	encoded, _ := json.Marshal(request)
	if storedActor != actor || raw != string(encoded) {
		return result, false, errors.New("amendment request ID replay changed content or actor")
	}
	if err = json.Unmarshal([]byte(reply), &result); err != nil {
		return result, false, err
	}
	result.Replay = true
	return result, true, nil
}

type GraphCommit struct {
	Request domain.GraphAmendment
	Actor   string
	Before  domain.WorkflowRun
	Tasks   []domain.Task
	Inputs  []domain.Artifact
	Now     time.Time
}

// CommitGraphAmendment publishes the complete candidate and input metadata in
// one transaction. Every affected attempt revision is advanced, invalidating
// planning work computed before an amendment won the serialization race.
func (s *Store) CommitGraphAmendment(ctx context.Context, c GraphCommit) (domain.GraphAmendmentResult, error) {
	var result domain.GraphAmendmentResult
	if err := domain.ValidateGraphAmendment(c.Request); err != nil {
		return result, err
	}
	if c.Actor == "" || c.Now.IsZero() {
		return result, errors.New("amendment actor and time required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	var actor, requestRaw, reply string
	err = tx.QueryRowContext(ctx, "SELECT actor,request,result FROM coordinator_graph_requests WHERE id=?", c.Request.ID).Scan(&actor, &requestRaw, &reply)
	encoded, _ := json.Marshal(c.Request)
	if err == nil {
		if actor != c.Actor || requestRaw != string(encoded) {
			return result, errors.New("amendment request ID replay changed content or actor")
		}
		if err = json.Unmarshal([]byte(reply), &result); err != nil {
			return result, err
		}
		result.Replay = true
		return result, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	snapshot, err := loadWorkflowProjectionTx(ctx, tx, c.Request.RunID)
	if err != nil {
		return result, err
	}
	run := snapshot.Run
	if run.GraphRevision != c.Request.ExpectedRevision || !reflect.DeepEqual(run, c.Before) {
		return result, ErrStaleGraph
	}
	if c.Request.Operation == "clone" {
		return result, errors.New("clone requires clone transaction")
	}
	if run.Sink != nil && run.Sink.Progress.Terminal() {
		return result, errors.New("completed sink forbids graph amendments")
	}
	if run.Progress.Terminal() {
		return result, errors.New("terminal run forbids graph amendments")
	}
	if err = domain.ValidateGraphTasks(run, c.Tasks); err != nil {
		return result, err
	}
	if err = validateGraphCandidateTx(ctx, tx, c, run, snapshot.Tasks); err != nil {
		return result, err
	}
	before := map[string]domain.Task{}
	after := map[string]domain.Task{}
	for _, task := range snapshot.Tasks {
		before[task.ID] = task
	}
	for _, task := range c.Tasks {
		after[task.ID] = task
	}
	for id := range before {
		if _, ok := after[id]; !ok {
			return result, errors.New("task deletion is unsupported")
		}
	}
	changed := map[string]bool{}
	for id, task := range after {
		if old, ok := before[id]; !ok || !reflect.DeepEqual(old, task) {
			changed[id] = true
		}
	}
	if len(changed) == 0 {
		return result, errors.New("amendment makes no definition change")
	}
	// Freeze every task which has ever had an assignment, even if it was released.
	for _, a := range snapshot.Attempts {
		if !changed[a.TaskID] {
			continue
		}
		if a.Progress.Terminal() || a.AssignmentID != "" || (a.Control != "" && a.Control != domain.ControlUnassigned) {
			return result, errors.New("terminal or previously assigned task cannot be edited")
		}
		for _, assignment := range snapshot.Assignments {
			if assignment.AttemptID == a.ID {
				return result, errors.New("task has assignment history")
			}
		}
	}
	// Only immutable retained input artifacts can be attached.
	artifacts, err := loadJSON[domain.Artifact](ctx, tx, "coordinator_artifacts")
	if err != nil {
		return result, err
	}
	inputByID := map[string]domain.Artifact{}
	for _, a := range append(artifacts, c.Inputs...) {
		inputByID[a.ID] = a
	}
	for _, task := range c.Tasks {
		for _, id := range append([]string{task.PromptArtifactID}, task.InputArtifactIDs...) {
			a, ok := inputByID[id]
			if !ok || a.Kind != domain.ArtifactInput || a.WorkflowRunID != run.ID || (a.TaskID != "" && a.TaskID != task.ID) {
				return result, errors.New("task input custody does not match run/task")
			}
			if id == task.PromptArtifactID && a.TaskID != task.ID {
				return result, errors.New("prompt must belong to task")
			}
		}
	}
	// Capture the original revision before changing the current pointer.
	if run.Graph == nil {
		original := domain.GraphDefinition{RunID: run.ID, Revision: run.GraphRevision, Actor: "submission", Reason: "original retained definition", RequestID: "initial:" + run.ID, CreatedAt: run.CreatedAt, Tasks: snapshot.Tasks, Digest: domain.GraphDigest(snapshot.Tasks)}
		if err = insertGraphTx(ctx, tx, original); err != nil {
			return result, err
		}
	}
	graph := domain.GraphDefinition{RunID: run.ID, Revision: run.GraphRevision + 1, Parent: run.GraphRevision, Actor: c.Actor, Reason: c.Request.Reason, RequestID: c.Request.ID, CreatedAt: c.Now.UTC(), Tasks: c.Tasks, Digest: domain.GraphDigest(c.Tasks)}
	run.GraphRevision = graph.Revision
	run.Graph = &graph
	run, err = domain.BindRunSink(run, c.Tasks)
	if err != nil {
		return result, err
	}
	run.Revision++
	run.UpdatedAt = c.Now.UTC()
	// Persist input metadata before the graph: rollback removes both on any failure.
	for _, a := range c.Inputs {
		if err = insertImmutableJSON(ctx, tx, "graph input", a.ID,
			"INSERT INTO coordinator_artifacts(id,workflow_run_id,task_id,attempt_id,sha256,record) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
			[]any{a.ID, a.WorkflowRunID, a.TaskID, a.AttemptID, a.SHA256},
			"SELECT record FROM coordinator_artifacts WHERE id=?", []any{a.ID}, a); err != nil {
			return result, err
		}
	}
	for _, a := range snapshot.Attempts {
		if changed[a.TaskID] {
			expected := a.Revision
			a.Revision++
			a.UpdatedAt = c.Now.UTC()
			a.Progress = domain.ProgressBlocked
			if err = updateAttemptTx(ctx, tx, a, expected); err != nil {
				return result, err
			}
		}
	}
	for id := range changed {
		if _, ok := before[id]; !ok {
			a := domain.Attempt{ID: "attempt:" + id + ":1", WorkflowRunID: run.ID, TaskID: id, Number: 1, Revision: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, UpdatedAt: c.Now.UTC()}
			if err = upsertJSON(ctx, tx, "new graph attempt", a.ID, "INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)", []any{a.ID, run.ID, id, 1, 1}, a); err != nil {
				return result, err
			}
		}
	}
	if err = saveGraphRunTx(ctx, tx, run); err != nil {
		return result, err
	}
	if err = validateRunGraphEdgesTx(ctx, tx); err != nil {
		return result, err
	}
	if err = insertGraphTx(ctx, tx, graph); err != nil {
		return result, err
	}
	event := nativeAuditInput{ID: "graph-amended:" + c.Request.ID, Kind: "graph-amended", WorkflowRunID: run.ID, TargetType: domain.AdminTargetWorkflowRun, TargetID: run.ID, Actor: c.Actor, Reason: c.Request.Reason, CreatedAt: c.Now.UTC(), Detail: nativeAuditDetail{ExpectedRevision: graph.Parent, Revision: graph.Revision, IdempotencyIdentity: c.Request.ID, Outcome: graph.Digest}}
	if _, err = insertNativeAuditEventTx(ctx, tx, event); err != nil {
		return result, err
	}
	result = domain.GraphAmendmentResult{Run: run, Graph: graph}
	raw, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_graph_requests(id,actor,request,result) VALUES(?,?,?,?)", c.Request.ID, c.Actor, string(encoded), string(raw)); err != nil {
		return result, err
	}
	return result, tx.Commit()
}
func insertGraphTx(ctx context.Context, tx *sql.Tx, graph domain.GraphDefinition) error {
	return insertImmutableJSON(ctx, tx, "graph revision", fmt.Sprintf("%s/%d", graph.RunID, graph.Revision),
		"INSERT INTO coordinator_graph_revisions(run_id,revision,record) VALUES(?,?,?) ON CONFLICT(run_id,revision) DO NOTHING", []any{graph.RunID, graph.Revision},
		"SELECT record FROM coordinator_graph_revisions WHERE run_id=? AND revision=?", []any{graph.RunID, graph.Revision}, graph)
}
func saveGraphRunTx(ctx context.Context, tx *sql.Tx, run domain.WorkflowRun) error {
	raw, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE coordinator_workflow_runs SET progress=?,revision=?,record=? WHERE id=?", run.Progress, run.Revision, string(raw), run.ID)
	return err
}
