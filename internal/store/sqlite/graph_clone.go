package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func (s *Store) CommitGraphClone(ctx context.Context, c GraphCommit) (domain.GraphAmendmentResult, error) {
	var result domain.GraphAmendmentResult
	if err := domain.ValidateGraphAmendment(c.Request); err != nil {
		return result, err
	}
	if c.Request.Operation != "clone" || c.Actor == "" || c.Now.IsZero() {
		return result, errors.New("invalid clone commit")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	encoded, _ := json.Marshal(c.Request)
	var actor, request, reply string
	err = tx.QueryRowContext(ctx, "SELECT actor,request,result FROM coordinator_graph_requests WHERE id=?", c.Request.ID).Scan(&actor, &request, &reply)
	if err == nil {
		if actor != c.Actor || request != string(encoded) {
			return result, errors.New("clone request ID replay changed content or actor")
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
	source, err := loadWorkflowProjectionTx(ctx, tx, c.Request.RunID)
	if err != nil {
		return result, err
	}
	if source.Run.GraphRevision != c.Request.ExpectedRevision || !reflect.DeepEqual(source.Run, c.Before) {
		return result, ErrStaleGraph
	}
	run := domain.WorkflowRun{ID: "run:clone:" + c.Request.ID, WorkflowID: source.Run.WorkflowID, GraphRevision: 1, Revision: 1, Progress: domain.ProgressQueued, CreatedAt: c.Now.UTC(), UpdatedAt: c.Now.UTC()}
	if err = domain.ValidateGraphTasks(run, c.Tasks); err != nil {
		return result, err
	}
	if len(c.Tasks) != len(source.Tasks) {
		return result, errors.New("clone task count differs from source")
	}
	graph := domain.GraphDefinition{RunID: run.ID, Revision: 1, Actor: c.Actor, Reason: c.Request.Reason, RequestID: c.Request.ID, CreatedAt: c.Now.UTC(), Tasks: c.Tasks, Digest: domain.GraphDigest(c.Tasks), ClonedFrom: &domain.GraphReference{RunID: source.Run.ID, Revision: source.Run.GraphRevision}}
	run.Graph = &graph
	run, err = domain.BindRunSink(run, c.Tasks)
	if err != nil {
		return result, err
	}
	inputByID := map[string]domain.Artifact{}
	for _, a := range c.Inputs {
		if a.WorkflowRunID != run.ID || a.Kind != domain.ArtifactInput {
			return result, errors.New("invalid cloned input")
		}
		// Match retained source content inside the transaction as well as the
		// verified opener before it. No worker output is implicitly imported.
		var count int
		err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM coordinator_artifacts WHERE workflow_run_id=? AND sha256=? AND json_extract(record,'$.storagePath')=? AND json_extract(record,'$.kind')='input'", source.Run.ID, a.SHA256, a.StoragePath).Scan(&count)
		if err != nil {
			return result, err
		}
		if count == 0 {
			return result, errors.New("clone source input disappeared")
		}
		if err = insertImmutableJSON(ctx, tx, "clone input", a.ID, "INSERT INTO coordinator_artifacts(id,workflow_run_id,task_id,attempt_id,sha256,record) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING", []any{a.ID, a.WorkflowRunID, a.TaskID, a.AttemptID, a.SHA256}, "SELECT record FROM coordinator_artifacts WHERE id=?", []any{a.ID}, a); err != nil {
			return result, err
		}
		inputByID[a.ID] = a
	}
	for _, task := range c.Tasks {
		for _, id := range append([]string{task.PromptArtifactID}, task.InputArtifactIDs...) {
			a, ok := inputByID[id]
			if !ok || (a.TaskID != "" && a.TaskID != task.ID) {
				return result, errors.New("clone input ownership mismatch")
			}
		}
		a := domain.Attempt{ID: "attempt:" + task.ID + ":1", WorkflowRunID: run.ID, TaskID: task.ID, Number: 1, Revision: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, UpdatedAt: c.Now.UTC()}
		if err = upsertJSON(ctx, tx, "clone attempt", a.ID, "INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)", []any{a.ID, run.ID, task.ID, 1, 1}, a); err != nil {
			return result, err
		}
	}
	if err = upsertJSON(ctx, tx, "clone run", run.ID, "INSERT INTO coordinator_workflow_runs(id,workflow_id,schedule_id,progress,revision,record) VALUES(?,?,?,?,?,?)", []any{run.ID, run.WorkflowID, "", run.Progress, run.Revision}, run); err != nil {
		return result, err
	}
	if err = validateRunGraphEdgesTx(ctx, tx); err != nil {
		return result, err
	}
	if err = insertGraphTx(ctx, tx, graph); err != nil {
		return result, err
	}
	if source.Run.Sink != nil {
		if err = pinNodeTx(ctx, tx, "clone:"+run.ID, domain.NodeRef{RunID: source.Run.ID, TaskID: source.Run.Sink.ID}); err != nil {
			return result, err
		}
	}
	if _, err = insertNativeAuditEventTx(ctx, tx, nativeAuditInput{ID: "graph-cloned:" + c.Request.ID, Kind: "graph-cloned", WorkflowRunID: run.ID, TargetType: domain.AdminTargetWorkflowRun, TargetID: run.ID, Actor: c.Actor, Reason: c.Request.Reason, CreatedAt: c.Now.UTC(), Detail: nativeAuditDetail{ExpectedRevision: c.Request.ExpectedRevision, Revision: 1, IdempotencyIdentity: c.Request.ID, Outcome: graph.Digest}}); err != nil {
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
