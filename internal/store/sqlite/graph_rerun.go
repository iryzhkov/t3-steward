package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV17 = `
CREATE TRIGGER IF NOT EXISTS release_rerun_source AFTER DELETE ON coordinator_workflow_runs BEGIN DELETE FROM coordinator_retention_pins WHERE owner='rerun:'||OLD.id; END;
`

// CommitGraphRerun publishes the new run a rerun creates, in one transaction
// that reads the source run and writes nothing to it.
//
// The source run keeps its progress, its graph revision, its attempts and its
// artifacts. That is the whole point of the operation: the failure stays
// readable, and the new run says in its own definition what it is a second
// attempt at.
func (s *Store) CommitGraphRerun(ctx context.Context, c GraphCommit) (domain.GraphAmendmentResult, error) {
	var result domain.GraphAmendmentResult
	if err := domain.ValidateGraphAmendment(c.Request); err != nil {
		return result, err
	}
	if c.Request.Operation != "rerun" || c.Actor == "" || c.Now.IsZero() || c.Rerun == nil {
		return result, errors.New("invalid rerun commit")
	}
	if len(c.Tasks) == 0 {
		return result, errors.New("a rerun must create at least one task")
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
		// The same key with the same content returns the same run; the same
		// key with different content is refused, as everywhere else.
		if actor != c.Actor || request != string(encoded) {
			return result, errors.New("rerun request ID replay changed content or actor")
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
	if source.Run.GraphRevision != c.Request.ExpectedRevision || source.Run.Revision != c.Before.Revision {
		return result, ErrStaleGraph
	}
	if !source.Run.Progress.Terminal() {
		return result, domain.ErrRerunSourceLive
	}
	run := domain.WorkflowRun{
		ID: "run:rerun:" + c.Request.ID, WorkflowID: source.Run.WorkflowID,
		GraphRevision: 1, Revision: 1, Progress: domain.ProgressQueued,
		CreatedAt: c.Now.UTC(), UpdatedAt: c.Now.UTC(),
	}
	if err = domain.ValidateGraphTasks(run, c.Tasks); err != nil {
		return result, err
	}
	graph := domain.GraphDefinition{
		RunID: run.ID, Revision: 1, Actor: c.Actor, Reason: c.Request.Reason,
		RequestID: c.Request.ID, CreatedAt: c.Now.UTC(), Tasks: c.Tasks,
		Digest:     domain.GraphDigest(c.Tasks),
		ClonedFrom: &domain.GraphReference{RunID: source.Run.ID, Revision: source.Run.GraphRevision},
		RerunOf:    c.Rerun,
	}
	run.Graph = &graph
	if run, err = domain.BindRunSink(run, c.Tasks); err != nil {
		return result, err
	}
	inputByID := map[string]domain.Artifact{}
	for _, input := range c.Inputs {
		if input.WorkflowRunID != run.ID || input.Kind != domain.ArtifactInput {
			return result, errors.New("invalid rerun input")
		}
		// Match the retained source content inside the transaction as well as
		// in the verified opener before it. A reference is only a reference if
		// what it points at is still there when the run is created.
		var count int
		err = tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM coordinator_artifacts WHERE workflow_run_id=? AND sha256=? AND json_extract(record,'$.storagePath')=?",
			source.Run.ID, input.SHA256, input.StoragePath).Scan(&count)
		if err != nil {
			return result, err
		}
		if count == 0 {
			return result, fmt.Errorf("rerun source artifact %s disappeared", input.SHA256)
		}
		if err = insertImmutableJSON(ctx, tx, "rerun input", input.ID,
			"INSERT INTO coordinator_artifacts(id,workflow_run_id,task_id,attempt_id,sha256,record) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
			[]any{input.ID, input.WorkflowRunID, input.TaskID, input.AttemptID, input.SHA256},
			"SELECT record FROM coordinator_artifacts WHERE id=?", []any{input.ID}, input); err != nil {
			return result, err
		}
		inputByID[input.ID] = input
	}
	for _, task := range c.Tasks {
		for _, id := range append([]string{task.PromptArtifactID}, task.InputArtifactIDs...) {
			if artifact, ok := inputByID[id]; !ok || (artifact.TaskID != "" && artifact.TaskID != task.ID) {
				return result, errors.New("rerun input ownership mismatch")
			}
		}
		for _, carried := range task.CarriedInputs {
			if artifact, ok := inputByID[carried.ArtifactID]; !ok || artifact.TaskID != task.ID {
				return result, errors.New("carried input ownership mismatch")
			}
		}
		attempt := domain.Attempt{
			ID: "attempt:" + task.ID + ":1", WorkflowRunID: run.ID, TaskID: task.ID,
			Number: 1, Revision: 1, Progress: domain.ProgressBlocked,
			Control: domain.ControlUnassigned, UpdatedAt: c.Now.UTC(),
		}
		if err = upsertJSON(ctx, tx, "rerun attempt", attempt.ID,
			"INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)",
			[]any{attempt.ID, run.ID, task.ID, 1, 1}, attempt); err != nil {
			return result, err
		}
	}
	// The rerun inherits the supervision configuration and the gate definitions
	// remapped onto its own tasks, and inherits no acceptance, hold or incident:
	// the evidence an acceptance was about belongs to the run that failed.
	inherited, err := inheritSupervisionTx(ctx, tx, source.Run, run, c.TaskIDRemap, c.Tasks, c.Now.UTC())
	if err != nil {
		return result, err
	}
	if inherited != nil {
		record := inherited.Record
		run.Supervision = &record
	}
	if err = upsertJSON(ctx, tx, "rerun run", run.ID,
		"INSERT INTO coordinator_workflow_runs(id,workflow_id,schedule_id,progress,revision,record) VALUES(?,?,?,?,?,?)",
		[]any{run.ID, run.WorkflowID, "", run.Progress, run.Revision}, run); err != nil {
		return result, err
	}
	if err = validateRunGraphEdgesTx(ctx, tx); err != nil {
		return result, err
	}
	if err = insertGraphTx(ctx, tx, graph); err != nil {
		return result, err
	}
	if err = saveInheritedSupervisionTx(ctx, tx, inherited); err != nil {
		return result, err
	}
	// Hold the source run against retention for as long as the new run exists:
	// its artifacts are the new run's inputs and its failure is the evidence
	// the new run points at.
	if source.Run.Sink != nil {
		if err = pinNodeTx(ctx, tx, "rerun:"+run.ID, domain.NodeRef{RunID: source.Run.ID, TaskID: source.Run.Sink.ID}); err != nil {
			return result, err
		}
	}
	if err = pinNodeTx(ctx, tx, "rerun:"+run.ID, domain.NodeRef{RunID: source.Run.ID, TaskID: c.Rerun.SourceTaskID}); err != nil {
		return result, err
	}
	detail := nativeAuditDetail{
		ExpectedRevision: c.Request.ExpectedRevision, Revision: 1,
		IdempotencyIdentity: c.Request.ID, Outcome: graph.Digest,
	}
	if _, err = insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: "graph-rerun:" + c.Request.ID, Kind: "graph-rerun", WorkflowRunID: run.ID,
		TargetType: domain.AdminTargetWorkflowRun, TargetID: run.ID, Actor: c.Actor,
		Reason: c.Request.Reason, CreatedAt: c.Now.UTC(), Detail: detail,
	}); err != nil {
		return result, err
	}
	result = domain.GraphAmendmentResult{Run: run, Graph: graph}
	raw, err := json.Marshal(result)
	if err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx,
		"INSERT INTO coordinator_graph_requests(id,actor,request,result) VALUES(?,?,?,?)",
		c.Request.ID, c.Actor, string(encoded), string(raw)); err != nil {
		return result, err
	}
	return result, tx.Commit()
}
