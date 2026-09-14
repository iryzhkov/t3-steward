package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// runSeed is everything a brand-new workflow run needs before anything will
// execute it.
//
// A workflow run row on its own is inert. The planner works from attempts, the
// execution package builder resolves every prompt and input artifact inside the
// custody of the run that owns it, and the dispatch fence compares a graph
// revision that has to exist. Three writes are what turn the row into work: one
// first attempt per task, the run's input artifacts rebound to it, and the
// immutable graph revision that pins the definition those attempts belong to.
//
// The graph clone and the schedule trigger both create runs, and both go
// through seedExecutableRunTx rather than each writing its own version. A
// second copy is how the two paths drift until one of them silently produces
// runs that can never be planned.
type runSeed struct {
	// Run is the new run, with its sink already bound and its Graph pointing at
	// the same definition as Graph below.
	Run domain.WorkflowRun
	// Tasks are the run-local task definitions the graph revision pins. They
	// already carry this run's own task identities and artifact identities.
	Tasks []domain.Task
	// Inputs are the input artifacts rebound to Run, deduplicated by identity.
	Inputs []domain.Artifact
	// Graph is the first immutable revision of the run's definition.
	Graph domain.GraphDefinition
	// ScheduleID names the schedule that owns the run, and is empty for every
	// run no schedule owns. It is stored on the run row because that column is
	// how a schedule finds its own active run.
	ScheduleID string
	Now        time.Time
}

// seedExecutableRunTx writes one seed inside the caller's transaction, in the
// order that keeps rollback total: artifacts first, then attempts, then the run
// row, then edge validation, then the graph revision. A run is therefore either
// completely executable or not present at all; there is no state in which a row
// exists that nothing can ever plan.
//
// kind names the creating operation and appears in the error messages, so a
// failure says which caller produced the seed it refused.
func seedExecutableRunTx(ctx context.Context, tx *sql.Tx, kind string, seed runSeed) error {
	inputByID := make(map[string]domain.Artifact, len(seed.Inputs))
	for _, input := range seed.Inputs {
		if input.WorkflowRunID != seed.Run.ID || input.Kind != domain.ArtifactInput {
			return fmt.Errorf("invalid %s input %q", kind, input.ID)
		}
		if err := insertImmutableJSON(ctx, tx, kind+" input", input.ID,
			"INSERT INTO coordinator_artifacts(id,workflow_run_id,task_id,attempt_id,sha256,record) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
			[]any{input.ID, input.WorkflowRunID, input.TaskID, input.AttemptID, input.SHA256},
			"SELECT record FROM coordinator_artifacts WHERE id=?", []any{input.ID}, input); err != nil {
			return err
		}
		inputByID[input.ID] = input
	}
	for _, task := range seed.Tasks {
		for _, id := range append([]string{task.PromptArtifactID}, task.InputArtifactIDs...) {
			artifact, ok := inputByID[id]
			if !ok || (artifact.TaskID != "" && artifact.TaskID != task.ID) {
				return fmt.Errorf("%s input ownership mismatch", kind)
			}
		}
		attempt := domain.Attempt{
			ID: "attempt:" + task.ID + ":1", WorkflowRunID: seed.Run.ID, TaskID: task.ID,
			Number: 1, Revision: 1, Progress: domain.ProgressBlocked,
			Control: domain.ControlUnassigned, UpdatedAt: seed.Now.UTC(),
		}
		if err := upsertJSON(ctx, tx, kind+" attempt", attempt.ID,
			"INSERT INTO coordinator_attempts(id,workflow_run_id,task_id,number,revision,record) VALUES(?,?,?,?,?,?)",
			[]any{attempt.ID, seed.Run.ID, task.ID, 1, 1}, attempt); err != nil {
			return err
		}
	}
	if err := upsertJSON(ctx, tx, kind+" run", seed.Run.ID,
		"INSERT INTO coordinator_workflow_runs(id,workflow_id,schedule_id,progress,revision,record) VALUES(?,?,?,?,?,?)",
		[]any{seed.Run.ID, seed.Run.WorkflowID, seed.ScheduleID, seed.Run.Progress, seed.Run.Revision}, seed.Run); err != nil {
		return err
	}
	if err := validateRunGraphEdgesTx(ctx, tx); err != nil {
		return err
	}
	return insertGraphTx(ctx, tx, seed.Graph)
}

// scheduledRunSeed turns a schedule's workflow definition into a seed for one
// occurrence.
//
// The workflow's task templates and the prompt and input artifacts submitted
// with them are the source, and the new run gets its own copies of both. The
// copies are not decoration. Task identities have to be run-local because the
// attempts, the graph revision and the sink are all keyed by them, and the
// artifacts have to be rebound because the execution package builder refuses an
// artifact that belongs to another run, and because artifact retention prunes
// per run: a scheduled run still pointing at the submitting run's artifacts
// would lose its own prompts the first time that run was pruned.
//
// Only the metadata rows are copied. The retained content is immutable and
// addressed by storage path and SHA-256, so every copy names the same bytes.
func scheduledRunSeed(ctx context.Context, tx *sql.Tx, run domain.WorkflowRun, now time.Time) (runSeed, error) {
	templates, err := loadWorkflowTasksTx(ctx, tx, run.WorkflowID)
	if err != nil {
		return runSeed{}, err
	}
	templates = domain.TasksForRun(run, templates)
	// A workflow that declares tasks it no longer has cannot produce a run that
	// will execute them, and a schedule that fired on one has not fired at all.
	// Saying so here is the difference between a loud refusal and a run row that
	// nothing will ever plan.
	if err := requireDeclaredWorkflowTasksTx(ctx, tx, run.WorkflowID, templates); err != nil {
		return runSeed{}, err
	}
	raw, err := json.Marshal(templates)
	if err != nil {
		return runSeed{}, fmt.Errorf("encode scheduled workflow %q tasks: %w", run.WorkflowID, err)
	}
	var tasks []domain.Task
	if err := json.Unmarshal(raw, &tasks); err != nil {
		return runSeed{}, fmt.Errorf("decode scheduled workflow %q tasks: %w", run.WorkflowID, err)
	}

	taskIDs := make(map[string]string, len(tasks))
	for index, task := range tasks {
		taskIDs[task.ID] = fmt.Sprintf("task:scheduled:%s:%d", run.ID, index)
	}
	artifactIDs := make(map[string]string, len(tasks))
	var inputs []domain.Artifact
	// rebind copies one submitted input artifact into the new run's custody,
	// once, and answers with the copy's identity.
	rebind := func(sourceID string) (string, error) {
		if copied, ok := artifactIDs[sourceID]; ok {
			return copied, nil
		}
		source, found, err := loadArtifactTx(ctx, tx, sourceID)
		if err != nil {
			return "", err
		}
		if !found {
			return "", fmt.Errorf("scheduled run input %q is no longer retained", sourceID)
		}
		if source.Kind != domain.ArtifactInput {
			return "", fmt.Errorf("scheduled run input %q is not a submitted input artifact", sourceID)
		}
		copied := source
		copied.ID = fmt.Sprintf("input:scheduled:%s:%d", run.ID, len(inputs))
		copied.WorkflowRunID = run.ID
		copied.AttemptID = ""
		if copied.TaskID != "" {
			mapped, ok := taskIDs[copied.TaskID]
			if !ok {
				return "", fmt.Errorf("scheduled run input %q names a task outside its workflow", sourceID)
			}
			copied.TaskID = mapped
		}
		// Producer and StoragePath are preserved so the retained submission
		// object keeps resolving under the same artifact root.
		copied.CreatedAt = now.UTC()
		inputs = append(inputs, copied)
		artifactIDs[sourceID] = copied.ID
		return copied.ID, nil
	}
	for index := range tasks {
		task := &tasks[index]
		task.ID = taskIDs[task.ID]
		task.RunID = run.ID
		task.DefinitionRevision = 1
		if task.PromptArtifactID, err = rebind(task.PromptArtifactID); err != nil {
			return runSeed{}, err
		}
		for position, id := range task.InputArtifactIDs {
			if task.InputArtifactIDs[position], err = rebind(id); err != nil {
				return runSeed{}, err
			}
		}
	}

	run.GraphRevision = 1
	// The occurrence is deliberately not re-validated against
	// domain.ValidateGraphTasks, even though the graph clone does validate.
	//
	// That check enforces rules a submission is allowed to fail. A version-2
	// manifest with no provider route at all is accepted at ingest —
	// validateRoutes in internal/backlog/manifest.go iterates the routes it was
	// given and returns nil on an empty list, and the task-level default falls
	// back to the workflow's routes, which may also be empty — yet
	// ValidateGraphTasks refuses a task with no route. Applying it here would
	// therefore make an already-accepted workflow permanently unschedulable, and
	// would do it at fire time rather than at submission where an operator could
	// act on it. The planner already reports such a run as having no eligible
	// worker, which is the honest place for it.
	//
	// BindRunSink below still refuses a definition whose identities are
	// unusable, which is what stops an unexecutable run from being created.
	graph := domain.GraphDefinition{
		RunID: run.ID, Revision: 1, Actor: "schedule", Reason: "scheduled occurrence",
		RequestID: "schedule:" + run.ID, CreatedAt: now.UTC(), Tasks: tasks,
		Digest: domain.GraphDigest(tasks),
	}
	run.Graph = &graph
	if run, err = domain.BindRunSink(run, tasks); err != nil {
		return runSeed{}, err
	}
	return runSeed{
		Run: run, Tasks: tasks, Inputs: inputs, Graph: graph,
		ScheduleID: run.ScheduleID, Now: now.UTC(),
	}, nil
}

// requireDeclaredWorkflowTasksTx refuses a workflow whose record is missing, or
// whose record declares a task identity that its stored definitions no longer
// contain. Those are the cases where a seeded run would silently have fewer
// tasks than the workflow promised.
//
// A workflow that declares no tasks at all is accepted and remains schedulable.
// It produces a run that is a sink with nothing in front of it, which is what a
// scheduled occurrence of such a workflow already was before runs were seeded,
// and refusing it here would turn a legal if pointless definition into a
// schedule that can never fire.
func requireDeclaredWorkflowTasksTx(ctx context.Context, tx *sql.Tx, workflowID string, templates []domain.Task) error {
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflows WHERE id=?", workflowID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("scheduled workflow %q is not defined", workflowID)
	}
	if err != nil {
		return fmt.Errorf("load scheduled workflow %q: %w", workflowID, err)
	}
	var workflow domain.Workflow
	if err := json.Unmarshal(raw, &workflow); err != nil {
		return fmt.Errorf("decode scheduled workflow %q: %w", workflowID, err)
	}
	available := make(map[string]bool, len(templates))
	for _, task := range templates {
		available[task.ID] = true
	}
	for _, id := range workflow.TaskIDs {
		if !available[id] {
			return fmt.Errorf("scheduled workflow %q declares task %q, which has no stored definition", workflowID, id)
		}
	}
	return nil
}
