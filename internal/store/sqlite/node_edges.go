package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// bindNodeEdgesTx is called after new definitions are inserted but before commit.
// Resolve aliases once, pin retained identities and validate the combined graph.
func bindNodeEdgesTx(ctx context.Context, tx *sql.Tx) error {
	records, err := nodeRecordsTx(ctx, tx)
	if err != nil {
		return err
	}
	for i := range records.Tasks {
		task := &records.Tasks[i]
		if len(task.ExternalNeeds) == 0 {
			continue
		}
		for j, ref := range task.ExternalNeeds {
			obs, err := resolveNodeRecords(ref, records)
			if err != nil {
				return err
			}
			task.ExternalNeeds[j] = obs.Target
		}
		raw, err := json.Marshal(task)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE coordinator_tasks SET record=? WHERE id=?", raw, task.ID); err != nil {
			return err
		}
	}
	return validateRunGraphEdgesTx(ctx, tx)
}

func validateRunGraphEdgesTx(ctx context.Context, tx *sql.Tx) error {
	records, err := nodeRecordsTx(ctx, tx)
	if err != nil {
		return err
	}
	hasGraph := false
	for _, run := range records.WorkflowRuns {
		if run.Graph != nil {
			hasGraph = true
		}
	}
	for _, task := range records.Tasks {
		if len(task.ExternalNeeds) > 0 {
			hasGraph = true
		}
	}
	if !hasGraph {
		return nil
	}
	edges := map[string][]string{}
	for _, run := range records.WorkflowRuns {
		names := map[string]string{}
		for _, task := range domain.TasksForRun(run, records.Tasks) {
			if task.WorkflowID == run.WorkflowID {
				names[task.Name] = task.ID
			}
		}
		for _, task := range domain.TasksForRun(run, records.Tasks) {
			if task.WorkflowID != run.WorkflowID {
				continue
			}
			key := (domain.NodeRef{RunID: run.ID, TaskID: task.ID}).String()
			for _, name := range task.Needs {
				id := names[name]
				if id == "" {
					return fmt.Errorf("missing local node %s", name)
				}
				edges[key] = append(edges[key], (domain.NodeRef{RunID: run.ID, TaskID: id}).String())
			}
			for _, ref := range task.ExternalNeeds {
				obs, err := resolveNodeRecords(ref, records)
				if err != nil {
					return err
				}
				if obs.Target != ref {
					return fmt.Errorf("external edge must use canonical identity: %s", ref.String())
				}
				edges[key] = append(edges[key], ref.String())
				if err := pinNodeTx(ctx, tx, "edge:"+run.ID, ref); err != nil {
					return err
				}
			}
			if run.Sink != nil {
				sink := (domain.NodeRef{RunID: run.ID, TaskID: run.Sink.ID}).String()
				edges[sink] = append(edges[sink], key)
			}
		}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(key string) error {
		if visiting[key] {
			return fmt.Errorf("cross-run dependency cycle at %s", key)
		}
		if done[key] {
			return nil
		}
		visiting[key] = true
		for _, to := range edges[key] {
			if err := visit(to); err != nil {
				return err
			}
		}
		delete(visiting, key)
		done[key] = true
		return nil
	}
	for key := range edges {
		if err := visit(key); err != nil {
			return err
		}
	}
	return nil
}

func nodeDependenciesTx(ctx context.Context, tx *sql.Tx, tasks []domain.Task) ([]domain.NodeObservation, error) {
	var refs []domain.NodeRef
	for _, task := range tasks {
		refs = append(refs, task.ExternalNeeds...)
	}
	if len(refs) == 0 {
		return nil, nil
	}
	records, err := nodeRecordsTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	observations := make([]domain.NodeObservation, 0, len(refs))
	for _, ref := range refs {
		obs, err := resolveNodeRecords(ref, records)
		if err != nil {
			return nil, err
		}
		observations = append(observations, obs)
	}
	return observations, nil
}
func requireExternalSuccessTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt) error {
	taskID := attempt.TaskID
	var runRaw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id=?", attempt.WorkflowRunID).Scan(&runRaw); err == nil {
		var run domain.WorkflowRun
		if err = json.Unmarshal(runRaw, &run); err != nil {
			return err
		}
		if run.Graph != nil {
			for _, task := range run.Graph.Tasks {
				if task.ID == taskID {
					return requireTaskExternalSuccessTx(ctx, tx, task)
				}
			}
			return errors.New("task missing from run graph")
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_tasks WHERE id=?", taskID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	var task domain.Task
	if err := json.Unmarshal(raw, &task); err != nil {
		return err
	}
	return requireTaskExternalSuccessTx(ctx, tx, task)
}
func requireTaskExternalSuccessTx(ctx context.Context, tx *sql.Tx, task domain.Task) error {
	observations, err := nodeDependenciesTx(ctx, tx, []domain.Task{task})
	if err != nil {
		return err
	}
	for _, obs := range observations {
		if obs.ExitCode != 0 {
			return fmt.Errorf("external dependency %s is %s", obs.Target.String(), obs.Reason)
		}
	}
	return nil
}
