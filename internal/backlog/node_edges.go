package backlog

import "github.com/iryzhkov/t3-steward/internal/domain"

func ResolveExternalNodes(tasks []domain.Task, runs []domain.WorkflowRun, allTasks []domain.Task, attempts []domain.Attempt, assignments []domain.Assignment) map[string]domain.NodeObservation {
	result := map[string]domain.NodeObservation{}
	for _, task := range tasks {
		for _, ref := range task.ExternalNeeds {
			obs, err := domain.ResolveNode(ref, runs, allTasks, attempts, assignments)
			if err != nil {
				obs = domain.NodeObservation{Target: ref, ExitCode: 2, Reason: err.Error()}
			}
			result[ref.String()] = obs
		}
	}
	return result
}
