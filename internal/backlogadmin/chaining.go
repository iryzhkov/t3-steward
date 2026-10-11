package backlogadmin

import (
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func (v view) campaignReferenceProblem(value string) string {
	ref, name, err := backlog.ParseCampaignReference(value)
	if err != nil {
		return err.Error()
	}
	observation, err := domain.ResolveNode(ref, v.records.WorkflowRuns, v.records.Tasks, v.records.Attempts, v.records.Assignments)
	if err != nil {
		return fmt.Sprintf("campaign ref %s: %v", value, err)
	}
	if observation.Progress != domain.ProgressSucceeded || observation.AttemptID == "" {
		return fmt.Sprintf("campaign ref %s producer is %s, requires succeeded", value, observation.Progress)
	}
	var producer domain.Task
	for _, run := range v.records.WorkflowRuns {
		if run.ID == ref.RunID {
			for _, task := range domain.TasksForRun(run, v.records.Tasks) {
				if task.ID == observation.Target.TaskID {
					producer = task
				}
			}
		}
	}
	if !backlog.DeclaresCommit(producer, name) {
		return fmt.Sprintf("campaign ref %s names an unknown commit declaration", value)
	}
	count := 0
	for _, artifact := range v.records.Artifacts {
		if artifact.WorkflowRunID == ref.RunID && artifact.TaskID == producer.ID && artifact.AttemptID == observation.AttemptID &&
			artifact.Kind == domain.ArtifactOutput && artifact.Name == name && artifact.SHA256 != "" && artifact.StoragePath != "" {
			count++
		}
	}
	if count != 1 {
		return fmt.Sprintf("campaign ref %s has %d retained successful records; requires one", value, count)
	}
	return ""
}
