package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type graphStore interface {
	GraphAmendmentReplay(context.Context, domain.GraphAmendment, string) (domain.GraphAmendmentResult, bool, error)
	CommitGraphAmendment(context.Context, sqlite.GraphCommit) (domain.GraphAmendmentResult, error)
}

func (s *Service) SetGraphAmendmentSupport(inputRoot string, validate func(domain.Workflow, domain.Task) error) {
	s.graphInputRoot, s.graphValidator = inputRoot, validate
}

func (s *Service) AmendGraph(ctx context.Context, p Principal, r domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
	var result domain.GraphAmendmentResult
	if err := s.authorizer.Authorize(ctx, p, Action{Kind: "graph-amendment", WorkflowRunID: r.RunID, TaskID: r.TaskID}); err != nil {
		return result, err
	}
	if err := domain.ValidateGraphAmendment(r); err != nil {
		return result, err
	}
	store, ok := s.reader.(graphStore)
	if !ok {
		return result, errors.New("graph amendment store unavailable")
	}
	if replay, found, err := store.GraphAmendmentReplay(ctx, r, p.ID); found || err != nil {
		return replay, err
	}
	if s.graphValidator == nil {
		return result, errors.New("graph amendment validation is not configured")
	}
	records, err := s.reader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return result, err
	}
	var run domain.WorkflowRun
	for _, candidate := range records.WorkflowRuns {
		if candidate.ID == r.RunID {
			run = candidate
			break
		}
	}
	if run.ID == "" {
		return result, notFound("workflow run", r.RunID)
	}
	if run.GraphRevision != r.ExpectedRevision {
		return result, sqlite.ErrStaleGraph
	}
	var workflow domain.Workflow
	for _, candidate := range records.Workflows {
		if candidate.ID == run.WorkflowID {
			workflow = candidate
			break
		}
	}
	if workflow.ID == "" {
		return result, notFound("workflow", run.WorkflowID)
	}
	if r.Operation == "clone" {
		return s.cloneGraph(ctx, p, r, records, run, workflow)
	}
	canonical := r
	if strings.Contains(r.Source, "/") {
		ref, err := domain.ParseNodeRef(r.Source)
		if err != nil {
			return result, err
		}
		obs, err := domain.ResolveNode(ref, records.WorkflowRuns, records.Tasks, records.Attempts, records.Assignments)
		if err != nil {
			return result, err
		}
		canonical.Source = obs.Target.String()
	}
	now := s.now().UTC()
	taskID, promptID := "task:graph:"+r.ID, "input:graph:"+r.ID
	tasks, err := domain.AmendTasks(canonical, run, records.Tasks, taskID, promptID)
	if err != nil {
		return result, err
	}
	for i := range tasks {
		for j, ref := range tasks[i].ExternalNeeds {
			obs, err := domain.ResolveNode(ref, records.WorkflowRuns, records.Tasks, records.Attempts, records.Assignments)
			if err != nil {
				return result, err
			}
			tasks[i].ExternalNeeds[j] = obs.Target
		}
		if err = s.graphValidator(workflow, tasks[i]); err != nil {
			return result, fmt.Errorf("validate task %s: %w", tasks[i].Name, err)
		}
	}
	var inputs []domain.Artifact
	if r.Operation == "task-add" {
		a, err := backlog.PrepareGraphInput(s.graphInputRoot, promptID, run.ID, taskID, r.Prompt, now)
		if err != nil {
			return result, err
		}
		inputs = append(inputs, a)
	}
	return store.CommitGraphAmendment(ctx, sqlite.GraphCommit{Request: r, Actor: p.ID, Before: run, Tasks: tasks, Inputs: inputs, Now: now})
}

func (c LocalClient) AmendGraph(ctx context.Context, r domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
	var response localResponse
	err := c.call(ctx, localRequest{Version: LocalTransportVersion, Operation: "graph-amendment", GraphAmendment: &r}, &response)
	if err != nil {
		return domain.GraphAmendmentResult{}, err
	}
	if response.GraphAmendment == nil {
		return domain.GraphAmendmentResult{}, errors.New("missing graph amendment response")
	}
	return *response.GraphAmendment, nil
}
