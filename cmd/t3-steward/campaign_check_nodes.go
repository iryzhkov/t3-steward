package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// addUnknownNodeReasons looks up every cross-run need of the checked tasks and
// marks a task impossible when its need names a run or task the coordinator
// does not hold. The viability query never sees cross-run needs, so without
// this a campaign that submission would refuse at binding was reported ready.
// The lookup is the ordinary workflow query, which every coordinator answers,
// rather than a new viability field an older coordinator would refuse to
// decode. A node that exists and has not finished is not reported: waiting
// for it is what the dependency is for.
func (c campaignCLI) addUnknownNodeReasons(ctx context.Context, plan campaign.Plan, matrix *backlogadmin.ViabilityMatrix) error {
	if c.detail == nil {
		return nil
	}
	checked := make(map[string]int, len(matrix.Tasks))
	for i, result := range matrix.Tasks {
		checked[result.Task] = i
	}
	runs := map[string]*backlogadmin.WorkflowDetail{}
	changed := false
	for _, task := range plan.Tasks {
		index, ok := checked[task.Name]
		if !ok {
			continue
		}
		for _, need := range task.ExternalNeeds {
			ref, err := domain.ParseNodeRef(need)
			if err != nil {
				return fmt.Errorf("campaign check: task %s: %w", task.Name, err)
			}
			detail, seen := runs[ref.RunID]
			if !seen {
				found, err := c.detail(ctx, ref.RunID)
				switch {
				case err == nil:
					detail = &found
				case errors.Is(err, backlogadmin.ErrNotFound) || strings.Contains(err.Error(), backlogadmin.ErrNotFound.Error()):
					detail = nil
				default:
					return fmt.Errorf("campaign check: task %s: look up cross-run dependency %q: %w", task.Name, need, err)
				}
				runs[ref.RunID] = detail
			}
			var problem string
			switch {
			case detail == nil:
				problem = fmt.Sprintf("run %q is unknown to the coordinator", ref.RunID)
			case !workflowDetailHasNode(*detail, ref.TaskID):
				problem = fmt.Sprintf("task %q is unknown in run %q", ref.TaskID, ref.RunID)
			default:
				continue
			}
			result := &matrix.Tasks[index]
			result.Outcome = backlogadmin.ViabilityImpossible
			result.Reasons = append(result.Reasons, backlogadmin.ViabilityReason{
				Code: backlogadmin.ReasonUnknownNode, Permanent: backlogadmin.PermanentViabilityReason(backlogadmin.ReasonUnknownNode),
				Detail: fmt.Sprintf("task %s needs %q: %s", task.Name, need, problem),
			})
			changed = true
		}
	}
	if changed {
		matrix.Outcome = backlogadmin.MatrixOutcome(*matrix)
	}
	return nil
}

// workflowDetailHasNode reports whether name is a task of the run, by ID or by
// name, or its sink, the same nodes domain.ResolveNode accepts at binding.
func workflowDetailHasNode(detail backlogadmin.WorkflowDetail, name string) bool {
	if sink := detail.Summary.Run.Sink; sink != nil && (name == domain.SinkTaskName || name == sink.ID) {
		return true
	}
	for _, task := range detail.Tasks {
		if task.Task.ID == name || task.Task.Name == name {
			return true
		}
	}
	return false
}
