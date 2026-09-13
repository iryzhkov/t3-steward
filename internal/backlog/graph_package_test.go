package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestGraphAmendmentDoesNotChangeOfferedPackage(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	task := records.Tasks[1]
	assignment.GraphRevision = 1
	assignment.TaskRevision = 1
	task.DefinitionRevision = 1
	records.Tasks[1] = task
	assignment.TaskDigest = domain.TaskDigest(task)
	records.Assignments[0] = assignment
	first, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tasks := append([]domain.Task(nil), records.Tasks...)
	tasks = append(tasks, domain.Task{ID: "later", WorkflowID: task.WorkflowID, Name: "later", Needs: []string{task.Name}})
	records.WorkflowRuns[0].GraphRevision = 2
	records.WorkflowRuns[0].Graph = &domain.GraphDefinition{RunID: records.WorkflowRuns[0].ID, Revision: 2, Parent: 1, Tasks: tasks}
	next, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.Package.SHA256 != next.Package.SHA256 || next.Package.Package.GraphRevision != 1 {
		t.Fatal("unrelated amendment changed dispatched package")
	}
	records.WorkflowRuns[0].Graph.Tasks[1].MaxTurns++
	if _, err = packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute)); err == nil {
		t.Fatal("changed assigned definition passed its digest fence")
	}
}
