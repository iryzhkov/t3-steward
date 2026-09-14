package backlog

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// scheduledRunShape is the part of a seeded scheduled run that a replay must
// not change. It is compared as a value so that a second delivery of the same
// occurrence cannot quietly add an attempt or rebind an artifact again.
type scheduledRunShape struct {
	RunID          string
	ScheduleID     string
	GraphRevision  int64
	GraphDigest    string
	AttemptIDs     []string
	ArtifactIDs    []string
	ArtifactDigest []string
}

type scheduledRunStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	LoadGraphRevisions(context.Context, string) ([]domain.GraphDefinition, error)
}

// assertScheduledRunIsExecutable checks everything that has to be true before a
// scheduled run is work rather than a row: it keeps the schedule that owns it,
// it has one attempt per task, every prompt and input artifact its tasks name
// is bound to this run and names the same retained content as the submission it
// came from, it has a graph revision to dispatch against, and the coordinator
// planner takes it with its dependency order intact.
func assertScheduledRunIsExecutable(
	ctx context.Context,
	t *testing.T,
	store scheduledRunStore,
	schedule domain.Schedule,
	result domain.ScheduleTriggerResult,
	submittedRunID string,
) scheduledRunShape {
	t.Helper()
	run := result.WorkflowRun
	if run == nil {
		t.Fatal("accepted trigger produced no workflow run")
	}
	// A clone deliberately blanks schedule_id; a scheduled run must not, because
	// that column is how the schedule finds its own active run and refuses the
	// next occurrence.
	if run.ScheduleID != schedule.ID {
		t.Fatalf("scheduled run schedule = %q, want %q", run.ScheduleID, schedule.ID)
	}
	if run.TriggerID != result.Trigger.ID {
		t.Fatalf("scheduled run trigger = %q, want %q", run.TriggerID, result.Trigger.ID)
	}
	if run.Graph == nil || run.GraphRevision != 1 || run.Sink == nil {
		t.Fatalf("scheduled run graph = %#v, sink = %#v", run.Graph, run.Sink)
	}

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tasks := domain.TasksForRun(*run, records.Tasks)
	if len(tasks) != len(run.Graph.Tasks) || len(tasks) == 0 {
		t.Fatalf("scheduled run tasks = %#v", tasks)
	}

	artifactByID := map[string]domain.Artifact{}
	for _, artifact := range records.Artifacts {
		artifactByID[artifact.ID] = artifact
	}
	submittedContent := map[string]bool{}
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID == submittedRunID && artifact.Kind == domain.ArtifactInput {
			submittedContent[artifact.SHA256+"\x00"+artifact.StoragePath] = true
		}
	}

	shape := scheduledRunShape{
		RunID: run.ID, ScheduleID: run.ScheduleID,
		GraphRevision: run.GraphRevision, GraphDigest: run.Graph.Digest,
	}
	seenArtifact := map[string]bool{}
	for _, task := range tasks {
		if task.RunID != run.ID {
			t.Fatalf("scheduled task %q is not run-local: %#v", task.ID, task)
		}
		for _, id := range append([]string{task.PromptArtifactID}, task.InputArtifactIDs...) {
			artifact, ok := artifactByID[id]
			if !ok {
				t.Fatalf("task %q names artifact %q, which is not retained", task.Name, id)
			}
			if artifact.WorkflowRunID != run.ID {
				t.Fatalf("artifact %q is bound to run %q, want %q", id, artifact.WorkflowRunID, run.ID)
			}
			if !submittedContent[artifact.SHA256+"\x00"+artifact.StoragePath] {
				t.Fatalf("rebound artifact %q does not name submitted content: %#v", id, artifact)
			}
			if !seenArtifact[id] {
				seenArtifact[id] = true
				shape.ArtifactIDs = append(shape.ArtifactIDs, id)
				shape.ArtifactDigest = append(shape.ArtifactDigest, artifact.SHA256)
			}
		}
	}

	var attempts []domain.Attempt
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID == run.ID {
			attempts = append(attempts, attempt)
			shape.AttemptIDs = append(shape.AttemptIDs, attempt.ID)
		}
	}
	if len(attempts) != len(tasks) {
		t.Fatalf("scheduled run has %d attempts for %d tasks", len(attempts), len(tasks))
	}
	sort.Strings(shape.AttemptIDs)
	sort.Strings(shape.ArtifactIDs)
	sort.Strings(shape.ArtifactDigest)

	graphs, err := store.LoadGraphRevisions(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(graphs) != 1 || graphs[0].Revision != 1 || graphs[0].Digest != run.Graph.Digest {
		t.Fatalf("scheduled run graph revisions = %#v", graphs)
	}

	plan, err := BuildCoordinatorPlanInput(CoordinatorPlanningStateInput{
		Now: run.CreatedAt.Add(time.Minute), CoordinatorEpoch: 1,
		Workflows: records.Workflows, WorkflowRuns: records.WorkflowRuns,
		Tasks: records.Tasks, Attempts: records.Attempts, Assignments: records.Assignments,
		QuotaPools: records.QuotaPools, DisableQuotaChecks: true,
		MaxWorkerSnapshotAge: time.Minute, MaxQuotaObservationAge: time.Minute,
		DeadlineRiskWindow: time.Hour,
	})
	if err != nil {
		t.Fatalf("build planner input: %v", err)
	}
	planned, ok := plannedWorkflow(plan, run.ID)
	if !ok {
		t.Fatalf("planner excluded scheduled run %q", run.ID)
	}
	if len(planned.State.Attempts) != len(tasks) {
		t.Fatalf("planner sees %d attempts for scheduled run", len(planned.State.Attempts))
	}
	ready := 0
	for _, attempt := range planned.State.Attempts {
		switch attempt.Progress {
		case domain.ProgressReady:
			ready++
		case domain.ProgressBlocked:
		default:
			t.Fatalf("planner sees scheduled attempt %q as %s", attempt.ID, attempt.Progress)
		}
	}
	if ready == 0 {
		t.Fatalf("planner found nothing ready in scheduled run %q", run.ID)
	}
	return shape
}

func plannedWorkflow(plan PlanInput, runID string) (PlanningWorkflow, bool) {
	for _, item := range plan.Workflows {
		if item.State.Run.ID == runID {
			return item, true
		}
	}
	return PlanningWorkflow{}, false
}
