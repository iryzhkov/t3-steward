package domain

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// rerunGraph is a diamond: root, two middles that both need it, and a join that
// needs both. It is the smallest shape where "descendant" and "ancestor" are
// not the same as "before" and "after" in the task list.
func rerunGraph(progress map[string]ProgressState) (WorkflowRun, []Task, []Attempt) {
	needs := map[string][]string{
		"left": {"root"}, "right": {"root"}, "join": {"left", "right"},
	}
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	var tasks []Task
	var attempts []Attempt
	for _, name := range []string{"join", "left", "right", "root"} {
		tasks = append(tasks, Task{
			ID: "task-" + name, WorkflowID: "workflow", Name: name,
			Class: TaskClassSurplus, MaxTurns: 1, PromptArtifactID: "prompt",
			Needs: needs[name], Routes: []ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}},
		})
		attempts = append(attempts, Attempt{
			ID: "attempt-" + name, WorkflowRunID: "run", TaskID: "task-" + name,
			Number: 1, Progress: progress[name], UpdatedAt: now,
		})
	}
	run := WorkflowRun{ID: "run", WorkflowID: "workflow", Progress: ProgressFailed, GraphRevision: 1}
	return run, tasks, attempts
}

func TestPlanRerunSelectsTheNamedTaskAndItsDescendants(t *testing.T) {
	run, tasks, attempts := rerunGraph(map[string]ProgressState{
		"root": ProgressSucceeded, "left": ProgressFailed,
		"right": ProgressSucceeded, "join": ProgressSkipped,
	})
	scope, err := PlanRerun(run, tasks, attempts, "left")
	if err != nil {
		t.Fatal(err)
	}
	if scope.From.Name != "left" || scope.FromAttempt.ID != "attempt-left" {
		t.Fatalf("scope start = %+v", scope.From)
	}
	rerun := append([]string(nil), scope.RerunNames()...)
	reuse := append([]string(nil), scope.ReuseNames()...)
	if !reflect.DeepEqual(rerun, []string{"join", "left"}) {
		t.Fatalf("rerun set = %v, want the named task and its descendant", rerun)
	}
	if !reflect.DeepEqual(reuse, []string{"right", "root"}) {
		t.Fatalf("reuse set = %v, want the tasks that succeeded", reuse)
	}
}

// The named task is resolved by ID as well as by manifest name, because an
// agent reading a graph has IDs and a human reading a manifest has names.
func TestPlanRerunResolvesTheTaskByIdentityOrName(t *testing.T) {
	run, tasks, attempts := rerunGraph(map[string]ProgressState{
		"root": ProgressSucceeded, "left": ProgressSucceeded,
		"right": ProgressSucceeded, "join": ProgressFailed,
	})
	byName, err := PlanRerun(run, tasks, attempts, "join")
	if err != nil {
		t.Fatal(err)
	}
	byID, err := PlanRerun(run, tasks, attempts, "task-join")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(byName.RerunNames(), byID.RerunNames()) {
		t.Fatalf("name and id disagree: %v vs %v", byName.RerunNames(), byID.RerunNames())
	}
	if _, err = PlanRerun(run, tasks, attempts, "absent"); err == nil {
		t.Fatal("an unknown task was accepted")
	}
}

func TestPlanRerunRefusesToReuseATaskThatDidNotSucceed(t *testing.T) {
	run, tasks, attempts := rerunGraph(map[string]ProgressState{
		"root": ProgressSucceeded, "left": ProgressFailed,
		"right": ProgressFailed, "join": ProgressSkipped,
	})
	// Rerunning from left would reuse right, which failed on its own branch.
	_, err := PlanRerun(run, tasks, attempts, "left")
	if err == nil {
		t.Fatal("a failed sibling was reused")
	}
	if !strings.Contains(err.Error(), "right is failed") {
		t.Fatalf("the refusal does not name the task or its state: %v", err)
	}
	// Rerunning from the task they both descend from is the recovery the
	// message points at, and it is accepted.
	scope, err := PlanRerun(run, tasks, attempts, "root")
	if err != nil {
		t.Fatal(err)
	}
	if len(scope.Reuse) != 0 || len(scope.Rerun) != 4 {
		t.Fatalf("rerunning from the root left %d tasks reused", len(scope.Reuse))
	}
}

func TestPlanRerunRefusesALiveSourceRun(t *testing.T) {
	run, tasks, attempts := rerunGraph(map[string]ProgressState{
		"root": ProgressSucceeded, "left": ProgressActive,
		"right": ProgressSucceeded, "join": ProgressBlocked,
	})
	run.Progress = ProgressActive
	if _, err := PlanRerun(run, tasks, attempts, "left"); !errors.Is(err, ErrRerunSourceLive) {
		t.Fatalf("err = %v, want the live-source refusal", err)
	}
}

// A task that was never attempted is reported as such rather than as a state,
// because "never attempted" and "failed" call for different next steps.
func TestPlanRerunNamesATaskThatWasNeverAttempted(t *testing.T) {
	run, tasks, attempts := rerunGraph(map[string]ProgressState{
		"root": ProgressSucceeded, "left": ProgressFailed,
		"right": ProgressSucceeded, "join": ProgressSkipped,
	})
	var kept []Attempt
	for _, attempt := range attempts {
		if attempt.TaskID != "task-right" {
			kept = append(kept, attempt)
		}
	}
	_, err := PlanRerun(run, tasks, kept, "left")
	if err == nil || !strings.Contains(err.Error(), "right is never attempted") {
		t.Fatalf("err = %v", err)
	}
}

// The last attempt decides, so a task that failed and was retried into success
// is reusable.
func TestPlanRerunReadsTheLastAttemptOfEachTask(t *testing.T) {
	run, tasks, attempts := rerunGraph(map[string]ProgressState{
		"root": ProgressFailed, "left": ProgressFailed,
		"right": ProgressSucceeded, "join": ProgressSkipped,
	})
	attempts = append(attempts, Attempt{
		ID: "attempt-root-2", WorkflowRunID: "run", TaskID: "task-root",
		Number: 2, Progress: ProgressSucceeded,
	})
	scope, err := PlanRerun(run, tasks, attempts, "left")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scope.ReuseNames(), []string{"right", "root"}) {
		t.Fatalf("reuse set = %v", scope.ReuseNames())
	}
}

func TestValidateGraphAmendmentRerunNeedsATaskAndNothingElse(t *testing.T) {
	base := GraphAmendment{ID: "key", RunID: "run", ExpectedRevision: 1, Reason: "why", Operation: "rerun"}
	if err := ValidateGraphAmendment(base); err == nil {
		t.Fatal("a rerun without a task was accepted")
	}
	base.TaskID = "implement"
	if err := ValidateGraphAmendment(base); err != nil {
		t.Fatal(err)
	}
	mixed := base
	model := "other"
	mixed.Model = &model
	if err := ValidateGraphAmendment(mixed); err == nil {
		t.Fatal("a rerun that also edits a definition was accepted")
	}
}

// A carried input has no edge on purpose. It must still be complete, and it
// must not name a task of the graph it lives in, which would mean the same file
// could be claimed from two places.
func TestValidateGraphTasksChecksCarriedInputs(t *testing.T) {
	run := WorkflowRun{ID: "run", WorkflowID: "workflow"}
	task := Task{
		ID: "task-a", WorkflowID: "workflow", Name: "a", Class: TaskClassSurplus,
		MaxTurns: 1, PromptArtifactID: "prompt",
		Routes:        []ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}},
		CarriedInputs: []CarriedInput{{Producer: "inspect", ProducerTaskID: "inspect", Name: "findings.md", ArtifactID: "input-1"}},
	}
	if err := ValidateGraphTasks(run, []Task{task}); err != nil {
		t.Fatal(err)
	}
	incomplete := task
	incomplete.CarriedInputs = []CarriedInput{{Producer: "inspect", Name: "findings.md"}}
	if err := ValidateGraphTasks(run, []Task{incomplete}); err == nil {
		t.Fatal("an incomplete carried input was accepted")
	}
	local := task
	local.CarriedInputs[0].Producer = "a"
	if err := ValidateGraphTasks(run, []Task{local}); err == nil {
		t.Fatal("a carried input named a task of this graph")
	}
}
