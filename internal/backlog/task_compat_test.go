package backlog

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestT3BacklogSubmissionFixtures(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		got, err := ParseWorkflowFile("testdata/t3-backlog-default.md")
		if err != nil {
			t.Fatal(err)
		}
		if got.Source.Project != "t3-steward development" ||
			got.Source.Title != "Continue backlog orchestrator implementation" ||
			got.Source.Importance != 5 || got.Source.Difficulty != 5 ||
			got.Source.MaxTurns != 6 || !got.Source.Gated() {
			t.Fatalf("source = %+v", got.Source)
		}
		if got.Workflow.Version != 1 || got.Workflow.Project != got.Source.Project ||
			got.Workflow.Name != got.Source.Title || got.Workflow.Class != domain.TaskClassSurplus {
			t.Fatalf("workflow = %+v", got.Workflow)
		}
		if got.Run.Progress != domain.ProgressQueued {
			t.Fatalf("run = %+v", got.Run)
		}
		if got.Attempt.Progress != domain.ProgressReady || got.Attempt.Control != domain.ControlUnassigned {
			t.Fatalf("attempt = %+v", got.Attempt)
		}
		if len(got.Workflow.TaskIDs) != 1 || got.Workflow.TaskIDs[0] != got.Task.ID ||
			got.Run.WorkflowID != got.Workflow.ID || got.Attempt.WorkflowRunID != got.Run.ID ||
			got.Attempt.TaskID != got.Task.ID || got.Task.PromptArtifactID == "" {
			t.Fatalf("broken one-task links: %+v", got)
		}
	})

	t.Run("all fields", func(t *testing.T) {
		got, err := ParseWorkflowFile("testdata/t3-backlog-all-fields.md")
		if err != nil {
			t.Fatal(err)
		}
		source := got.Source
		if source.Project != "project: quoted" || source.Title != `Review "scheduler"` ||
			source.Importance != 4 || source.Difficulty != 2 || source.MaxTurns != 7 ||
			source.Host != "normandy" || source.Instance != "codex" || source.Model != "gpt-5.6-sol" ||
			source.Options["effort"] != "high" || source.EstimatedCost == nil || *source.EstimatedCost != 12.5 ||
			source.NotBefore == nil || source.Deadline == nil || source.IsEnabled() || source.Gated() {
			t.Fatalf("source = %+v", source)
		}
		if got.Workflow.Class != domain.TaskClassRequired || got.Run.Progress != domain.ProgressSkipped ||
			got.Attempt.Progress != domain.ProgressSkipped || got.Attempt.Control != domain.ControlStopped ||
			got.Run.CompletedAt == nil || got.Attempt.CompletedAt == nil {
			t.Fatalf("disabled workflow = %+v", got)
		}
		if !reflect.DeepEqual(got.Task.Placement.Hosts, []string{"normandy"}) ||
			len(got.Task.Routes) != 1 ||
			got.Task.Routes[0].WorkerID != "normandy" ||
			got.Task.Routes[0].ProviderInstanceID != "codex" ||
			got.Task.Routes[0].Model != "gpt-5.6-sol" ||
			got.Task.Routes[0].Options["effort"] != "high" ||
			got.Task.EstimatedCost == nil || *got.Task.EstimatedCost != 12.5 {
			t.Fatalf("task = %+v", got.Task)
		}
	})
}

func TestT3JobEnqueueSubmissionFixture(t *testing.T) {
	got, err := ParseWorkflowFile("testdata/t3-job-enqueue.md")
	if err != nil {
		t.Fatal(err)
	}
	source := got.Source
	if source.Project != "nightly maintenance" || source.Title != "Scheduled review" ||
		source.Importance != 4 || source.Difficulty != 2 || source.MaxTurns != 5 ||
		source.Model != "claude-opus-5" || source.Instance != "claudeAgent" ||
		source.Host != "normandy" || source.Options["effort"] != "high" ||
		source.Options["contextWindow"] != "1m" || source.NotBefore == nil || source.Deadline == nil {
		t.Fatalf("t3-job source = %+v", source)
	}
	if source.Gated() != true || got.Workflow.Version != 1 ||
		got.Workflow.Class != domain.TaskClassSurplus ||
		got.Attempt.Progress != domain.ProgressReady ||
		!reflect.DeepEqual(got.Task.Placement.Hosts, []string{"normandy"}) {
		t.Fatalf("t3-job workflow adaptation = %+v", got)
	}
	if got.Task.Routes[0].ProviderInstanceID != "claudeAgent" ||
		got.Task.Routes[0].Model != "claude-opus-5" ||
		got.Task.Routes[0].Options["contextWindow"] != "1m" {
		t.Fatalf("t3-job route = %+v", got.Task.Routes)
	}
}

func TestLegacyWorkflowIdentityTracksContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compat.md")
	raw := []byte("---\nproject: p\n---\n# Derived title\n")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := ParseWorkflowFile(path)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseWorkflowFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if first.Workflow.ID != again.Workflow.ID {
		t.Fatalf("unchanged identity differs: %q != %q", first.Workflow.ID, again.Workflow.ID)
	}
	if first.Source.ID != "compat" || first.Source.Title != "Derived title" {
		t.Fatalf("file metadata defaults = %+v", first.Source)
	}

	changed := []byte("---\nproject: p\n---\n# Derived title\n\nchanged\n")
	if err := os.WriteFile(path, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	updated, err := ParseWorkflowFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Workflow.ID == first.Workflow.ID {
		t.Fatalf("edited task retained immutable workflow id %q", updated.Workflow.ID)
	}
	if !strings.HasPrefix(updated.Workflow.ID, "legacy:compat:") {
		t.Fatalf("workflow id = %q", updated.Workflow.ID)
	}
}

func TestLoadDirUsesLegacyWorkflowAdapter(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"b.md", "a.md"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("---\nproject: p\n---\n"+name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ".ignored.md"), []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}

	workflows, workflowErrs := LoadLegacyWorkflows(dir)
	tasks, taskErrs := LoadDir(dir)
	if len(workflowErrs) != 0 || len(taskErrs) != 0 {
		t.Fatalf("load errors: %v / %v", workflowErrs, taskErrs)
	}
	if len(workflows) != 2 || len(tasks) != 2 {
		t.Fatalf("counts = %d workflows, %d tasks", len(workflows), len(tasks))
	}
	for i := range workflows {
		if !reflect.DeepEqual(tasks[i], workflows[i].Source) {
			t.Fatalf("task %d differs from adapter source", i)
		}
	}
}

func TestLegacyTaskOrderingCompatibility(t *testing.T) {
	now := time.Date(2030, 1, 2, 12, 0, 0, 0, time.UTC)
	soon := now.Add(23 * time.Hour)
	past := now.Add(-time.Hour)
	costFour := 4.0
	tasks := []Task{
		{ID: "seed", Importance: 5, Difficulty: 1},
		{ID: "state", Importance: 5, Difficulty: 5},
		{ID: "override", Importance: 5, Difficulty: 5, EstimatedCost: &costFour},
		{ID: "ordinary", Importance: 4, Difficulty: 1},
		{ID: "past", Importance: 1, Difficulty: 1, Deadline: &past},
		{ID: "soon", Importance: 2, Difficulty: 5, Deadline: &soon},
	}
	states := map[string]*State{"state": {ID: "state", EstimatedCost: 3}}

	Order(tasks, states, now)

	got := make([]string, len(tasks))
	for i := range tasks {
		got[i] = tasks[i].ID
	}
	want := []string{"soon", "past", "state", "override", "seed", "ordinary"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}
