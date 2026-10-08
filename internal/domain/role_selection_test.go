package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTasksForRunOverlaysRoleSelectionWithoutMutatingTemplate(t *testing.T) {
	template := Task{ID: "task", WorkflowID: "workflow", Role: "execute", RoleEffort: "low"}
	selection := RoleSelection{Role: "execute", Route: "codex/gpt", Effort: "low", PolicyDigest: "digest", Candidates: []RoleCandidateVerdict{{Route: "codex/gpt", Eligible: true}}, ResolvedAt: time.Now()}
	run := WorkflowRun{ID: "run", WorkflowID: "workflow", RouteSelections: map[string]RoleSelection{"task": selection}}
	for _, graph := range []bool{false, true} {
		if graph {
			run.Graph = &GraphDefinition{Tasks: []Task{template}}
		}
		tasks := TasksForRun(run, []Task{template})
		if len(tasks) != 1 || len(tasks[0].Routes) != 1 || tasks[0].Routes[0].ProviderInstanceID != "codex" || tasks[0].Routes[0].Model != "gpt" || tasks[0].Routes[0].Options["effort"] != "low" || tasks[0].RoleSelection == nil {
			t.Fatalf("overlay = %#v", tasks)
		}
		tasks[0].RoleSelection.Candidates[0].Reason = "mutated"
		tasks[0].Routes[0].Options["effort"] = "high"
		if run.RouteSelections["task"].Candidates[0].Reason != "" {
			t.Fatal("receipt aliases run")
		}
		task, ok := TaskForAttempt(Attempt{WorkflowRunID: "run", TaskID: "task"}, []WorkflowRun{run}, []Task{template})
		if !ok || task.Routes[0].Options["effort"] != "low" {
			t.Fatalf("attempt overlay = %#v", task)
		}
	}
	if len(template.Routes) != 0 || template.RoleSelection != nil {
		t.Fatal("template mutated")
	}
}

func TestRunRoleSelectionPreservesResolvedGraphRoute(t *testing.T) {
	selection := RoleSelection{Role: "execute", Route: "codex/gpt", Effort: "low"}
	amended := Task{ID: "task", WorkflowID: "workflow", Role: "execute", Routes: []ProviderRoute{{ProviderInstanceID: "claudeAgent", Model: "opus"}}}
	run := WorkflowRun{ID: "run", WorkflowID: "workflow", RouteSelections: map[string]RoleSelection{"task": selection}, Graph: &GraphDefinition{Tasks: []Task{amended}}}
	tasks := TasksForRun(run, nil)
	if len(tasks) != 1 || tasks[0].Routes[0].ProviderInstanceID != "claudeAgent" {
		t.Fatalf("amended route replaced: %#v", tasks)
	}
}

func TestTaskWithoutRoleKeepsLegacyJSONAndDigest(t *testing.T) {
	task := Task{ID: "old", WorkflowID: "workflow", Routes: []ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}}}
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"role", "roleEffort", "roleSelection"} {
		if strings.Contains(string(raw), "\""+field+"\"") {
			t.Fatalf("new field in old task: %s", raw)
		}
	}
	run := WorkflowRun{ID: "run", WorkflowID: "workflow"}
	if got := TasksForRun(run, []Task{task}); len(got) != 1 || TaskDigest(got[0]) != TaskDigest(task) {
		t.Fatal("legacy digest changed")
	}
}
