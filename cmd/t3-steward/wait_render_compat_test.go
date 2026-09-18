package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// rc69TaskWaitDetail is a verbatim copy of TaskWaitDetail at v0.11.0-rc.69
// (git show bd5362b:internal/backlogadmin/types.go): the entry shape of the
// "waits" key in a run document produced by the previous release.
type rc69TaskWaitDetail struct {
	ID           string    `json:"id"`
	TaskID       string    `json:"taskId"`
	TaskName     string    `json:"taskName,omitempty"`
	AttemptID    string    `json:"attemptId"`
	Name         string    `json:"name,omitempty"`
	Condition    string    `json:"condition,omitempty"`
	RegisteredAt time.Time `json:"registeredAt"`
	Deadline     time.Time `json:"deadline"`
}

// rc69WorkflowDetail and rc69Diagnosis are the two documents as an rc.69
// coordinator emits them: one wait key called "waits", meaning the live task
// waits in a run document and the node waits in a diagnosis, and no
// "taskWaits" key at all in the run document. Only the wait keys are pinned at
// the rc.69 shape; the parts the renderer needs in order to have something to
// print are built from the live types, because nothing about them changed in
// this stage and giving them a frozen copy would pin the wrong thing.
type rc69WorkflowDetail struct {
	Summary backlogadmin.WorkflowSummary `json:"summary"`
	Tasks   []backlogadmin.TaskDetail    `json:"tasks"`
	Waits   []rc69TaskWaitDetail         `json:"waits,omitempty"`
}

type rc69Diagnosis struct {
	GraphRevision int64              `json:"graphRevision"`
	GeneratedAt   time.Time          `json:"generatedAt"`
	Workflow      rc69WorkflowDetail `json:"workflow"`
	Waits         []domain.NodeWait  `json:"waits"`
	TaskWaits     []domain.TaskWait  `json:"taskWaits"`
}

var waitRenderCompatNow = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

func rc69RunDocument() rc69WorkflowDetail {
	return rc69WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
			Workflow: domain.Workflow{ID: "workflow-1", Name: "workflow"},
		},
		Tasks: []backlogadmin.TaskDetail{{
			Task:    domain.Task{ID: "task-1", Name: "alpha"},
			Attempt: &domain.Attempt{ID: "attempt-1", Progress: domain.ProgressWaitingExternal},
		}},
		Waits: []rc69TaskWaitDetail{{
			ID: "tw-live", TaskID: "task-1", TaskName: "alpha", AttemptID: "attempt-1",
			Name: "ci", Condition: "gh run view",
			RegisteredAt: waitRenderCompatNow.Add(-time.Hour),
			Deadline:     waitRenderCompatNow.Add(time.Hour),
		}},
	}
}

// A mixed-version window is the normal state during a release, and it runs in
// both directions: the deprecated key exists so that an rc.69 client can read
// an rc.70 coordinator, and this is the other half, an rc.70 client reading an
// rc.69 coordinator. The renderers read only the new keys, so a parked task
// would be shown as parked with nothing to say about what it is parked on.
func TestTheRenderersReadTheDeprecatedWaitsKeyOfAnOlderCoordinator(t *testing.T) {
	raw, err := json.Marshal(rc69RunDocument())
	if err != nil {
		t.Fatal(err)
	}
	var detail backlogadmin.WorkflowDetail
	if err := json.Unmarshal(raw, &detail); err != nil {
		t.Fatal(err)
	}
	if len(detail.TaskWaits) != 0 {
		t.Fatalf("an rc.69 run document carries taskWaits: %+v", detail.TaskWaits)
	}
	var out bytes.Buffer
	renderWorkflow(&out, &detail)
	if want := `wait tw-live "ci": gh run view (deadline 2026-09-18T13:00:00Z)`; !strings.Contains(out.String(), want) {
		t.Fatalf("the run text does not contain %q:\n%s", want, out.String())
	}

	diagnosis := rc69Diagnosis{
		GraphRevision: 3, GeneratedAt: waitRenderCompatNow,
		Workflow: rc69RunDocument(),
		Waits: []domain.NodeWait{{
			Request: domain.NodeWaitRequest{
				ID: "nw-1", ThreadID: "thread-x", Name: "until alpha",
				Target: domain.NodeRef{RunID: "run-1", TaskID: "task-1"},
			},
			Host: "omarchy-pc", Delivery: "pending",
			Deadline: waitRenderCompatNow.Add(2 * time.Hour),
		}},
		TaskWaits: []domain.TaskWait{{
			ID: "tw-live", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Name: "ci", Condition: "gh run view",
			RegisteredAt: waitRenderCompatNow.Add(-time.Hour),
			Deadline:     waitRenderCompatNow.Add(time.Hour),
		}},
	}
	if raw, err = json.Marshal(diagnosis); err != nil {
		t.Fatal(err)
	}
	var current backlogadmin.Diagnosis
	if err := json.Unmarshal(raw, &current); err != nil {
		t.Fatal(err)
	}
	if len(current.NodeWaits) != 0 {
		t.Fatalf("an rc.69 diagnosis carries nodeWaits: %+v", current.NodeWaits)
	}
	out.Reset()
	renderDiagnosis(&out, &current)
	for _, want := range []string{
		"node waits:\n",
		`  nw-1 "until alpha" thread=thread-x host=omarchy-pc delivery=pending (deadline 2026-09-18T14:00:00Z)`,
		// taskWaits is not renamed in a diagnosis: rc.69 emits it under this
		// name already, so only the node waits need the fallback here.
		`  tw-live task=task-1 attempt=attempt-1 "ci": gh run view`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("the diagnosis text does not contain %q:\n%s", want, out.String())
		}
	}
}

// The fallback is a fallback: when this release's own documents carry both
// keys, the new one is what is rendered and no wait is printed twice.
func TestTheRenderersPreferTheNewWaitKeysWhenBothArePresent(t *testing.T) {
	settledAt := waitRenderCompatNow.Add(-time.Hour)
	live := backlogadmin.TaskWaitDetail{
		ID: "tw-live", TaskID: "task-1", AttemptID: "attempt-1", Name: "ci",
		Condition: "gh run view", Deadline: waitRenderCompatNow.Add(time.Hour),
	}
	detail := backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
			Workflow: domain.Workflow{ID: "workflow-1", Name: "workflow"},
		},
		Tasks: []backlogadmin.TaskDetail{{
			Task:    domain.Task{ID: "task-1", Name: "alpha"},
			Attempt: &domain.Attempt{ID: "attempt-1", Progress: domain.ProgressWaitingExternal},
		}},
		TaskWaits: []backlogadmin.TaskWaitDetail{live, {
			ID: "tw-settled", TaskID: "task-1", AttemptID: "attempt-1", Name: "review",
			Condition: "gh pr view", Outcome: string(domain.TaskWaitMet), SettledAt: &settledAt,
		}},
		Waits: []backlogadmin.TaskWaitDetail{live},
	}
	var out bytes.Buffer
	renderWorkflow(&out, &detail)
	if got := strings.Count(out.String(), "wait tw-live "); got != 1 {
		t.Fatalf("the live wait is printed %d times:\n%s", got, out.String())
	}
	if !strings.Contains(out.String(), "wait tw-settled ") {
		t.Fatalf("the settled wait, which only taskWaits carries, is not printed:\n%s", out.String())
	}

	nodeWait := domain.NodeWait{
		Request: domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread-x", Name: "until alpha"},
		Host:    "omarchy-pc", Delivery: "pending", Deadline: waitRenderCompatNow.Add(2 * time.Hour),
	}
	diagnosis := backlogadmin.Diagnosis{
		GraphRevision: 3, GeneratedAt: waitRenderCompatNow, Workflow: detail,
		NodeWaits: []domain.NodeWait{nodeWait},
		Waits:     []domain.NodeWait{nodeWait},
	}
	out.Reset()
	renderDiagnosis(&out, &diagnosis)
	if got := strings.Count(out.String(), "nw-1 "); got != 1 {
		t.Fatalf("the node wait is printed %d times:\n%s", got, out.String())
	}
}
