package main

import (
	"bytes"
	"context"
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

func rc69DiagnosisDocument() rc69Diagnosis {
	return rc69Diagnosis{
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
}

// decodeInto is the one thing a transport does with a coordinator's bytes
// (internal/backlogadmin/ssh_client.go: plain json.Unmarshal), so it is what
// turns an older release's document into this release's types.
func decodeInto(t *testing.T, document, target any) {
	t.Helper()
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}

// rc69RunAnswer and rc69DiagnosisAnswer are what this CLI holds after a
// coordinator of the previous release has answered. A fresh value is built per
// call, because the CLI fills the renamed keys of the document it was given.
func rc69RunAnswer(t *testing.T) backlogadmin.Response {
	t.Helper()
	var detail backlogadmin.WorkflowDetail
	decodeInto(t, rc69RunDocument(), &detail)
	if len(detail.TaskWaits) != 0 {
		t.Fatalf("an rc.69 run document carries taskWaits: %+v", detail.TaskWaits)
	}
	return backlogadmin.Response{Kind: backlogadmin.QueryWorkflow, Workflow: &detail}
}

func rc69DiagnosisAnswer(t *testing.T) backlogadmin.Response {
	t.Helper()
	var diagnosis backlogadmin.Diagnosis
	decodeInto(t, rc69DiagnosisDocument(), &diagnosis)
	if len(diagnosis.NodeWaits) != 0 {
		t.Fatalf("an rc.69 diagnosis carries nodeWaits: %+v", diagnosis.NodeWaits)
	}
	return backlogadmin.Response{Kind: backlogadmin.QueryDiagnose, Diagnosis: &diagnosis}
}

// answeredWith runs one admin command against a coordinator that answers with
// that document. The whole command is exercised rather than a renderer,
// because the defect this pins is that the text form and the --json form read
// the answer differently.
func answeredWith(t *testing.T, response backlogadmin.Response, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service:   &fakeAdminMutationService{queryResponse: response},
		principal: backlogadmin.Principal{ID: "operator"},
		stdout:    &out,
	}
	if err := cli.runBacklog(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// waitIDs and nodeWaitIDs keep a failure readable: what matters is which key
// carries which wait, not the rest of a run document.
func waitIDs(waits []backlogadmin.TaskWaitDetail) []string {
	ids := make([]string, 0, len(waits))
	for _, wait := range waits {
		ids = append(ids, wait.ID)
	}
	return ids
}

func nodeWaitIDs(waits []domain.NodeWait) []string {
	ids := make([]string, 0, len(waits))
	for _, wait := range waits {
		ids = append(ids, wait.Request.ID)
	}
	return ids
}

// printedDocument is what an agent parses out of a --json answer.
func printedDocument(t *testing.T, printed string) backlogadmin.Response {
	t.Helper()
	var response backlogadmin.Response
	if err := json.Unmarshal([]byte(printed), &response); err != nil {
		t.Fatalf("--json did not print one document: %v\n%s", err, printed)
	}
	return response
}

// A mixed-version window is the normal state during a release, and it runs in
// both directions: the deprecated key exists so that an rc.69 client can read
// an rc.70 coordinator, and this is the other half, an rc.70 client reading an
// rc.69 coordinator. This client reads only the new keys, so a parked task
// would be shown as parked with nothing to say about what it is parked on, and
// --json -- the form the skills tell an agent to use -- would print
// "taskWaits": null and "nodeWaits": null beside a populated "waits", which
// errors nowhere and reads as a run parked on nothing.
func TestTheCLIReadsTheDeprecatedWaitsKeyOfAnOlderCoordinator(t *testing.T) {
	t.Run("run text", func(t *testing.T) {
		text := answeredWith(t, rc69RunAnswer(t), "show", "run-1")
		if want := `wait tw-live "ci": gh run view (deadline 2026-09-18T13:00:00Z)`; !strings.Contains(text, want) {
			t.Fatalf("the run text does not contain %q:\n%s", want, text)
		}
	})
	t.Run("run json", func(t *testing.T) {
		document := printedDocument(t, answeredWith(t, rc69RunAnswer(t), "show", "run-1", "--json"))
		if document.Workflow == nil {
			t.Fatal("--json printed no run document")
		}
		if len(document.Workflow.TaskWaits) != 1 || document.Workflow.TaskWaits[0].ID != "tw-live" {
			t.Fatalf("taskWaits = %v beside waits = %v, want the wait the older coordinator sent",
				waitIDs(document.Workflow.TaskWaits), waitIDs(document.Workflow.Waits))
		}
		// The deprecated key keeps what it carried: this release still emits it
		// so that a client of the previous release can read this coordinator.
		if len(document.Workflow.Waits) != 1 {
			t.Fatalf("waits = %+v, want the deprecated key left populated", document.Workflow.Waits)
		}
	})
	t.Run("diagnosis text", func(t *testing.T) {
		text := answeredWith(t, rc69DiagnosisAnswer(t), "diagnose", "run-1")
		for _, want := range []string{
			"node waits:\n",
			`  nw-1 "until alpha" thread=thread-x host=omarchy-pc delivery=pending (deadline 2026-09-18T14:00:00Z)`,
			// taskWaits is not renamed in a diagnosis: rc.69 emits it under this
			// name already, so only the node waits need the fallback here.
			`  tw-live task=task-1 attempt=attempt-1 "ci": gh run view`,
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("the diagnosis text does not contain %q:\n%s", want, text)
			}
		}
	})
	t.Run("diagnosis json", func(t *testing.T) {
		document := printedDocument(t, answeredWith(t, rc69DiagnosisAnswer(t), "diagnose", "run-1", "--json"))
		if document.Diagnosis == nil {
			t.Fatal("--json printed no diagnosis")
		}
		if len(document.Diagnosis.NodeWaits) != 1 || document.Diagnosis.NodeWaits[0].Request.ID != "nw-1" {
			t.Fatalf("nodeWaits = %v beside waits = %v, want the node wait the older coordinator sent",
				nodeWaitIDs(document.Diagnosis.NodeWaits), nodeWaitIDs(document.Diagnosis.Waits))
		}
		if len(document.Diagnosis.Waits) != 1 {
			t.Fatalf("waits = %+v, want the deprecated key left populated", document.Diagnosis.Waits)
		}
		// The run document nested in a diagnosis is the same document and is
		// normalised the same way.
		if len(document.Diagnosis.Workflow.TaskWaits) != 1 {
			t.Fatalf("the nested run document's taskWaits = %+v", document.Diagnosis.Workflow.TaskWaits)
		}
	})
}

// The fallback is a fallback: when this release's own documents carry both
// keys, the new one is what is read and no wait is printed twice.
func TestTheCLIPrefersTheNewWaitKeysWhenBothArePresent(t *testing.T) {
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
	text := answeredWith(t, backlogadmin.Response{
		Kind: backlogadmin.QueryWorkflow, Workflow: &detail,
	}, "show", "run-1")
	if got := strings.Count(text, "wait tw-live "); got != 1 {
		t.Fatalf("the live wait is printed %d times:\n%s", got, text)
	}
	if !strings.Contains(text, "wait tw-settled ") {
		t.Fatalf("the settled wait, which only taskWaits carries, is not printed:\n%s", text)
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
	text = answeredWith(t, backlogadmin.Response{
		Kind: backlogadmin.QueryDiagnose, Diagnosis: &diagnosis,
	}, "diagnose", "run-1")
	if got := strings.Count(text, "nw-1 "); got != 1 {
		t.Fatalf("the node wait is printed %d times:\n%s", got, text)
	}
	document := printedDocument(t, answeredWith(t, backlogadmin.Response{
		Kind: backlogadmin.QueryDiagnose, Diagnosis: &diagnosis,
	}, "diagnose", "run-1", "--json"))
	if len(document.Diagnosis.NodeWaits) != 1 || len(document.Diagnosis.Workflow.TaskWaits) != 2 {
		t.Fatalf("a document that carries both keys was changed by normalisation: %+v", document.Diagnosis)
	}
}
