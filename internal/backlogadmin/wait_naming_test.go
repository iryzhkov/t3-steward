package backlogadmin

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var waitNamingNow = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

// waitNamingView is one run with one task and two task waits, one live and one
// settled, built without a store: the naming of the two wait families is a
// property of the documents, not of where the records came from.
func waitNamingView() view {
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 1, Name: "workflow", Class: domain.TaskClassSurplus,
			TaskIDs: []string{"task-1"},
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive,
			Revision: 1, CreatedAt: waitNamingNow, UpdatedAt: waitNamingNow,
		}},
		Tasks: []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "alpha", Class: domain.TaskClassSurplus}},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal,
			Revision: 2, UpdatedAt: waitNamingNow,
		}},
	}
	settledAt := waitNamingNow.Add(-time.Hour)
	v := newView(records, nil, nil, RuntimeInfo{}, waitNamingNow)
	v.taskWaits = []domain.TaskWait{
		{
			ID: "tw-live", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Name: "ci", Condition: "gh run view", Kind: domain.WaitKindShell,
			RegisteredAt: waitNamingNow.Add(-2 * time.Hour), Deadline: waitNamingNow.Add(time.Hour),
		},
		{
			ID: "tw-settled", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Name: "review", Condition: "gh pr view", Kind: domain.WaitKindShell,
			RegisteredAt: waitNamingNow.Add(-3 * time.Hour), Deadline: waitNamingNow.Add(time.Hour),
			SettledAt: &settledAt,
			Result: &domain.TaskWaitResult{
				Outcome: domain.TaskWaitFailed, ExitCode: 2, Reason: "the check failed",
				ObservedAt: settledAt,
			},
		},
	}
	return v
}

// A settled wait is why a task stopped waiting, and it was dropped from the
// run document entirely: "campaign show" could say a task was parked and, one
// tick later, say nothing at all about what became of the wait.
func TestWorkflowDetailListsSettledTaskWaitsWithTheirOutcome(t *testing.T) {
	detail, ok := waitNamingView().workflowDetail("run-1")
	if !ok {
		t.Fatal("run-1 has no detail")
	}
	if len(detail.TaskWaits) != 2 {
		t.Fatalf("taskWaits = %+v, want the live and the settled one", detail.TaskWaits)
	}
	byID := map[string]TaskWaitDetail{}
	for _, wait := range detail.TaskWaits {
		byID[wait.ID] = wait
	}
	settled := byID["tw-settled"]
	if settled.Outcome != string(domain.TaskWaitFailed) || settled.ExitCode != 2 ||
		settled.Reason != "the check failed" || settled.SettledAt == nil {
		t.Fatalf("settled wait = %+v", settled)
	}
	if live := byID["tw-live"]; live.Outcome != "" || live.SettledAt != nil {
		t.Fatalf("a live wait reported an outcome: %+v", live)
	}
	// The old key keeps its old meaning for one release: the live waits only.
	if len(detail.Waits) != 1 || detail.Waits[0].ID != "tw-live" {
		t.Fatalf("waits = %+v, want the live wait alone", detail.Waits)
	}
}

// rc69TaskWaitDetail is a verbatim copy of TaskWaitDetail at v0.11.0-rc.69
// (git show bd5362b:internal/backlogadmin/types.go), the shape every deployed
// admin client decodes the "waits" entries of a run document into. It is kept
// here, not shared with the live type, so that a change to the live type
// cannot silently change what this test pins.
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

// rc69WorkflowDetail and rc69Diagnosis are how rc.69 reads the two documents
// this stage renamed keys in. Only the wait keys are pinned: the rest of each
// document is held as raw JSON because nothing else in it is what this test is
// about, and giving those fields the live types would defeat the pin.
type rc69WorkflowDetail struct {
	Summary       json.RawMessage      `json:"summary"`
	Tasks         json.RawMessage      `json:"tasks"`
	Artifacts     json.RawMessage      `json:"artifacts,omitempty"`
	ResourceLocks json.RawMessage      `json:"resourceLocks,omitempty"`
	Reservations  json.RawMessage      `json:"reservations,omitempty"`
	Waits         []rc69TaskWaitDetail `json:"waits,omitempty"`
	Gates         json.RawMessage      `json:"gates,omitempty"`
}

type rc69Diagnosis struct {
	Revisions     json.RawMessage   `json:"revisions"`
	GraphRevision int64             `json:"graphRevision"`
	GeneratedAt   time.Time         `json:"generatedAt"`
	Status        json.RawMessage   `json:"status"`
	Workflow      json.RawMessage   `json:"workflow"`
	Graph         json.RawMessage   `json:"graph"`
	Events        json.RawMessage   `json:"events"`
	Explanations  json.RawMessage   `json:"explanations"`
	Assignments   json.RawMessage   `json:"assignments"`
	Commands      json.RawMessage   `json:"commands"`
	Waits         []domain.NodeWait `json:"waits"`
	TaskWaits     json.RawMessage   `json:"taskWaits"`
	Workers       json.RawMessage   `json:"workers"`
	Unavailable   json.RawMessage   `json:"unavailable,omitempty"`
}

// decodeStrict is the strict decode: the one an owner-only local socket does
// (readLocalJSON, local_transport.go) and the one that says whether a document
// carries a field the previous release does not know.
func decodeStrict(raw []byte, into any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(into)
}

// One key, two meanings: "waits" was the task waits in a run document and the
// node waits in a diagnosis. Both documents now name each family, and both
// keep the old key for one release so an rc.69 client still reads them.
//
// This is the cross-release pin stages 3 and 4 have (runtime_status_compat_test
// and wait_task_compat_test): the rc.69 shapes are declared here verbatim and
// this branch's documents are decoded into them, so "kept for one release"
// means the previous release's own reader, not a key of the same name.
func TestWaitKeysNameTheSameFamilyInBothDocuments(t *testing.T) {
	detail, ok := waitNamingView().workflowDetail("run-1")
	if !ok {
		t.Fatal("run-1 has no detail")
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"waits", "taskWaits"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("the run document has no %q key: %s", key, raw)
		}
	}

	// The cross-host admin path decodes the response leniently
	// (ssh_client.go), which is how an rc.69 admin host reads this document.
	var oldRun rc69WorkflowDetail
	if err := json.Unmarshal(raw, &oldRun); err != nil {
		t.Fatalf("an rc.69 client cannot decode the run document: %v\n%s", err, raw)
	}
	if len(oldRun.Waits) != 1 {
		t.Fatalf("rc.69 reads waits = %+v, want the one live wait", oldRun.Waits)
	}
	if got, want := oldRun.Waits[0], (rc69TaskWaitDetail{
		ID: "tw-live", TaskID: "task-1", TaskName: "alpha", AttemptID: "attempt-1",
		Name: "ci", Condition: "gh run view",
		RegisteredAt: waitNamingNow.Add(-2 * time.Hour), Deadline: waitNamingNow.Add(time.Hour),
	}); got != want {
		t.Fatalf("rc.69 reads waits[0] = %+v, want %+v", got, want)
	}

	// Each entry of the deprecated key is frozen at the rc.69 shape for the
	// deprecation window: everything this release added to a wait travels
	// under taskWaits, so even a strict rc.69 reader of one entry accepts it.
	var entries []json.RawMessage
	if err := json.Unmarshal(document["waits"], &entries); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		var old rc69TaskWaitDetail
		if err := decodeStrict(entry, &old); err != nil {
			t.Fatalf("an entry of the deprecated waits key is not the rc.69 shape: %v\n%s", err, entry)
		}
	}

	diagnosis := Diagnosis{
		NodeWaits: []domain.NodeWait{{Request: domain.NodeWaitRequest{ID: "nw-1"}}},
		TaskWaits: []domain.TaskWait{{ID: "tw-live"}},
	}
	diagnosis.Waits = diagnosis.NodeWaits
	raw, err = json.Marshal(diagnosis)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"waits", "nodeWaits", "taskWaits"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("the diagnosis has no %q key: %s", key, raw)
		}
	}
	var oldDiagnosis rc69Diagnosis
	if err := json.Unmarshal(raw, &oldDiagnosis); err != nil {
		t.Fatalf("an rc.69 client cannot decode the diagnosis: %v\n%s", err, raw)
	}
	if len(oldDiagnosis.Waits) != 1 || oldDiagnosis.Waits[0].Request.ID != "nw-1" {
		t.Fatalf("rc.69 reads the diagnosis waits = %+v, want the node waits", oldDiagnosis.Waits)
	}
	// A node wait record is untouched by this stage, so a strict rc.69 reader
	// accepts the whole entry, not only the fields it names.
	if err := json.Unmarshal(document["waits"], &entries); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		var old domain.NodeWait
		if err := decodeStrict(entry, &old); err != nil {
			t.Fatalf("an entry of the diagnosis waits key is not the rc.69 shape: %v\n%s", err, entry)
		}
	}
}

// What the strict decode of a whole document does, stated rather than assumed.
// Both documents gain a key rc.69 does not know, so the strict path refuses
// them: that path is the owner-only local socket, reachable only when an rc.69
// binary talks to an rc.70 daemon on the same host, which is the downgrade
// shape reported on 2026-09-18 and is not new in this stage -- every key stages
// 1 to 4 added is in it too. The cross-host path, which is the one that spans
// releases, is lenient and is pinned above.
func TestTheStrictLocalDecodeRefusesTheNewWaitKeys(t *testing.T) {
	detail, ok := waitNamingView().workflowDetail("run-1")
	if !ok {
		t.Fatal("run-1 has no detail")
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var oldRun rc69WorkflowDetail
	err = decodeStrict(raw, &oldRun)
	if err == nil || !strings.Contains(err.Error(), `unknown field "taskWaits"`) {
		t.Fatalf("strict rc.69 decode of the run document: %v, want a refusal naming taskWaits", err)
	}
	diagnosis := Diagnosis{NodeWaits: []domain.NodeWait{{Request: domain.NodeWaitRequest{ID: "nw-1"}}}}
	diagnosis.Waits = diagnosis.NodeWaits
	if raw, err = json.Marshal(diagnosis); err != nil {
		t.Fatal(err)
	}
	var oldDiagnosis rc69Diagnosis
	err = decodeStrict(raw, &oldDiagnosis)
	if err == nil || !strings.Contains(err.Error(), `unknown field "nodeWaits"`) {
		t.Fatalf("strict rc.69 decode of the diagnosis: %v, want a refusal naming nodeWaits", err)
	}
}

// Both keys are present even when there is nothing to report, so a reader
// branches on the array rather than on whether the field exists at all.
func TestWaitKeysAreAlwaysPresent(t *testing.T) {
	raw, err := json.Marshal(WorkflowDetail{})
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"waits", "taskWaits"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("an empty run document omits %q: %s", key, raw)
		}
	}
}
