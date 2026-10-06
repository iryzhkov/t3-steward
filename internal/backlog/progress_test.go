package backlog

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func progressFixture() (sqlite.CoordinatorRecords, time.Time) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	records := sqlite.CoordinatorRecords{}
	for i, state := range []domain.ProgressState{domain.ProgressQueued, domain.ProgressActive, domain.ProgressWaitingExternal, domain.ProgressFailed, domain.ProgressSucceeded, domain.ProgressCancelled} {
		id := string(rune('a' + i))
		records.Workflows = append(records.Workflows, domain.Workflow{ID: id, Name: stateString(state), TaskIDs: []string{id}})
		records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{ID: id, WorkflowID: id, Progress: state, UpdatedAt: now})
		records.Tasks = append(records.Tasks, domain.Task{ID: id, WorkflowID: id, Name: "work"})
		if i == 0 {
			continue
		}
		records.Attempts = append(records.Attempts, domain.Attempt{ID: id, WorkflowRunID: id, TaskID: id, Number: 1, Progress: state, Control: domain.ControlRunning, AssignmentID: id, UpdatedAt: now})
		records.Assignments = append(records.Assignments, domain.Assignment{ID: id, WorkerID: "worker", Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol", Options: map[string]string{"effort": "medium"}}})
	}
	records.Artifacts = []domain.Artifact{{ID: "out", WorkflowRunID: "e", AttemptID: "e", Kind: domain.ArtifactOutput, Name: "handoff.md"}}
	records.ReviewRounds = []review.Round{{ID: "round", WorkflowRunID: "e", Reviewers: []review.Reviewer{{ID: "r", TaskID: "e", Role: "reviewer", Route: "codex/sol", Verdict: &review.Verdict{Verdict: "accept"}}}}}
	return records, now
}
func stateString(s domain.ProgressState) string { return string(s) }

func TestProgressReplayAndAmendedGraph(t *testing.T) {
	records, now := progressFixture()
	records.WorkflowRuns[1].Graph = &domain.GraphDefinition{Tasks: []domain.Task{{ID: "new", Name: "amended"}}}
	records.Attempts = append(records.Attempts,
		domain.Attempt{ID: "retry", TaskID: "new", WorkflowRunID: "b", Number: 2, Progress: domain.ProgressActive, UpdatedAt: now},
		domain.Attempt{ID: "old", TaskID: "new", WorkflowRunID: "b", Number: 1, Progress: domain.ProgressFailed, UpdatedAt: now},
		domain.Attempt{ID: "overseer", TaskID: "new", WorkflowRunID: "b", Number: 3, Progress: domain.ProgressSucceeded, SupervisionActivationID: "activation", UpdatedAt: now})
	doc, err := BuildProgress(records, nil, false, nil, ProgressFilter{RunIDs: []string{"b", "b"}}, now)
	if err != nil {
		t.Fatal(err)
	}
	run := doc.Runs[0]
	if len(doc.Runs) != 1 || run.Total != 1 || run.Done != 0 || run.CurrentTask != "amended" || run.Tasks[0].Attempts != 2 || run.Tasks[0].State != domain.ProgressActive {
		t.Fatalf("amended/retry: %+v", doc)
	}
	if run.Tasks[0].ReviewVerdicts != ledgerReviewsUnavailable {
		t.Fatal(run.Tasks[0])
	}
	records.Attempts[0], records.Attempts[len(records.Attempts)-1] = records.Attempts[len(records.Attempts)-1], records.Attempts[0]
	again, err := BuildProgress(records, nil, false, nil, ProgressFilter{RunIDs: []string{"b"}}, now)
	if err != nil || !reflect.DeepEqual(doc, again) {
		t.Fatalf("reordered: %+v %v", again, err)
	}
	var out strings.Builder
	doc.Runs[0].Name = strings.Repeat("界", 300) + "\x1b\nsecret"
	if err = RenderProgress(&out, doc); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(out.String(), "\n") {
		if len([]rune(line)) > 120 || strings.ContainsRune(line, '\x1b') {
			t.Fatalf("unbounded/unsafe: %q", line)
		}
	}
}

func TestProgressTextColumns(t *testing.T) {
	line := progressLine(strings.Repeat("界", 120))
	// Non-ASCII names must be escaped or measured for terminal display width.
	// Every retained wide glyph occupies two columns.
	columns := 0
	for _, r := range line {
		if r == '界' {
			columns += 2
		} else {
			columns++
		}
	}
	if columns > 120 {
		t.Fatalf("text occupies %d terminal columns: %s", columns, line)
	}
}

func TestProgressGolden(t *testing.T) {
	records, now := progressFixture()
	doc, err := BuildProgress(records, records.ReviewRounds, true, nil, ProgressFilter{}, now)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	want, err := os.ReadFile("testdata/progress.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(want) {
		t.Fatalf("JSON golden mismatch:\n%s", data)
	}
	var text strings.Builder
	if err := RenderProgress(&text, doc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "review=accept") {
		t.Fatalf("text hides recorded verdict: %s", text.String())
	}
	want, err = os.ReadFile("testdata/progress.txt")
	if err != nil {
		t.Fatal(err)
	}
	if text.String() != string(want) {
		t.Fatalf("text golden mismatch:\n%s", text.String())
	}
	for _, line := range strings.Split(text.String(), "\n") {
		if len([]rune(line)) > 120 {
			t.Fatalf("wide line: %s", line)
		}
	}
}

func TestProgressFiltersAndLedgerAgreement(t *testing.T) {
	records, now := progressFixture()
	old := now.Add(-25 * time.Hour)
	records.WorkflowRuns[3].UpdatedAt = old
	records.Attempts[2].UpdatedAt = old
	waits := []domain.NodeWait{{Request: domain.NodeWaitRequest{ThreadID: "owner", Target: domain.NodeRef{RunID: "e", TaskID: domain.SinkTaskName}}}}
	doc, err := BuildProgress(records, records.ReviewRounds, true, waits, ProgressFilter{}, now)
	if err != nil || len(doc.Runs) != 5 {
		t.Fatalf("default window: %+v %v", doc, err)
	}
	doc, err = BuildProgress(records, records.ReviewRounds, true, waits, ProgressFilter{Owner: "owner", Since: now}, now)
	if err != nil || len(doc.Runs) != 1 {
		t.Fatalf("owner/since: %+v %v", doc, err)
	}
	task := doc.Runs[0].Tasks[0]
	v := newLedgerRunView(records, records.Workflows[4], records.WorkflowRuns[4])
	facts := &ledgerFacts{reconciler: &LedgerReconciler{ReviewRounds: func(context.Context, string) ([]review.Round, error) { return records.ReviewRounds, nil }}, rounds: map[string][]review.Round{"e": records.ReviewRounds}}
	body, err := v.record(context.Background(), facts, ledgerAttemptBoundary+"e", 1024)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"progress " + string(task.State), v.route(records.Attempts[3]), task.ReviewVerdicts} {
		if !strings.Contains(string(body), value) {
			t.Fatalf("ledger/mirror disagree on %q: %s", value, body)
		}
	}
	if task.Route != "codex/sol" || task.Effort != "medium" || !reflect.DeepEqual(task.Outputs, []string{"handoff.md"}) {
		t.Fatalf("facts: %+v", task)
	}
	doc, err = BuildProgress(records, nil, true, nil, ProgressFilter{RunIDs: []string{"d"}}, now)
	if err != nil || len(doc.Runs) != 1 {
		t.Fatalf("explicit old run: %+v %v", doc, err)
	}
	if _, err = BuildProgress(records, nil, true, nil, ProgressFilter{RunIDs: []string{"missing"}}, now); err == nil {
		t.Fatal("missing run accepted")
	}
	doc, err = BuildProgress(records, nil, true, waits, ProgressFilter{Owner: "nobody"}, now)
	var out strings.Builder
	if err != nil {
		t.Fatal(err)
	}
	if err = RenderProgress(&out, doc); err != nil {
		t.Fatal(err)
	}
	if out.String() != "No campaign runs match.\n" {
		t.Fatalf("empty: %q", out.String())
	}
}
