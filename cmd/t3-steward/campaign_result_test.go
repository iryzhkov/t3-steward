package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignResultFixture is a recorded run: the workflow document the
// coordinator answered with and the retained artifacts it served.
type campaignResultFixture struct {
	Workflow  backlogadmin.WorkflowDetail `json:"workflow"`
	Artifacts map[string]json.RawMessage  `json:"artifacts"`
}

func loadCampaignResultFixture(t *testing.T, name string) campaignResultFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "campaign_result", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture campaignResultFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

// open serves the fixture's artifacts: a JSON string is the file's text, any
// other value its bytes.
func (f campaignResultFixture) open(_ context.Context, id string) (backlogadmin.ArtifactContent, error) {
	raw, ok := f.Artifacts[id]
	if !ok {
		return backlogadmin.ArtifactContent{}, errors.New("artifact " + id + " is not retained")
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		raw = []byte(text)
	}
	return backlogadmin.ArtifactContent{Content: io.NopCloser(bytes.NewReader(raw))}, nil
}

func (f campaignResultFixture) cli(stdout io.Writer) campaignCLI {
	return campaignCLI{
		stdout: stdout, stderr: io.Discard,
		detail: func(_ context.Context, run string) (backlogadmin.WorkflowDetail, error) {
			if run != f.Workflow.Summary.Run.ID {
				return backlogadmin.WorkflowDetail{}, errors.New("no run " + run)
			}
			return f.Workflow, nil
		},
		openArtifact: f.open,
	}
}

// Each recorded run prints its short block, and the exit code is the run's
// own verdict: 0 succeeded, 2 failed, 1 not terminal.
func TestCampaignResultRecordedRuns(t *testing.T) {
	for _, c := range []struct {
		fixture string
		code    int
		want    string
	}{
		{"accepted", 0, `run run-accepted: succeeded (2 tasks: 2 succeeded)
  implement  succeeded; review gate accepted-head; commit 111111111111; verification passed 2/2
  review     succeeded; verdict ACCEPT blocking=0 on implement 111111111111
review ACCEPT and verification passed on the same commit: yes, 111111111111 (review accepted, implement verification passed)
`},
		{"changes-requested", 0, `run run-changes: succeeded (2 tasks: 2 succeeded)
  implement  succeeded; commit 333333333333; verification passed 1/1
  review     succeeded; verdict CHANGES_REQUESTED blocking=2 on implement 333333333333
review ACCEPT and verification passed on the same commit: no (review recorded CHANGES_REQUESTED)
`},
		{"failed", 2, `run run-failed: failed (2 tasks: 1 failed, 1 skipped)
  implement  failed; failure verification: verification command failed (2): make test; verification failed 1/3 (make test (exit 2))
  review     skipped
review ACCEPT and verification passed on the same commit: no (no review verdict is recorded)
`},
		{"pending", 1, `run run-pending: active (2 tasks: 1 active, 1 succeeded)
  implement  succeeded; commit 555555555555; verification passed 1/1
  review     active
review ACCEPT and verification passed on the same commit: no (no review verdict is recorded)
`},
	} {
		t.Run(c.fixture, func(t *testing.T) {
			fixture := loadCampaignResultFixture(t, c.fixture)
			var out bytes.Buffer
			err := fixture.cli(&out).run(context.Background(), []string{"result", fixture.Workflow.Summary.Run.ID})
			if got := out.String(); got != c.want {
				t.Fatalf("output:\n%s\nwant:\n%s", got, c.want)
			}
			if code := exitCodeFor(err); code != c.code {
				t.Fatalf("exit %d (%v), want %d", code, err, c.code)
			}
		})
	}
}

// --json prints one versioned document with the same facts.
func TestCampaignResultJSON(t *testing.T) {
	for _, c := range []struct {
		fixture  string
		accepted bool
		commit   string
		verdict  string
		class    string
	}{
		{"accepted", true, "1111111111111111111111111111111111111111", "ACCEPT", ""},
		{"changes-requested", false, "", "CHANGES_REQUESTED", ""},
		{"failed", false, "", "", "verification"},
		{"pending", false, "", "", ""},
	} {
		t.Run(c.fixture, func(t *testing.T) {
			fixture := loadCampaignResultFixture(t, c.fixture)
			var out bytes.Buffer
			_ = fixture.cli(&out).run(context.Background(), []string{"result", "--json", fixture.Workflow.Summary.Run.ID})
			decoder := json.NewDecoder(&out)
			decoder.DisallowUnknownFields()
			var document campaignResultDocument
			if err := decoder.Decode(&document); err != nil {
				t.Fatal(err)
			}
			if decoder.More() {
				t.Fatal("more than one document was printed")
			}
			if document.SchemaVersion != "t3-steward.campaign-result/v1" || document.Run != fixture.Workflow.Summary.Run.ID ||
				document.State != string(fixture.Workflow.Summary.Run.Progress) || len(document.Tasks) != 2 {
				t.Fatalf("document = %+v", document)
			}
			if document.Acceptance.Accepted != c.accepted || document.Acceptance.Commit != c.commit || document.Acceptance.Reason == "" {
				t.Fatalf("acceptance = %+v", document.Acceptance)
			}
			implement, review := document.Tasks[0], document.Tasks[1]
			if implement.Task != "implement" || review.Task != "review" {
				t.Fatalf("tasks are not in manifest order: %+v", document.Tasks)
			}
			if implement.FailureClass != c.class {
				t.Fatalf("failure class = %q, want %q", implement.FailureClass, c.class)
			}
			if c.verdict == "" && review.Verdict != nil || c.verdict != "" && (review.Verdict == nil || review.Verdict.Verdict != c.verdict) {
				t.Fatalf("verdict = %+v", review.Verdict)
			}
		})
	}
	// The JSON of the accepted run names the accepting and the verified task
	// and the declared commit of the latest attempt only.
	fixture := loadCampaignResultFixture(t, "accepted")
	document := buildCampaignResult(context.Background(), fixture.Workflow, fixture.open)
	if a := document.Acceptance; a.ReviewTask != "review" || a.VerifiedTask != "implement" {
		t.Fatalf("acceptance = %+v", a)
	}
	if commits := document.Tasks[0].Commits; len(commits) != 1 || commits[0].Name != "implementation" ||
		commits[0].Commit != "1111111111111111111111111111111111111111" {
		t.Fatalf("commits = %+v", commits)
	}
	if v := document.Tasks[0].Verification; v.State != "passed" || v.Passed != 2 || v.Declared != 2 {
		t.Fatalf("verification = %+v", v)
	}
}

// The acceptance line is computed from recorded verdict and verification
// facts on one commit. A run in which every task succeeded, and so every exit
// code was 0, is not accepted when any of those facts is absent or names
// another commit.
func TestCampaignResultAcceptanceNeedsRecordedFactsOnOneCommit(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name   string
		change func(*campaignResultFixture)
		reason string
	}{
		{"no recorded verdict, only a review.md that says ACCEPT", func(f *campaignResultFixture) {
			f.Workflow.Tasks[0].Attempt.ReviewVerdict = nil
			f.Workflow.Tasks[1].Attempt.ReviewGate = nil
		}, "no review verdict is recorded"},
		{"ordinary reports have no commit binding", func(f *campaignResultFixture) {
			f.Workflow.Tasks[1].Attempt.ReviewGate = nil
		}, "verification of implement is not bound to declared commit"},
		{"producer gate names another commit", func(f *campaignResultFixture) {
			f.Workflow.Tasks[1].Attempt.ReviewGate.ReviewedHead = "2222222222222222222222222222222222222222"
		}, "verification of implement is not bound to declared commit"},
		{"producer gate failed", func(f *campaignResultFixture) {
			f.Workflow.Tasks[1].Attempt.ReviewGate.Passed = false
		}, "verification of implement is not bound to declared commit"},
		{"producer gate did not accept the head", func(f *campaignResultFixture) {
			f.Workflow.Tasks[1].Attempt.ReviewGate.Code = domain.ReviewGateNotAccepted
		}, "verification of implement is not bound to declared commit"},
		{"no verification declared", func(f *campaignResultFixture) {
			f.Workflow.Tasks[1].Task.Verification = nil
		}, "verification of implement is not-declared"},
		{"a verification report was not retained", func(f *campaignResultFixture) {
			f.Workflow.Tasks[1].Artifacts = f.Workflow.Tasks[1].Artifacts[:4]
		}, "verification of implement is missing"},
		{"a verification report is unreadable", func(f *campaignResultFixture) {
			f.Artifacts["art-verify-1"] = json.RawMessage(`{}`)
		}, "verification of implement is unavailable"},
		{"a verification command failed", func(f *campaignResultFixture) {
			f.Artifacts["art-verify-1"] = json.RawMessage(`{"command":"make test","exitCode":1}`)
		}, "verification of implement is failed"},
		{"the commit record is unreadable", func(f *campaignResultFixture) {
			delete(f.Artifacts, "art-commit")
		}, "the commit implementation of implement review accepted is unreadable"},
		{"the commit record is of a failed attempt", func(f *campaignResultFixture) {
			f.Artifacts["art-commit"] = json.RawMessage(`{"version":"campaign-commit/v1","workflowRunId":"run-accepted","taskId":"t-implement","name":"implementation","repository":"r","base":"0000000000000000000000000000000000000000","commit":"1111111111111111111111111111111111111111","ref":"refs/campaigns-quarantine/run-accepted/t-implement/a-implement-2/implementation","failedAttempt":{"id":"a-implement-2","verificationFailures":["verification command failed (1): make test"]}}`)
		}, "unreadable"},
		{"the review consumed no commit", func(f *campaignResultFixture) {
			f.Workflow.Tasks[0].Task.DependencyInputs = nil
			f.Workflow.Tasks[1].Attempt.ReviewGate = nil
		}, "review recorded ACCEPT but consumed no recorded commit"},
		{"the producer's latest attempt has no commit record", func(f *campaignResultFixture) {
			f.Workflow.Tasks[1].Artifacts = append(f.Workflow.Tasks[1].Artifacts[:2], f.Workflow.Tasks[1].Artifacts[3:]...)
		}, "no commit record was retained"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fixture := loadCampaignResultFixture(t, "accepted")
			c.change(&fixture)
			document := buildCampaignResult(ctx, fixture.Workflow, fixture.open)
			if document.Acceptance.Accepted || !strings.Contains(document.Acceptance.Reason, c.reason) {
				t.Fatalf("acceptance = %+v, want no with %q", document.Acceptance, c.reason)
			}
			for _, task := range document.Tasks {
				if task.State != "succeeded" {
					t.Fatalf("the change also failed a task: %+v", task)
				}
			}
		})
	}
	// A second review that asks for changes on the same commit outweighs an
	// ACCEPT.
	fixture := loadCampaignResultFixture(t, "accepted")
	second := fixture.Workflow.Tasks[0]
	second.Task.ID, second.Task.Name = "t-review-2", "review-2"
	attempt := *second.Attempt
	attempt.ReviewVerdict = &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 1}
	second.Attempt = &attempt
	fixture.Workflow.Tasks = append(fixture.Workflow.Tasks, second)
	document := buildCampaignResult(ctx, fixture.Workflow, fixture.open)
	if document.Acceptance.Accepted || !strings.Contains(document.Acceptance.Reason, "review-2 also recorded changes requested on 111111111111") {
		t.Fatalf("acceptance = %+v", document.Acceptance)
	}
	// Without an artifact reader nothing can be confirmed.
	fixture = loadCampaignResultFixture(t, "accepted")
	if document := buildCampaignResult(ctx, fixture.Workflow, nil); document.Acceptance.Accepted {
		t.Fatalf("accepted without reading a record: %+v", document.Acceptance)
	}
}

// A review-declared task binds its accepted review round, its clean workspace
// and its declared commit to one head through the review gate: that is a
// recorded ACCEPT on the commit, and the task's own verification is checked
// on it. A gate whose reviewed head is not the recorded commit is not.
func TestCampaignResultReviewGateBindsTheCommit(t *testing.T) {
	ctx := context.Background()
	fixture := loadCampaignResultFixture(t, "accepted")
	fixture.Workflow.Tasks = fixture.Workflow.Tasks[1:]
	fixture.Workflow.Summary.Workflow.TaskIDs = []string{"t-implement"}
	implement := &fixture.Workflow.Tasks[0]
	implement.Task.ReviewRequirements = &domain.TaskReviewRequirements{}
	gate := &domain.ReviewCompletionGate{Passed: true, Code: domain.ReviewGateAccepted, RoundVerdict: "accept",
		ReviewedHead: "1111111111111111111111111111111111111111"}
	implement.Attempt.ReviewGate = gate
	document := buildCampaignResult(ctx, fixture.Workflow, fixture.open)
	if a := document.Acceptance; !a.Accepted || a.Commit != gate.ReviewedHead || a.ReviewTask != "implement" || a.VerifiedTask != "implement" {
		t.Fatalf("acceptance = %+v", a)
	}
	var out bytes.Buffer
	renderCampaignResult(&out, document)
	if !strings.Contains(out.String(), "review gate accepted-head") ||
		!strings.Contains(out.String(), "yes, 111111111111 (implement review gate accepted-head, verification passed)") {
		t.Fatalf("output:\n%s", out.String())
	}

	gate.ReviewedHead = "2222222222222222222222222222222222222222"
	document = buildCampaignResult(ctx, fixture.Workflow, fixture.open)
	if a := document.Acceptance; a.Accepted || !strings.Contains(a.Reason, "reviewed head 222222222222 is not its recorded declared commit") {
		t.Fatalf("acceptance = %+v", a)
	}
	gate.ReviewedHead, gate.Passed, gate.Code = "1111111111111111111111111111111111111111", false, domain.ReviewGateNotAccepted
	document = buildCampaignResult(ctx, fixture.Workflow, fixture.open)
	if a := document.Acceptance; a.Accepted || !strings.Contains(a.Reason, "implement review gate review-not-accepted") {
		t.Fatalf("acceptance = %+v", a)
	}
}

// --wait blocks until the run is terminal and then prints the result;
// --timeout bounds it and still prints the last state seen.
func TestCampaignResultWait(t *testing.T) {
	pending := loadCampaignResultFixture(t, "pending")
	accepted := loadCampaignResultFixture(t, "accepted")
	calls := 0
	var out bytes.Buffer
	cli := campaignCLI{
		stdout: &out, stderr: io.Discard,
		detail: func(context.Context, string) (backlogadmin.WorkflowDetail, error) {
			calls++
			if calls < 3 {
				return pending.Workflow, nil
			}
			return accepted.Workflow, nil
		},
		openArtifact: accepted.open,
	}
	if err := cli.run(context.Background(), []string{"result", "run-accepted", "--wait", "--timeout", "30s"}); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || !strings.Contains(out.String(), "same commit: yes") {
		t.Fatalf("calls %d, output:\n%s", calls, out.String())
	}

	out.Reset()
	cli.detail = func(context.Context, string) (backlogadmin.WorkflowDetail, error) { return pending.Workflow, nil }
	cli.openArtifact = pending.open
	started := time.Now()
	err := cli.run(context.Background(), []string{"result", "run-pending", "--wait", "--timeout", "300ms"})
	if exitCodeFor(err) != 1 || !strings.Contains(out.String(), "run run-pending: active") || time.Since(started) > 10*time.Second {
		t.Fatalf("timeout: %v, output:\n%s", err, out.String())
	}
}

// Malformed command lines are refused before the coordinator is asked.
func TestCampaignResultArguments(t *testing.T) {
	fixture := loadCampaignResultFixture(t, "accepted")
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"result"}, "needs a run"},
		{[]string{"result", "a", "b"}, "one run"},
		{[]string{"result", "run-accepted/review"}, "task result run-accepted/review"},
		{[]string{"result", "run-accepted", "--output", "x"}, "unknown campaign result flag"},
		{[]string{"result", "run-accepted", "--json", "--json"}, "duplicate --json"},
		{[]string{"result", "run-accepted", "--timeout", "1m"}, "--timeout requires --wait"},
		{[]string{"resutl", "run-accepted"}, `did you mean "result"`},
	} {
		err := fixture.cli(io.Discard).run(context.Background(), c.args)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%v: %v, want %q", c.args, err, c.want)
		}
	}
}

// The failure class is read from the recorded failure's stable prefix.
func TestCampaignFailureClass(t *testing.T) {
	for failure, want := range map[string]string{
		"verification command failed (2): make test":           "verification",
		"gate command failed (1): make check":                  "gate",
		"review gate review-not-accepted: round 2":             "review-gate",
		"missing declared output: handoff.md (the turn ended)": "missing-output",
		"terminal dependency prevented execution: x failed":    "dependency",
		"T3 refused to start the provider turn":                "infrastructure",
		"something new":                                        "other",
	} {
		if got := campaignFailureClass(domain.ProgressFailed, failure); got != want {
			t.Fatalf("%q: %q, want %q", failure, got, want)
		}
	}
	if got := campaignFailureClass(domain.ProgressCancelled, ""); got != "cancelled" {
		t.Fatalf("cancelled: %q", got)
	}
	if got := campaignFailureClass(domain.ProgressSucceeded, "verification command failed"); got != "" {
		t.Fatalf("succeeded: %q", got)
	}
}
