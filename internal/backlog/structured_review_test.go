package backlog

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStructuredReviewParse(t *testing.T) {
	for _, synonym := range []string{"ACCEPT", "ACCEPTED", "APPROVE", "accept", "CHANGES_REQUESTED", "CHANGES REQUESTED", "REQUEST_CHANGES", "REJECT", "changes-requested"} {
		want := "accept"
		if strings.Contains(synonym, "CHANGE") || synonym == "REJECT" || synonym == "changes-requested" {
			want = "changes-requested"
		}
		for _, line := range []bool{false, true} {
			decl := domain.ReviewOutput{Verdict: "verdict.json"}
			raw := []byte(fmt.Sprintf("{\"verdict\":%q,\"blocking_findings\":2,\"finding_titles\":[\"one\",\"two\"]}", synonym))
			if line {
				decl = domain.ReviewOutput{VerdictLine: "review.md"}
				raw = []byte(synonym + "\nbody that must not be copied")
			}
			got, err := ParseReviewVerdict(decl, raw)
			if err != nil || got.Verdict != want {
				t.Fatalf("%s line=%v: %+v %v", synonym, line, got, err)
			}
		}
	}
	for _, raw := range []string{"", "{}", "null", "{", `{"verdict":"ACCEPT","blocking_findings":null}`, `{"verdict":"ACCEPT","finding_titles":[null]}`, `{"verdict":"REJECT","verdict":"ACCEPT"}`, "{\"verdict\":\"maybe\"}", "{\"verdict\":\"accept\",\"blocking_findings\":-1}", "{\"verdict\":\"accept\",\"blocking_findings\":1.5}", "{\"verdict\":\"accept\"} {}"} {
		if _, err := ParseReviewVerdict(domain.ReviewOutput{Verdict: "v.json"}, []byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	for _, raw := range []string{"", "\nACCEPT", "ACCEPT later", "maybe", strings.Repeat("a", MaxReviewVerdictBytes+1)} {
		if _, err := ParseReviewVerdict(domain.ReviewOutput{VerdictLine: "v.md"}, []byte(raw)); err == nil {
			t.Fatalf("accepted bad line length %d", len(raw))
		}
	}
	raw := []byte("{\"verdict\":\"reject\",\"finding_titles\":[\"" + strings.Repeat("x", 400) + "\",\"2\",\"3\",\"4\",\"5\",\"6\"]}")
	got, err := ParseReviewVerdict(domain.ReviewOutput{Verdict: "v.json"}, raw)
	if err != nil || len(got.FindingTitles) != 5 || len(got.FindingTitles[0]) > 160 {
		t.Fatalf("unbounded: %+v %v", got, err)
	}
}

// A verdict line may carry one leading VERDICT: label, the form review
// prompts ask for. Everything after the label obeys the bare-word rule, and
// the JSON verdict field never takes the label.
func TestStructuredReviewVerdictLabel(t *testing.T) {
	line := domain.ReviewOutput{VerdictLine: "review.md"}
	for raw, want := range map[string]string{
		"VERDICT: ACCEPT\n\nreviewed abc":     "accept",
		"VERDICT: CHANGES_REQUESTED\nbody":    "changes-requested",
		"verdict: approve":                    "accept",
		"Verdict:\tReject\r\n":                "changes-requested",
		"VERDICT:ACCEPTED":                    "accept",
		"  VERDICT:  request-changes  \nbody": "changes-requested",
		"VERDICT: changes requested":          "changes-requested",
	} {
		got, err := ParseReviewVerdict(line, []byte(raw))
		if err != nil || got.Verdict != want {
			t.Fatalf("%q: %+v %v, want %s", raw, got, err, want)
		}
	}
	for _, raw := range []string{
		"VERDICT ACCEPT", "VERDICT:", "VERDICT: ", "VERDICT: ACCEPT later", "VERDICT: VERDICT: ACCEPT",
		"**VERDICT: ACCEPT**", "VERDICT: maybe", "\nVERDICT: ACCEPT", "VERDICT: **ACCEPT**", "VERDICT - ACCEPT",
		"VERDICT:: ACCEPT", "\xef\xbb\xbfVERDICT: ACCEPT", "# VERDICT: ACCEPT", "VERDICT: CHANGES  REQUESTED",
		// Only spaces and tabs may follow the label.
		"VERDICT:\xc2\xa0ACCEPT", "VERDICT:\xe2\x80\x83ACCEPT", "VERDICT:\vACCEPT", "VERDICT:\fACCEPT", "VERDICT: \rACCEPT",
	} {
		if got, err := ParseReviewVerdict(line, []byte(raw)); err == nil {
			t.Fatalf("accepted %q as %+v", raw, got)
		} else if !strings.Contains(err.Error(), "VERDICT:") {
			t.Fatalf("%q: the line-form refusal %q does not name the labelled form", raw, err)
		}
	}
	for _, verdict := range []string{"VERDICT: ACCEPT", "verdict: REJECT"} {
		raw := []byte(fmt.Sprintf("{\"verdict\":%q}", verdict))
		if got, err := ParseReviewVerdict(domain.ReviewOutput{Verdict: "verdict.json"}, raw); err == nil {
			t.Fatalf("JSON verdict %q accepted as %+v", verdict, got)
		} else if strings.Contains(err.Error(), "VERDICT:") {
			t.Fatalf("the JSON refusal %q names the line form", err)
		}
	}
}

// A labelled verdict line goes through the coordinator's result import: a
// review asking for changes still succeeds and records its verdict, durably
// across a restart, and an unknown verdict fails verification.
func TestStructuredReviewVerdictLineImport(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      domain.ProgressState
		verdict   string
	}{
		{"changes requested", "VERDICT: CHANGES_REQUESTED\n\nP1 internal/x.go:1 lost evidence\n", domain.ProgressSucceeded, "changes-requested"},
		{"accept", "VERDICT: ACCEPT\n", domain.ProgressSucceeded, "accept"},
		{"unknown", "VERDICT: maybe\n", domain.ProgressFailed, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			now := coordinatorTestTime
			dbPath := filepath.Join(t.TempDir(), "state.db")
			s, err := sqlite.OpenMigrated(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if s != nil {
					s.Close()
				}
			}()
			task := testTask("task")
			task.ReviewOutput = &domain.ReviewOutput{VerdictLine: "review.md"}
			task.Outputs = []domain.ArtifactDeclaration{{Name: "review.md", MediaType: "text/markdown"}}
			attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
			assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
			if err := s.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			data := resultUploadOpener{"review": []byte(tc.raw), "final-message-attempt-1": []byte("review finished"), "thread-archive-attempt-1": []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)}
			objects := []workerproto.ArtifactObject{
				resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
				resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"]),
				resultObject("review", "results/review.md", "output", "text/markdown", data["review"]),
			}
			manifest := resultManifest(now, assignment, objects)
			response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
			importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: s, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: s}, MaxArtifactBytes: 4096, MaxTotalBytes: 16384, Now: func() time.Time { return now.Add(time.Minute) }}
			report, err := importer.Import(ctx, response, data)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != tc.want {
				t.Fatalf("report: %+v", report)
			}
			if failure := report.Transition[0].Attempt.Failure; (tc.want == domain.ProgressFailed) != strings.Contains(failure, "review_output verification failed") {
				t.Fatalf("failure: %q", failure)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = nil
			s, err = sqlite.OpenMigrated(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			records, err := s.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got := records.Attempts[0]
			if got.Progress != tc.want {
				t.Fatalf("restart progress: %+v", got)
			}
			if tc.verdict == "" {
				if got.ReviewVerdict != nil {
					t.Fatalf("a refused verdict was recorded: %+v", got.ReviewVerdict)
				}
				return
			}
			if got.ReviewVerdict == nil || got.ReviewVerdict.Verdict != tc.verdict {
				t.Fatalf("recorded verdict %+v, want %s", got.ReviewVerdict, tc.verdict)
			}
			obs, err := domain.ResolveNodeState(domain.NodeRef{RunID: "run-1", TaskID: task.ID}, domain.NodeStateTerminal, records.WorkflowRuns, records.Tasks, records.Attempts, records.Assignments, nil)
			if err != nil || obs.ExitCode != 0 || obs.Fields["review"] != tc.verdict {
				t.Fatalf("node: %+v %v", obs, err)
			}
		})
	}
}

func TestStructuredReviewInvalidUTF8(t *testing.T) {
	raw := append([]byte("{\"verdict\":\"ACCEPT\",\"finding_titles\":[\""), 0xff)
	raw = append(raw, []byte("\"]}")...)
	if _, err := ParseReviewVerdict(domain.ReviewOutput{Verdict: "v.json"}, raw); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestStructuredReviewDeclaration(t *testing.T) {
	base := "version: 2\nname: review-test\nenvironment:\n  project: test\ntasks:\n  review:\n    prompt_file: prompt.md\n    outputs: [verdict.json, review.md]\n"
	for _, decl := range []string{"{verdict: verdict.json}", "{verdict_line: review.md}"} {
		m, err := ParseManifest([]byte(base + "    review_output: " + decl + "\n"))
		if err != nil || m.Tasks["review"].ReviewOutput == nil {
			t.Fatalf("%s: %v", decl, err)
		}
	}
	for _, decl := range []string{"{}", "{verdict: ../bad}", "{verdict: absent.json}", "{verdict: verdict.json, verdict_line: review.md}", "{unknown: review.md}"} {
		if _, err := ParseManifest([]byte(base + "    review_output: " + decl + "\n")); err == nil {
			t.Fatalf("accepted %s", decl)
		}
	}
}

func TestStructuredReviewMissingAndLegacy(t *testing.T) {
	task := domain.Task{ReviewOutput: &domain.ReviewOutput{Verdict: "v.json"}}
	if _, err := reviewVerdictFromResult(task, nil, nil); err == nil {
		t.Fatal("missing verdict accepted")
	}
	task.ReviewOutput = nil
	if got, err := reviewVerdictFromResult(task, nil, nil); err != nil || got != nil {
		t.Fatalf("legacy: %+v %v", got, err)
	}
}
