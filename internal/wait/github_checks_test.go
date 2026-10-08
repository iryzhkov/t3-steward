package wait

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const checksSHA = "0123456789abcdef0123456789abcdef01234567"

// commitChecksAnswer is gh api graphql's answer for a commit with the given
// rollup state and contexts.
func commitChecksAnswer(state string, total int, nodes string) string {
	return `{"data":{"repository":{"object":{"oid":"` + checksSHA + `","url":"https://github.com/o/r/commit/` + checksSHA +
		`","statusCheckRollup":{"state":"` + state + `","contexts":{"totalCount":` + strconv.Itoa(total) + `,"nodes":[` + nodes + `]}}}}}}`
}

const (
	lintFailed  = `{"__typename":"CheckRun","name":"lint","status":"COMPLETED","conclusion":"FAILURE"}`
	testPassed  = `{"__typename":"CheckRun","name":"test","status":"COMPLETED","conclusion":"SUCCESS"}`
	buildActive = `{"__typename":"CheckRun","name":"build","status":"IN_PROGRESS","conclusion":null}`
	docsSkipped = `{"__typename":"CheckRun","name":"docs","status":"COMPLETED","conclusion":"SKIPPED"}`
	statusOK    = `{"__typename":"StatusContext","context":"ci/legacy","state":"SUCCESS"}`
	statusWait  = `{"__typename":"StatusContext","context":"ci/legacy","state":"PENDING"}`
)

// A commit's checks settle only once every check run and status finished:
// met when none failed, failed when one did, waiting otherwise, and the
// reason is always the one line counting the conclusions.
func TestCommitChecksMapping(t *testing.T) {
	target := GitHubTarget{Kind: "commit", ID: checksSHA, State: ChecksCompleted, Repo: "o/r"}
	cases := []struct {
		name   string
		answer string
		want   Status
		reason string
		fields map[string]string
	}{
		{"all passed", commitChecksAnswer("SUCCESS", 3, testPassed+","+docsSkipped+","+statusOK), StatusMet,
			"checks passed: 3 checks, 1 skipped, 2 success",
			map[string]string{"conclusion": "success", "checks": "skipped=1,success=2", "target": "commit:" + checksSHA}},
		{"one failed", commitChecksAnswer("FAILURE", 2, lintFailed+","+testPassed), StatusFailed,
			"checks failed: 2 checks, 1 failure (lint), 1 success",
			map[string]string{"conclusion": "failure", "checks": "failure=1,success=1"}},
		{"a failure does not settle while another check runs", commitChecksAnswer("PENDING", 3, lintFailed+","+buildActive+","+statusWait), StatusWaiting,
			"checks pending: 3 checks, 1 failure (lint), 2 pending (build, ci/legacy)", nil},
		{"no checks yet", `{"data":{"repository":{"object":{"oid":"` + checksSHA + `","url":"u","statusCheckRollup":null}}}}`, StatusWaiting,
			"checks pending: no checks reported yet", nil},
		{"unlisted checks are judged by the rollup", commitChecksAnswer("FAILURE", 150, testPassed), StatusFailed,
			"checks failed: 150 checks, 1 success, 149 not listed, rollup failure", nil},
		{"unlisted pending checks wait", commitChecksAnswer("PENDING", 150, testPassed), StatusWaiting, "", nil},
	}
	for _, c := range cases {
		reading, err := EvaluateGitHub(target, []byte(c.answer))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if reading.Status != c.want {
			t.Fatalf("%s: status %s, want %s (%s)", c.name, reading.Status, c.want, reading.Reason)
		}
		if c.reason != "" && reading.Reason != c.reason {
			t.Fatalf("%s: reason %q, want %q", c.name, reading.Reason, c.reason)
		}
		for k, v := range c.fields {
			if reading.Fields[k] != v {
				t.Fatalf("%s: field %s = %q, want %q (%v)", c.name, k, reading.Fields[k], v, reading.Fields)
			}
		}
	}
}

// A commit or repository GitHub does not know is an error the poll gives up
// on at once, and a GraphQL error is an error rather than a pending reading.
func TestCommitChecksMissingTargets(t *testing.T) {
	target := GitHubTarget{Kind: "commit", ID: "abcdef1", State: ChecksCompleted, Repo: "o/r"}
	for answer, want := range map[string]string{
		`{"data":{"repository":{"object":null}}}`:                     "commit abcdef1 not found in o/r",
		`{"data":{"repository":null}}`:                                "repository o/r not found",
		`{"data":null,"errors":[{"message":"Something went wrong"}]}`: "gh api graphql: Something went wrong",
		`not json`: "something other than the requested JSON",
	} {
		_, err := EvaluateGitHub(target, []byte(answer))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err %v, want %q", answer, err, want)
		}
	}
	if !gitHubGonePattern.MatchString("commit abcdef1 not found in o/r") || !gitHubGonePattern.MatchString("repository o/r not found") {
		t.Fatal("a missing commit or repository would be polled three times")
	}
}

// The commit target is read by one fixed gh api graphql call whose variables
// are all strings, and is refused without a repository or a hexadecimal id.
func TestCommitChecksTargetArgumentsAndValidation(t *testing.T) {
	target := GitHubTarget{Kind: "commit", ID: "1234567", State: ChecksCompleted, Repo: "owner/name"}
	if err := target.Validate(); err != nil {
		t.Fatal(err)
	}
	args := target.Args()
	if len(args) != 10 || args[0] != "api" || args[1] != "graphql" || !strings.HasPrefix(args[3], "query=query(") ||
		strings.Join(args[4:], " ") != "-f owner=owner -f name=name -f expression=1234567" {
		t.Fatalf("args = %q", args)
	}
	for _, arg := range args {
		if arg == "-F" || arg == "--repo" {
			t.Fatalf("args carry %s: %q", arg, args)
		}
	}
	for _, bad := range []GitHubTarget{
		{Kind: "commit", ID: "1234567", State: ChecksCompleted},
		{Kind: "commit", ID: "xyz1234", State: ChecksCompleted, Repo: "o/r"},
		{Kind: "commit", ID: "123456", State: ChecksCompleted, Repo: "o/r"},
		{Kind: "commit", ID: "1234567", State: "merged", Repo: "o/r"},
		{Kind: "run", ID: "1", State: ChecksCompleted},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("%+v was accepted", bad)
		}
	}
}

// A pull request's checks-completed reads the same rollup gh pr view returns
// and, unlike checks-passed, keeps waiting past the first failure.
func TestPullRequestChecksCompleted(t *testing.T) {
	target := GitHubTarget{Kind: "pr", ID: "7", State: ChecksCompleted}
	pending := `{"state":"OPEN","statusCheckRollup":[` + lintFailed + `,` + buildActive + `]}`
	if reading, err := EvaluateGitHub(target, []byte(pending)); err != nil || reading.Status != StatusWaiting {
		t.Fatalf("pending = %+v %v", reading, err)
	}
	if reading, _ := EvaluateGitHub(GitHubTarget{Kind: "pr", ID: "7", State: "checks-passed"}, []byte(pending)); reading.Status != StatusFailed {
		t.Fatalf("checks-passed no longer fails early: %+v", reading)
	}
	done := `{"state":"OPEN","statusCheckRollup":[` + lintFailed + `,` + testPassed + `]}`
	reading, err := EvaluateGitHub(target, []byte(done))
	if err != nil || reading.Status != StatusFailed || reading.Reason != "checks failed: 2 checks, 1 failure (lint), 1 success" {
		t.Fatalf("done = %+v %v", reading, err)
	}
	closed := `{"state":"CLOSED","statusCheckRollup":[` + buildActive + `]}`
	if reading, _ := EvaluateGitHub(target, []byte(closed)); reading.Status != StatusFailed || !strings.Contains(reading.Reason, "closed without merging") {
		t.Fatalf("closed = %+v", reading)
	}
}

// Check names come from the remote: they are clipped, kept to one line and
// cannot forge the separators of the summary, and a long list is capped.
func TestChecksSummaryNamesAreBounded(t *testing.T) {
	var checks []gitHubCheck
	for _, name := range []string{"a\nb", "x, (y)", strings.Repeat("n", 100), "d", "e"} {
		checks = append(checks, gitHubCheck{Name: name, Status: "COMPLETED", Conclusion: "FAILURE"})
	}
	s := summarizeChecks(checks, len(checks), "")
	if strings.ContainsAny(s.Line, "\n\r") || !strings.Contains(s.Line, "(a b, x y, "+strings.Repeat("n", 40)+", +2 more)") {
		t.Fatalf("line = %q", s.Line)
	}
}

// A registered commit checks wait polls with the fake gh, stays waiting while
// a check runs and wakes failed with the summary as its reason and the counts
// in the trailer; nothing reaches the network.
func TestCommitChecksWaitWakesWithTheSummary(t *testing.T) {
	answer := commitChecksAnswer("PENDING", 2, lintFailed+","+buildActive)
	var calls [][]string
	runner, store, control, now := gitHubRunner(t, func(_ context.Context, _ string, args []string) (string, error) {
		calls = append(calls, args)
		return answer, nil
	})
	ctx := context.Background()
	w := store.waits["w1"]
	w.GitHub = &GitHubTarget{Kind: "commit", ID: checksSHA, State: ChecksCompleted, Repo: "o/r"}
	_ = store.SaveWait(ctx, w)
	runner.Tick(ctx, nil, nil)
	if w := store.waits["w1"]; w.Status != StatusWaiting || !strings.HasPrefix(w.LastOutput, "checks pending:") {
		t.Fatalf("a running check settled the wait: %+v", w)
	}
	answer = commitChecksAnswer("FAILURE", 2, lintFailed+","+testPassed)
	*now = now.Add(2 * time.Minute)
	runner.Tick(ctx, nil, nil)
	w = store.waits["w1"]
	if w.Outcome != "failed" || w.Reason != "checks failed: 2 checks, 1 failure (lint), 1 success" || w.LastOutput != w.Reason {
		t.Fatalf("settled = %+v", w)
	}
	if len(calls) != 2 || calls[0][0] != "api" {
		t.Fatalf("gh calls = %v", calls)
	}
	if len(control.texts) != 1 {
		t.Fatalf("texts = %v", control.texts)
	}
	parsed, ok := ParseWakeTrailer(control.texts[0])
	if !ok || parsed["kind"] != string(domain.WaitKindGitHub) || parsed["outcome"] != "failed" || parsed["target"] != "commit:"+checksSHA ||
		parsed["conclusion"] != "failure" || parsed["checks"] != "failure=1,success=1" {
		t.Fatalf("trailer = %v", parsed)
	}
	if !strings.Contains(control.texts[0], "checks failed: 2 checks, 1 failure (lint), 1 success") {
		t.Fatalf("the wake does not carry the summary line: %q", control.texts[0])
	}
}
