package wait

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Every row of the contract's mapping, from gh's JSON to an outcome.
func TestGitHubMappingRows(t *testing.T) {
	cases := []struct {
		name   string
		target GitHubTarget
		json   string
		want   Status
		fields map[string]string
	}{
		{"run completed success", GitHubTarget{Kind: "run", ID: "1", State: "completed"}, `{"status":"completed","conclusion":"success","url":"https://x/1"}`, StatusMet, map[string]string{"target": "run:1", "state": "completed", "conclusion": "success", "url": "https://x/1"}},
		{"run completed failure", GitHubTarget{Kind: "run", ID: "1", State: "completed"}, `{"status":"completed","conclusion":"failure","url":"https://x/1"}`, StatusFailed, map[string]string{"conclusion": "failure"}},
		{"run completed cancelled", GitHubTarget{Kind: "run", ID: "1", State: "completed"}, `{"status":"completed","conclusion":"cancelled"}`, StatusFailed, nil},
		{"run queued", GitHubTarget{Kind: "run", ID: "1", State: "completed"}, `{"status":"queued","conclusion":""}`, StatusWaiting, nil},
		{"run in progress", GitHubTarget{Kind: "run", ID: "1", State: "completed"}, `{"status":"in_progress"}`, StatusWaiting, nil},
		{"pr merged", GitHubTarget{Kind: "pr", ID: "7", State: "merged"}, `{"state":"MERGED","mergedAt":"2030-01-01T00:00:00Z","url":"https://x/pull/7"}`, StatusMet, map[string]string{"target": "pr:7", "state": "merged", "url": "https://x/pull/7"}},
		{"pr closed unmerged", GitHubTarget{Kind: "pr", ID: "7", State: "merged"}, `{"state":"CLOSED","mergedAt":null}`, StatusFailed, map[string]string{"state": "closed"}},
		{"pr open", GitHubTarget{Kind: "pr", ID: "7", State: "merged"}, `{"state":"OPEN"}`, StatusWaiting, nil},
		{"checks all passed", GitHubTarget{Kind: "pr", ID: "7", State: "checks-passed"}, `{"state":"OPEN","statusCheckRollup":[{"conclusion":"SUCCESS","status":"COMPLETED"},{"state":"SUCCESS"},{"conclusion":"SKIPPED","status":"COMPLETED"}]}`, StatusMet, map[string]string{"conclusion": "success"}},
		{"checks one failed", GitHubTarget{Kind: "pr", ID: "7", State: "checks-passed"}, `{"state":"OPEN","statusCheckRollup":[{"conclusion":"SUCCESS","status":"COMPLETED"},{"conclusion":"FAILURE","status":"COMPLETED"}]}`, StatusFailed, map[string]string{"conclusion": "failure"}},
		{"checks pending", GitHubTarget{Kind: "pr", ID: "7", State: "checks-passed"}, `{"state":"OPEN","statusCheckRollup":[{"conclusion":"SUCCESS","status":"COMPLETED"},{"conclusion":"","status":"IN_PROGRESS"}]}`, StatusWaiting, nil},
		{"checks none yet", GitHubTarget{Kind: "pr", ID: "7", State: "checks-passed"}, `{"state":"OPEN","statusCheckRollup":[]}`, StatusWaiting, nil},
		{"reviewed approved", GitHubTarget{Kind: "pr", ID: "7", State: "reviewed"}, `{"state":"OPEN","reviewDecision":"APPROVED"}`, StatusMet, map[string]string{"state": "approved"}},
		{"reviewed changes requested", GitHubTarget{Kind: "pr", ID: "7", State: "reviewed"}, `{"state":"OPEN","reviewDecision":"CHANGES_REQUESTED"}`, StatusMet, map[string]string{"state": "changes_requested"}},
		{"reviewed not yet", GitHubTarget{Kind: "pr", ID: "7", State: "reviewed"}, `{"state":"OPEN","reviewDecision":"REVIEW_REQUIRED"}`, StatusWaiting, nil},
		{"reviewed closed first", GitHubTarget{Kind: "pr", ID: "7", State: "reviewed"}, `{"state":"CLOSED","reviewDecision":""}`, StatusFailed, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reading, err := EvaluateGitHub(c.target, []byte(c.json))
			if err != nil {
				t.Fatal(err)
			}
			if reading.Status != c.want {
				t.Fatalf("status = %q (%s), want %q", reading.Status, reading.Reason, c.want)
			}
			for key, want := range c.fields {
				if reading.Fields[key] != want {
					t.Fatalf("%s = %q, want %q (fields %v)", key, reading.Fields[key], want, reading.Fields)
				}
			}
		})
	}
	if _, err := EvaluateGitHub(GitHubTarget{Kind: "run", ID: "1", State: "completed"}, []byte("not json")); err == nil {
		t.Fatal("unparseable gh output was accepted")
	}
}

// gh runs with fixed arguments: the target decides them, nothing else.
func TestGitHubTargetArgumentsAreFixed(t *testing.T) {
	run := GitHubTarget{Kind: "run", ID: "123", State: "completed"}
	if got := strings.Join(run.Args(), " "); got != "run view 123 --json status,conclusion,url" {
		t.Fatalf("run args = %q", got)
	}
	pr := GitHubTarget{Kind: "pr", ID: "45", State: "merged", Repo: "o/r"}
	if got := strings.Join(pr.Args(), " "); got != "pr view 45 --json state,mergedAt,reviewDecision,statusCheckRollup,url --repo o/r" {
		t.Fatalf("pr args = %q", got)
	}
	for _, bad := range []GitHubTarget{
		{Kind: "run", ID: "1", State: "merged"},
		{Kind: "pr", ID: "1", State: "completed"},
		{Kind: "issue", ID: "1"},
		{Kind: "run", ID: ""},
		{Kind: "run", ID: "1", State: "completed", Repo: "no-slash"},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("%+v was accepted", bad)
		}
	}
}

// A github poll runs gh in the directory the wait was registered in.
//
// gh resolves the repository from its working directory when the wait names
// none, and the poll runs in the steward daemon, whose working directory is the
// filesystem root under systemd. Such a wait answered "not a git repository"
// three times and gave up, having read its target successfully at registration.
func TestGitHubPollRunsInTheRegisteringDirectory(t *testing.T) {
	var dirs []string
	runner, store, _, _ := gitHubRunner(t, func(_ context.Context, dir string, _ []string) (string, error) {
		dirs = append(dirs, dir)
		return `{"status":"in_progress"}`, nil
	})
	runner.Tick(context.Background(), nil, nil)
	if len(dirs) != 1 || dirs[0] != "/registered/in" {
		t.Fatalf("gh ran in %v, want the wait's own directory", dirs)
	}
	if w := store.waits["w1"]; w.Status != StatusWaiting {
		t.Fatalf("a readable target settled the wait: %+v", w)
	}
}

func gitHubRunner(t *testing.T, gh GitHubRunner) (*Runner, *memStore, *memControl, *time.Time) {
	t.Helper()
	store := &memStore{waits: map[string]Wait{}}
	control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1"}}}
	runner := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	runner.SetClock(func() time.Time { return now })
	runner.GitHub = gh
	runner.Exec = func(context.Context, Wait) (string, int, error) {
		t.Fatal("a github wait ran a shell command")
		return "", 0, nil
	}
	_ = store.SaveWait(context.Background(), Wait{
		ID: "w1", ThreadID: "t1", Name: "ci", Kind: domain.WaitKindGitHub, Dir: "/registered/in",
		GitHub: &GitHubTarget{Kind: "run", ID: "123", State: "completed"},
		Every:  30 * time.Second, MaxEvery: time.Minute, Timeout: time.Hour, Wake: WakeEach,
		Status: StatusWaiting, CreatedAt: now,
	})
	return runner, store, control, &now
}

// Three consecutive gh errors give up with the last error; a success in
// between resets the count. The wake trailer names the target and the
// observed state.
func TestGitHubGivesUpAfterThreeConsecutiveErrorsAndWakesWithTheTrailer(t *testing.T) {
	calls := 0
	answers := []func() (string, error){
		func() (string, error) { return "", errors.New("gh: HTTP 502") },
		func() (string, error) { return "", errors.New("gh: HTTP 502") },
		func() (string, error) { return `{"status":"in_progress"}`, nil },
		func() (string, error) { return "", errors.New("gh: HTTP 502") },
		func() (string, error) { return "", errors.New("gh: HTTP 502") },
		func() (string, error) { return "", errors.New("gh: HTTP 503 the last one") },
	}
	runner, store, control, now := gitHubRunner(t, func(context.Context, string, []string) (string, error) {
		calls++
		return answers[calls-1]()
	})
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		runner.Tick(ctx, nil, nil)
		if w := store.waits["w1"]; w.Status != StatusWaiting {
			t.Fatalf("gave up after %d calls: %+v", calls, w)
		}
		*now = now.Add(5 * time.Minute)
	}
	runner.Tick(ctx, nil, nil)
	w := store.waits["w1"]
	if w.Outcome != "gave-up" || !strings.Contains(w.Reason, "the last one") || calls != 6 {
		t.Fatalf("after six calls: %+v (calls %d)", w, calls)
	}
	if len(control.texts) != 1 || !strings.HasPrefix(control.texts[0], "t3-steward-wait kind=github outcome=gave-up wait=w1") {
		t.Fatalf("wake = %v", control.texts)
	}
	if parsed, _ := ParseWakeTrailer(control.texts[0]); parsed["target"] != "run:123" {
		t.Fatalf("the trailer does not name the target: %v", parsed)
	}
}

// A run that completes successfully wakes met with state, conclusion and url.
func TestGitHubRunSuccessWakesMetWithTheFields(t *testing.T) {
	answer := `{"status":"in_progress","conclusion":"","url":"https://github.com/o/r/actions/runs/123"}`
	runner, store, control, now := gitHubRunner(t, func(_ context.Context, _ string, args []string) (string, error) {
		if strings.Join(args, " ") != "run view 123 --json status,conclusion,url" {
			t.Fatalf("gh args = %v", args)
		}
		return answer, nil
	})
	ctx := context.Background()
	runner.Tick(ctx, nil, nil)
	if w := store.waits["w1"]; w.Status != StatusWaiting || w.Errors != 0 {
		t.Fatalf("in progress settled: %+v", w)
	}
	answer = `{"status":"completed","conclusion":"success","url":"https://github.com/o/r/actions/runs/123"}`
	*now = now.Add(time.Minute)
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)
	if len(control.texts) != 1 {
		t.Fatalf("texts = %v", control.texts)
	}
	parsed, ok := ParseWakeTrailer(control.texts[0])
	if !ok || parsed["kind"] != "github" || parsed["outcome"] != "met" || parsed["target"] != "run:123" ||
		parsed["state"] != "completed" || parsed["conclusion"] != "success" || parsed["url"] != "https://github.com/o/r/actions/runs/123" {
		t.Fatalf("trailer = %v", parsed)
	}
}

// A target that no longer exists gives up at once rather than after three
// identical answers.
func TestGitHubMissingTargetGivesUpAtOnce(t *testing.T) {
	runner, store, _, _ := gitHubRunner(t, func(context.Context, string, []string) (string, error) {
		return "", errors.New("could not find any workflow run with id 123: HTTP 404: Not Found")
	})
	runner.Tick(context.Background(), nil, nil)
	if w := store.waits["w1"]; w.Outcome != "gave-up" || w.Runs != 1 {
		t.Fatalf("a missing target was polled again: %+v", w)
	}
}
