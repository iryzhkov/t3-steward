package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// --github run <id> | pr <n> with --state and --repo parse into a fixed target;
// a state that does not belong to the target kind is refused.
func TestGitHubWaitRegistrationParsesTheTarget(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	spec, err := parseLocalWaitSpec([]string{"--github", "run", "123"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Kind != domain.WaitKindGitHub || spec.GitHub == nil || spec.GitHub.Kind != "run" || spec.GitHub.ID != "123" || spec.GitHub.State != "completed" {
		t.Fatalf("spec = %+v github=%+v", spec, spec.GitHub)
	}
	if spec.Condition != "github run:123 completed" {
		t.Fatalf("condition = %q", spec.Condition)
	}
	spec, err = parseLocalWaitSpec([]string{"--name", "pr", "--github", "pr", "45", "--state", "checks-passed", "--repo", "o/r", "--every", "1m"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if spec.GitHub.Kind != "pr" || spec.GitHub.ID != "45" || spec.GitHub.State != "checks-passed" || spec.GitHub.Repo != "o/r" || spec.Every != time.Minute || spec.Name != "pr" {
		t.Fatalf("spec = %+v github=%+v", spec, spec.GitHub)
	}
	if spec, err := parseLocalWaitSpec([]string{"--github=pr:45", "--state", "merged"}, now); err != nil || spec.GitHub.ID != "45" {
		t.Fatalf("--github=pr:45: %+v %v", spec, err)
	}
	for _, args := range [][]string{
		{"--github", "run", "123", "--state", "merged"},
		{"--github", "pr", "45", "--state", "completed"},
		{"--github", "issue", "1"},
		{"--github", "run"},
		{"--github", "run", "123", "--", "true"},
		{"--github", "run", "123", "--for", "1h"},
		{"--state", "merged", "--", "true"},
	} {
		if _, err := parseLocalWaitSpec(args, now); err == nil {
			t.Fatalf("%v was accepted", args)
		}
	}
}

// A github target may name its repository, as owner/name#<n> or as the URL gh
// and the browser show, and that repository is recorded as --repo would be. A
// target in any other form is refused before gh is asked, with the forms that
// are accepted, rather than handed to gh as a branch name.
func TestGitHubWaitTargetAcceptsQualifiedForms(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		args           []string
		kind, id, repo string
	}{
		{[]string{"--github", "pr", "owner/repo#45"}, "pr", "45", "owner/repo"},
		{[]string{"--github", "pr", "https://github.com/owner/repo/pull/45"}, "pr", "45", "owner/repo"},
		{[]string{"--github", "pr", "https://github.com/owner/repo/pull/45/files?x=1"}, "pr", "45", "owner/repo"},
		{[]string{"--github", "https://github.com/owner/repo/pull/45"}, "pr", "45", "owner/repo"},
		{[]string{"--github", "run", "https://github.com/owner/repo/actions/runs/987/job/1"}, "run", "987", "owner/repo"},
		{[]string{"--github", "https://github.com/owner/repo/actions/runs/987"}, "run", "987", "owner/repo"},
		{[]string{"--github", "run", "owner/repo#987"}, "run", "987", "owner/repo"},
		{[]string{"--github=pr:owner/repo#45", "--repo", "OWNER/repo"}, "pr", "45", "owner/repo"},
	} {
		spec, err := parseLocalWaitSpec(c.args, now)
		if err != nil {
			t.Fatalf("%v: %v", c.args, err)
		}
		if spec.GitHub.Kind != c.kind || spec.GitHub.ID != c.id || spec.GitHub.Repo != c.repo {
			t.Fatalf("%v parsed as %+v", c.args, spec.GitHub)
		}
		if got := strings.Join(spec.GitHub.Args(), " "); !strings.Contains(got, " "+c.id+" ") || !strings.HasSuffix(got, "--repo "+c.repo) {
			t.Fatalf("%v asks gh %q", c.args, got)
		}
	}
	// A pr id in none of those forms and without a # is a branch name, which gh
	// pr view has always accepted, and it reaches gh unchanged.
	for _, branch := range []string{"feature-branch", "fix/rc96-ax-cli"} {
		spec, err := parseLocalWaitSpec([]string{"--github", "pr", branch}, now)
		if err != nil {
			t.Fatalf("pr %s: %v", branch, err)
		}
		if spec.GitHub.ID != branch || spec.GitHub.Repo != "" || spec.GitHub.Args()[2] != branch {
			t.Fatalf("pr %s parsed as %+v", branch, spec.GitHub)
		}
	}
	// A run has no branch reading, and no kind accepts another host's URL; both
	// refusals say GitHub Enterprise is not supported.
	for _, args := range [][]string{
		{"--github", "run", "feature-branch"},
		{"--github", "pr", "https://github.example.com/owner/repo/pull/45"},
	} {
		_, err := parseLocalWaitSpec(args, now)
		if err == nil || !strings.Contains(err.Error(), "GitHub Enterprise hosts are not supported") {
			t.Fatalf("%v: %v", args, err)
		}
	}
	for _, args := range [][]string{
		{"--github", "pr", "https://github.com/owner/repo/issues/45"},
		{"--github", "pr", "https://github.com/owner/repo/actions/runs/987"},
		{"--github", "pr", "owner/repo#45", "--repo", "other/repo"},
		{"--github=issue:45"},
		{"--github", "pr", "owner/repo#abc"},
		{"--github", "pr", "owner/repo#next"},
		{"--github", "pr", "owner#45"},
		{"--github", "run", "owner#45"},
	} {
		_, err := parseLocalWaitSpec(args, now)
		if err == nil {
			t.Fatalf("%v was accepted", args)
		}
		if !strings.Contains(err.Error(), "owner/name#<n>") && !strings.Contains(err.Error(), "--repo names") &&
			!strings.Contains(err.Error(), "unknown --github kind") {
			t.Fatalf("%v was refused without the accepted forms: %v", args, err)
		}
	}
}

// A target with a # that is not owner/name#<number> is refused as malformed
// rather than handed to gh as a branch name, which gh answered with "no pull
// requests found for branch".
func TestGitHubWaitRefusesMalformedQualifiedTarget(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	_, err := parseLocalWaitSpec([]string{"--github", "pr", "owner/name#abc"}, now)
	if err == nil || !strings.Contains(err.Error(), "malformed") || !strings.Contains(err.Error(), "pr owner/name#<n>") {
		t.Fatalf("owner/name#abc: %v", err)
	}
}

// An unknown --github kind is named as such. Its id is left behind as a
// positional argument, which used to be reported as a command after -- that
// was never given.
func TestGitHubWaitNamesAnUnknownKind(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	_, err := parseLocalWaitSpec([]string{"--github", "issue", "5"}, now)
	if err == nil || err.Error() != `unknown --github kind "issue"; use run or pr` {
		t.Fatalf("--github issue 5: %v", err)
	}
}

// --task current --github parks the attempt on a github record: gh is read once
// at registration, a run that is still in progress is accepted, and one that
// is already complete or unreadable is refused.
func TestTaskBoundGitHubWaitProbesAndRegisters(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	answer, answerErr := `{"status":"completed","conclusion":"success","url":"u"}`, error(nil)
	var seen [][]string
	var seenDirs []string
	previous := gitHubCommand
	gitHubCommand = func(_ context.Context, dir string, args []string) (string, error) {
		seen = append(seen, args)
		seenDirs = append(seenDirs, dir)
		if len(args) != 0 && args[0] == "repo" {
			// The repository this directory is a checkout of, which is what a
			// registration without --repo records on the wait.
			return `{"nameWithOwner":"resolved/repo"}`, nil
		}
		return answer, answerErr
	}
	t.Cleanup(func() { gitHubCommand = previous })

	err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "done", "--github", "run", "123"})
	if err == nil || !strings.Contains(err.Error(), "already") {
		t.Fatalf("a completed run was registered: %v", err)
	}
	answer, answerErr = "", errors.New("gh: not logged in")
	err = cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "broken", "--github", "run", "123"})
	if err == nil || !strings.Contains(err.Error(), "not logged in") {
		t.Fatalf("an unreadable target was registered: %v", err)
	}
	answer, answerErr = `{"status":"in_progress","conclusion":"","url":"u"}`, nil
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "ci", "--github", "run", "123", "--repo", "o/r"}); err != nil {
		t.Fatal(err)
	}
	// A registration that names the repository asks gh for nothing else; the
	// two before it, which named none, each resolved one first.
	if len(seen) != 5 || strings.Join(seen[4], " ") != "run view 123 --json status,conclusion,url --repo o/r" {
		t.Fatalf("gh was called with %v", seen)
	}
	if strings.Join(seen[0], " ") != "repo view --json nameWithOwner" {
		t.Fatalf("the repository of the calling directory was not resolved: %v", seen)
	}
	// Every call runs in the directory the wait is registered in, because gh
	// resolves a repository from its working directory and the daemon's own is
	// not a checkout of anything.
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i, dir := range seenDirs {
		if dir != working {
			t.Fatalf("gh call %d ran in %q, want the registering directory %q", i, dir, working)
		}
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
	if waits[0].Kind != domain.WaitKindGitHub || waits[0].Condition != "github run:123 completed in o/r" {
		t.Fatalf("coordinator record = %+v", waits[0])
	}
	local, err := store.ListWaits(ctx, "")
	if err != nil || len(local) != 1 {
		t.Fatalf("local=%v err=%v", local, err)
	}
	if local[0].Kind != domain.WaitKindGitHub || local[0].GitHub == nil || local[0].GitHub.Repo != "o/r" || local[0].Runs != 1 || local[0].LastExit != 1 {
		t.Fatalf("local row = %+v", local[0])
	}
	if local[0].Dir == "" {
		t.Fatalf("the local row records no directory to poll in: %+v", local[0])
	}
	// A registration that names no repository records the one its directory is a
	// checkout of, so the stored wait says which repository it is about.
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "resolved", "--github", "run", "456"}); err != nil {
		t.Fatal(err)
	}
	local, err = store.ListWaits(ctx, "")
	if err != nil || len(local) != 2 {
		t.Fatalf("local=%v err=%v", local, err)
	}
	resolved := local[0]
	if resolved.GitHub == nil || resolved.GitHub.ID != "456" {
		resolved = local[1]
	}
	if resolved.GitHub == nil || resolved.GitHub.Repo != "resolved/repo" {
		t.Fatalf("the registration did not record the repository of its directory: %+v", resolved.GitHub)
	}
}
