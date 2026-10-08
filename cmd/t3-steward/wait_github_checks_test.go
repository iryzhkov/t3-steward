package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

const checksTestSHA = "0123456789abcdef0123456789abcdef01234567"

// --github-checks reads a commit or a pull request into a github wait in state
// checks-completed, naming its repository as --github does.
func TestGitHubChecksWaitParsesTheTarget(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		args           []string
		kind, id, repo string
	}{
		{[]string{"--github-checks", "owner/repo@" + checksTestSHA}, "commit", checksTestSHA, "owner/repo"},
		{[]string{"--github-checks", "owner/repo@ABCDEF1"}, "commit", "abcdef1", "owner/repo"},
		{[]string{"--github-checks", "https://github.com/owner/repo/commit/" + checksTestSHA + "?diff=split"}, "commit", checksTestSHA, "owner/repo"},
		{[]string{"--github-checks", "abcdef1", "--repo", "owner/repo"}, "commit", "abcdef1", "owner/repo"},
		{[]string{"--github-checks", "owner/repo#45"}, "pr", "45", "owner/repo"},
		{[]string{"--github-checks", "https://github.com/owner/repo/pull/45/checks"}, "pr", "45", "owner/repo"},
		{[]string{"--github-checks", "45"}, "pr", "45", ""},
		{[]string{"--github-checks", "1234567", "--repo", "owner/repo"}, "pr", "1234567", "owner/repo"},
	} {
		spec, err := parseLocalWaitSpec(c.args, now)
		if err != nil {
			t.Fatalf("%v: %v", c.args, err)
		}
		if spec.Kind != domain.WaitKindGitHub || spec.GitHub == nil || spec.GitHub.Kind != c.kind || spec.GitHub.ID != c.id ||
			spec.GitHub.Repo != c.repo || spec.GitHub.State != wait.ChecksCompleted {
			t.Fatalf("%v parsed as %+v", c.args, spec.GitHub)
		}
		want := "github " + c.kind + ":" + c.id + " checks-completed"
		if c.repo != "" {
			want += " in " + c.repo
		}
		if spec.Condition != want {
			t.Fatalf("%v condition = %q, want %q", c.args, spec.Condition, want)
		}
	}
}

// A malformed target, a commit without a repository, a second repository,
// --state and a second kind are all refused before gh is asked.
func TestGitHubChecksWaitRefusals(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"--github-checks", "abcdef1"}, "needs its repository"},
		{[]string{"--github-checks", "owner/repo@xyz"}, "hexadecimal"},
		{[]string{"--github-checks", "owner/repo@abc"}, "hexadecimal"},
		{[]string{"--github-checks", "@abcdef1"}, "owner/name@<sha>"},
		{[]string{"--github-checks", "owner@abcdef1"}, "owner/name@<sha>"},
		{[]string{"--github-checks", "owner/repo@abcdef1", "--repo", "other/repo"}, "--repo names"},
		{[]string{"--github-checks", "owner/repo@abcdef1", "--state", "merged"}, "--state belongs to --github"},
		{[]string{"--github-checks", "https://github.com/owner/repo/issues/4"}, "owner/name@<sha>"},
		{[]string{"--github-checks", "https://example.com/owner/repo/commit/abcdef1"}, "owner/name@<sha>"},
		{[]string{"--github-checks", "main"}, "owner/name@<sha>"},
		{[]string{"--github-checks", "owner/repo@abcdef1", "--github", "run", "1"}, "one wait has one kind"},
		{[]string{"--github-checks", "owner/repo@abcdef1", "--for", "1h"}, "one wait has one kind"},
	} {
		_, err := parseLocalWaitSpec(c.args, now)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%v: %v, want %q", c.args, err, c.want)
		}
	}
}

// --task current --github-checks parks the attempt on the commit's checks:
// the fake gh is read once, a commit whose checks still run is registered with
// the summary as its first line, and one whose checks already finished is
// refused with the summary. No real network is touched.
func TestTaskBoundGitHubChecksWaitProbesAndRegisters(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	answer := func(state, nodes string) string {
		return `{"data":{"repository":{"object":{"oid":"` + checksTestSHA + `","url":"u","statusCheckRollup":{"state":"` + state +
			`","contexts":{"totalCount":2,"nodes":[` + nodes + `]}}}}}}`
	}
	current := answer("SUCCESS", `{"name":"lint","status":"COMPLETED","conclusion":"SUCCESS"},{"context":"ci","state":"SUCCESS"}`)
	var seen [][]string
	previous := gitHubCommand
	gitHubCommand = func(_ context.Context, _ string, args []string) (string, error) {
		seen = append(seen, args)
		return current, nil
	}
	t.Cleanup(func() { gitHubCommand = previous })

	err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "done", "--github-checks", "o/r@" + checksTestSHA})
	if err == nil || !strings.Contains(err.Error(), "already holds (checks passed: 2 checks, 2 success)") {
		t.Fatalf("finished checks were registered: %v", err)
	}
	current = answer("PENDING", `{"name":"lint","status":"IN_PROGRESS"},{"context":"ci","state":"SUCCESS"}`)
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "ci", "--github-checks", "o/r@" + checksTestSHA}); err != nil {
		t.Fatal(err)
	}
	for _, args := range seen {
		if len(args) < 2 || args[0] != "api" || args[1] != "graphql" {
			t.Fatalf("gh was asked %v", args)
		}
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 || waits[0].Kind != domain.WaitKindGitHub ||
		waits[0].Condition != "github commit:"+checksTestSHA+" checks-completed in o/r" {
		t.Fatalf("coordinator record = %+v %v", waits, err)
	}
	local, err := store.ListWaits(ctx, "")
	if err != nil || len(local) != 1 || local[0].GitHub == nil || local[0].GitHub.Kind != "commit" ||
		local[0].LastOutput != "checks pending: 2 checks, 1 pending (lint), 1 success" {
		t.Fatalf("local = %+v %v", local, err)
	}
}
