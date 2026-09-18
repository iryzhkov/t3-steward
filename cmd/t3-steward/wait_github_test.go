package main

import (
	"context"
	"errors"
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

// --task current --github parks the attempt on a github record: gh is read once
// at registration, a run that is still in progress is accepted, and one that
// is already complete or unreadable is refused.
func TestTaskBoundGitHubWaitProbesAndRegisters(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	answer, answerErr := `{"status":"completed","conclusion":"success","url":"u"}`, error(nil)
	var seen [][]string
	previous := gitHubCommand
	gitHubCommand = func(_ context.Context, args []string) (string, error) {
		seen = append(seen, args)
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
	if len(seen) != 3 || strings.Join(seen[2], " ") != "run view 123 --json status,conclusion,url --repo o/r" {
		t.Fatalf("gh was called with %v", seen)
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
}
