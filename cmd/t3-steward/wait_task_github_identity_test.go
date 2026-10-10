package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestW2ReviewGitHubMissingLocalIdentityReplay(t *testing.T) {
	cfg, store := taskWaitCLIFixture(t)
	ctx := context.Background()
	a, b := t.TempDir(), t.TempDir()
	previous := gitHubCommand
	gitHubCommand = func(_ context.Context, dir string, args []string) (string, error) {
		if len(args) != 0 && args[0] == "repo" {
			if dir == a {
				return `{"nameWithOwner":"owner/project-a"}`, nil
			}
			return `{"nameWithOwner":"owner/project-b"}`, nil
		}
		return `{"status":"in_progress","conclusion":"","url":"u"}`, nil
	}
	t.Cleanup(func() { gitHubCommand = previous })
	args := []string{"--task", "current", "--request-id", "gh-missing", "--github", "run", "123"}
	spec, err := parseLocalWaitSpec(args, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	spec.Dir = a
	// Project A's registration as the coordinator committed it; the local save never happened.
	if _, err := store.RegisterTaskWait(ctx, localTaskWaitRegistration(spec, identity), taskWaitFixtureNow); err != nil {
		t.Fatal(err)
	}
	t.Chdir(b)
	output := captureStdout(t, func() { err = cmdTaskWaitAdd(ctx, cfg, args) })
	checks, _ := store.ListWaits(ctx, "")
	if err == nil && strings.Contains(output, "registered") {
		repo := ""
		if len(checks) == 1 && checks[0].GitHub != nil {
			repo = checks[0].GitHub.Repo
		}
		t.Fatalf("project B's github condition adopted project A's wait: repo=%q output=%s", repo, output)
	}
}

func TestTaskWaitGitHubCrashSafeReplay(t *testing.T) {
	for _, tc := range []struct {
		name         string
		originalRepo string
		retryRepo    string
		explicit     bool
		wantRefusal  bool
	}{
		{"complete-same", "owner/project-a", "owner/project-a", false, false},
		{"complete-changed", "owner/project-a", "owner/project-b", false, true},
		{"older-same", "", "owner/project-a", false, true},
		{"explicit-same-other-checkout", "owner/project-a", "owner/project-b", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, store := taskWaitCLIFixture(t)
			ctx := context.Background()
			previous := gitHubCommand
			gitHubCommand = func(_ context.Context, _ string, args []string) (string, error) {
				if len(args) != 0 && args[0] == "repo" {
					return `{"nameWithOwner":"` + tc.retryRepo + `"}`, nil
				}
				return `{"status":"in_progress","conclusion":"","url":"u"}`, nil
			}
			t.Cleanup(func() { gitHubCommand = previous })
			args := []string{"--task", "current", "--request-id", "gh-recovery", "--name", "ci", "--github", "run", "123"}
			if tc.explicit {
				args = append(args, "--repo", tc.originalRepo)
			}
			spec, err := parseLocalWaitSpec(args, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			spec.GitHub.Repo = tc.originalRepo
			identity, err := resolveTaskIdentity(os.Getenv)
			if err != nil {
				t.Fatal(err)
			}
			// Commit only the coordinator record, as if saving the local poll crashed.
			original, err := store.RegisterTaskWait(ctx, localTaskWaitRegistration(spec, identity), taskWaitFixtureNow)
			if err != nil {
				t.Fatal(err)
			}
			output := captureStdout(t, func() { err = cmdTaskWaitAdd(ctx, cfg, args) })
			if tc.wantRefusal {
				if err == nil || !strings.Contains(err.Error(), "different condition") || strings.Contains(output, "registered") {
					t.Fatalf("changed/unbound identity replayed: err=%v output=%s", err, output)
				}
			} else if err != nil || !strings.Contains(output, "registered") {
				t.Fatalf("identical recovery refused: err=%v output=%s", err, output)
			}
			checks, err := store.ListWaits(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantRefusal && len(checks) != 0 {
				t.Fatalf("refused replay saved checks: %+v", checks)
			}
			if !tc.wantRefusal && (len(checks) != 1 || checks[0].GitHub.Repo != tc.originalRepo || checks[0].TaskWaitID != original.ID) {
				t.Fatalf("recovered identity changed: %+v", checks)
			}
			records, err := store.ListTaskWaits(ctx)
			if err != nil || len(records) != 1 || records[0].ConditionDigest != original.ConditionDigest {
				t.Fatalf("coordinator identity changed: records=%+v err=%v", records, err)
			}
		})
	}
}

func TestTaskWaitGitHubResolvedRepositoryDistinguishesConditions(t *testing.T) {
	cfg, store := taskWaitCLIFixture(t)
	ctx := context.Background()
	previous := gitHubCommand
	repo := "owner/project-a"
	gitHubCommand = func(_ context.Context, _ string, args []string) (string, error) {
		if len(args) != 0 && args[0] == "repo" {
			return `{"nameWithOwner":"` + repo + `"}`, nil
		}
		return `{"status":"in_progress","conclusion":"","url":"u"}`, nil
	}
	t.Cleanup(func() { gitHubCommand = previous })
	args := []string{"--task", "current", "--github", "run", "123"}
	registerTaskConditions(t, ctx, cfg, args, args)
	repo = "owner/project-b"
	registerTaskConditions(t, ctx, cfg, args)
	records, err := store.ListTaskWaits(ctx)
	if err != nil || len(records) != 2 || records[0].ConditionDigest == records[1].ConditionDigest {
		t.Fatalf("repositories did not produce two conditions: records=%+v err=%v", records, err)
	}
	checks, err := store.ListWaits(ctx, "")
	if err != nil || len(checks) != 2 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
}

func TestTaskWaitGitHubUnresolvedRepositoryRefusesToPark(t *testing.T) {
	cfg, store := taskWaitCLIFixture(t)
	ctx := context.Background()
	previous := gitHubCommand
	gitHubCommand = func(_ context.Context, _ string, _ []string) (string, error) {
		return "{}", nil
	}
	t.Cleanup(func() { gitHubCommand = previous })
	var err error
	output := captureStdout(t, func() {
		err = cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--github", "run", "123"})
	})
	if err == nil || !strings.Contains(err.Error(), "--repo owner/name") || strings.Contains(output, "registered") {
		t.Fatalf("unbound wait was not clearly refused: err=%v output=%s", err, output)
	}
	records, err := store.ListTaskWaits(ctx)
	if err != nil || len(records) != 0 {
		t.Fatalf("refusal parked task: records=%+v err=%v", records, err)
	}
	checks, err := store.ListWaits(ctx, "")
	if err != nil || len(checks) != 0 {
		t.Fatalf("refusal saved check: checks=%+v err=%v", checks, err)
	}
}
