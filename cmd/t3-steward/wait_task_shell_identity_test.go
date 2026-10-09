package main

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func TestW2ReviewRelativeDirectoryReplay(t *testing.T) {
	cfg, store := taskWaitCLIFixture(t)
	ctx := context.Background()
	a, b := t.TempDir(), t.TempDir()
	args := []string{"--task", "current", "--request-id", "relative-dir", "--dir", ".", "--", "false"}
	t.Chdir(a)
	registerTaskConditions(t, ctx, cfg, args)
	// Equivalent absolute spelling still replays the same check.
	registerTaskConditions(t, ctx, cfg, []string{"--task", "current", "--request-id", "relative-dir", "--dir", a, "--", "false"})
	t.Chdir(b)
	var err error
	output := captureStdout(t, func() { err = cmdTaskWaitAdd(ctx, cfg, args) })
	if !errors.Is(err, errTaskCheckConditionChanged) || strings.Contains(output, "registered") {
		t.Fatalf("different resolved directories replayed: err=%v output=%s", err, output)
	}
	checks, err := store.ListWaits(ctx, "")
	if err != nil || len(checks) != 1 || checks[0].Dir != a || !filepath.IsAbs(checks[0].Dir) {
		t.Fatalf("original resolved directory lost: checks=%+v err=%v", checks, err)
	}
}

func TestTaskWaitRefusesOlderRelativeShellDirectory(t *testing.T) {
	for _, dir := range []string{"", ".", "project"} {
		saved := wait.Wait{TaskWaitID: "tw-old", Kind: domain.WaitKindShell, Command: []string{"false"}, Dir: dir}
		local := saved
		local.Dir = t.TempDir()
		err := refuseChangedTaskCheck("old", saved, local)
		if !errors.Is(err, errTaskCheckConditionChanged) || !strings.Contains(err.Error(), "cannot be established") {
			t.Fatalf("older directory %q was not clearly refused: %v", dir, err)
		}
	}
}

// Reproduce the coordinator-commit/local-save crash boundary, both for an
// older registration lacking execution identity and for complete new ones.
func TestW2ReviewMissingLocalIdentityReplay(t *testing.T) {
	for _, tc := range []struct {
		name        string
		complete    bool
		command     []string
		changedDir  bool
		wantRefusal bool
	}{
		{"older-directory", false, []string{"false"}, true, true},
		{"older-same", false, []string{"false"}, false, true},
		{"complete-directory", true, []string{"false"}, true, true},
		{"complete-argv", true, []string{"sh", "-c", "false", "x"}, false, true},
		{"complete-same", true, []string{"false"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, store := taskWaitCLIFixture(t)
			ctx := context.Background()
			firstDir, secondDir := t.TempDir(), t.TempDir()
			originalCommand := []string{"false"}
			if tc.name == "complete-argv" {
				originalCommand = []string{"sh", "-c", "false x"}
			}
			r := domain.TaskWaitRegistration{
				RequestID: "missing-local", WorkflowRunID: "run-1", TaskID: "task-1",
				AttemptID: "attempt-1", IssuedRevision: 7, ThreadID: "thread-1",
				Wake: domain.WakeAll, MaxDuration: 24 * time.Hour,
				Name: strings.Join(originalCommand, " "), Condition: strings.Join(originalCommand, " "),
			}
			if tc.complete {
				r.Shell = &domain.ShellWaitCondition{Dir: firstDir, Command: originalCommand}
			}
			original, err := store.RegisterTaskWait(ctx, r, taskWaitFixtureNow)
			if err != nil {
				t.Fatal(err)
			}
			dir := firstDir
			if tc.changedDir {
				dir = secondDir
			}
			args := append([]string{"--task", "current", "--request-id", "missing-local", "--dir", dir, "--"}, tc.command...)
			output := captureStdout(t, func() { err = cmdTaskWaitAdd(ctx, cfg, args) })
			if tc.wantRefusal {
				if err == nil || !strings.Contains(err.Error(), "different condition") || strings.Contains(output, "registered") {
					t.Fatalf("unverifiable/changed shell identity replayed: err=%v output=%s", err, output)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			checks, err := store.ListWaits(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			wantChecks := 0
			if !tc.wantRefusal {
				wantChecks = 1
			}
			if len(checks) != wantChecks {
				t.Fatalf("checks=%+v, want %d", checks, wantChecks)
			}
			if len(checks) == 1 && (checks[0].Dir != firstDir || strings.Join(checks[0].Command, " ") != r.Condition) {
				t.Fatalf("recovered check changed original identity: %+v", checks[0])
			}
			records, err := store.ListTaskWaits(ctx)
			if err != nil || len(records) != 1 || records[0].ConditionDigest != original.ConditionDigest {
				t.Fatalf("coordinator identity changed: records=%+v err=%v", records, err)
			}
		})
	}
}
