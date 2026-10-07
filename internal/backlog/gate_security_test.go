package backlog

import (
	"context"
	"os/exec"
	"testing"
)

func securityGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	raw, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %s %v", args, raw, err)
	}
	return string(raw)
}
func TestH2GateIndexFlagsBypass(t *testing.T) {
	for _, flag := range []string{"--assume-unchanged", "--skip-worktree"} {
		t.Run(flag, func(t *testing.T) {
			dir := h2GateRepository(t)
			req := h2GateRequest(dir, "clean")
			req.Task.Gate.Commands = []string{"test \"$(cat source.txt)\" = source"}
			f := AttemptFinalizer{StorageRoot: t.TempDir(), Processes: &directRunner{}}
			first, _, err := f.runGate(context.Background(), req)
			if err != nil || !first.Passed {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			securityGit(t, dir, "update-index", flag, "source.txt")
			writeTestFile(t, dir, "source.txt", "poison")
			req.Attempt.ID = "poison"
			second, _, err := f.runGate(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if second.Passed {
				t.Fatalf("dirty tracked file bypassed worker gate: %+v", second)
			}
		})
	}
}
func TestH2GateCoreWorktreeBypass(t *testing.T) {
	dir := h2GateRepository(t)
	req := h2GateRequest(dir, "root-clean")
	req.Task.Gate.Commands = []string{"test \"$(cat source.txt)\" = source"}
	f := AttemptFinalizer{StorageRoot: t.TempDir(), Processes: &directRunner{}}
	first, _, err := f.runGate(context.Background(), req)
	if err != nil || !first.Passed {
		t.Fatalf("first=%v err=%v", first.Passed, err)
	}
	alternative := t.TempDir()
	writeTestFile(t, alternative, "source.txt", "source")
	securityGit(t, dir, "config", "core.worktree", alternative)
	writeTestFile(t, dir, "source.txt", "poison")
	req.Attempt.ID = "root-poison"
	second, _, err := f.runGate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if second.Passed {
		t.Fatalf("core.worktree substituted different filesystem: %+v", second)
	}
}
func TestH2GateManualGitlink(t *testing.T) {
	dir := h2GateRepository(t)
	sub := h2GateRepository(t)
	hash := securityGit(t, sub, "rev-parse", "HEAD")
	hash = hash[:len(hash)-1]
	securityGit(t, dir, "-c", "protocol.file.allow=always", "clone", "-q", sub, "nested")
	securityGit(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+hash+",nested")
	securityGit(t, dir, "commit", "-qm", "manual gitlink")
	req := h2GateRequest(dir, "manual-clean")
	req.Task.Gate.Commands = []string{"test \"$(cat nested/source.txt)\" = source"}
	f := AttemptFinalizer{StorageRoot: t.TempDir(), Processes: &directRunner{}}
	first, _, err := f.runGate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, dir, "nested/source.txt", "poison")
	req.Attempt.ID = "manual-poison"
	second, _, err := f.runGate(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Passed || second.Passed {
		t.Fatalf("manual gitlink bypass: first=%v second=%v", first.Passed, second.Passed)
	}
}
