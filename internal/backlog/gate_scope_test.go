package backlog

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A request with KillRemaining clears its scope before it starts and after it
// exits; an ordinary request still leaves the scope alone on a normal exit.
func TestSystemdScopeRunnerKillRemaining(t *testing.T) {
	for _, kill := range []bool{false, true} {
		t.Run(fmt.Sprintf("kill=%v", kill), func(t *testing.T) {
			root := t.TempDir()
			calls := filepath.Join(root, "calls")
			systemdRun := writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\necho run >> %q\n", calls))
			systemctl := writeExecutable(t, root, "systemctl", fmt.Sprintf("#!/bin/sh\necho \"$*\" >> %q\nexit 1\n", calls))
			_, err := (SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}).Run(context.Background(),
				ProcessRequest{ID: "verify-attempt-1-gate-0", Dir: root, Program: "/bin/sh", Args: []string{"-c", "true"}, KillRemaining: kill})
			if err != nil {
				t.Fatal(err)
			}
			killCall := "--user kill --kill-who=all --signal=KILL " + processScopeUnit("verify-attempt-1-gate-0")
			want := "run"
			if kill {
				want = killCall + "\nrun\n" + killCall
			}
			if got := strings.TrimSpace(readAbsoluteTestFile(t, calls)); got != want {
				t.Fatalf("calls:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}

func requireUserSystemd(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("systemd-run"); err != nil {
		t.Skip("systemd-run unavailable")
	}
	if out, _ := exec.Command("systemctl", "--user", "show-environment").CombinedOutput(); len(out) == 0 {
		t.Skip("no user systemd instance")
	}
}

// A child left by an earlier gate command waited, through inotify, for the
// worker to open the first declared output for hashing and then rewrote the
// second, untracked one before it was hashed. The digest recorded the new
// bytes, so capture matched content the last gate command never checked.
// The gate's scope is now killed when each command exits, so the child is
// gone before the worker hashes anything.
func TestGateChildCannotRewriteUntrackedOutputBeforeDigest(t *testing.T) {
	requireUserSystemd(t)
	if _, err := exec.LookPath("inotifywait"); err != nil {
		t.Skip("inotifywait unavailable")
	}
	dir := h2GateRepository(t)
	big := filepath.Join(dir, "big.bin")
	if err := os.WriteFile(big, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(big, 128<<20); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, dir, "out.txt", "good")
	storage := t.TempDir()
	id := fmt.Sprintf("gate-digest-%d", time.Now().UnixNano())
	req := h2GateRequest(dir, id)
	agent := `(inotifywait -qq -e open big.bin; printf 'bad\n' > out.txt) >/dev/null 2>&1 </dev/null & sleep 0.3`
	req.Task.Gate = &domain.TaskGate{Commands: []string{agent, "grep -qx good out.txt"}, Timeout: 5 * time.Second}
	req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "big.bin"}, {Name: "out.txt"}}
	t.Cleanup(func() {
		_ = exec.Command("systemctl", "--user", "kill", "--kill-who=all", "--signal=KILL", processScopeUnit(fmt.Sprintf("verify-%s-gate-0", id))).Run()
	})
	result, err := (AttemptFinalizer{StorageRoot: storage, Processes: SystemdScopeRunner{}}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	for _, artifact := range result.Artifacts {
		if artifact.Name != "out.txt" {
			continue
		}
		if got := strings.TrimSpace(string(readStoredArtifact(t, storage, artifact))); got != "good" && result.Completion.VerificationPassed {
			t.Fatalf("worker passed with captured out.txt=%q; the last gate command required \"good\"", got)
		}
		return
	}
	t.Fatalf("no out.txt captured: %+v", result.Completion)
}

// Finalization of one attempt is retried after a lost settlement, a deferred
// collection or a worker restart, and the retry runs each gate command under
// the same scope unit. A background child left by the first run kept that
// unit loaded, so systemd-run refused the retry and the gate failed a command
// that had passed.
func TestGateRefinalizationAfterBackgroundChild(t *testing.T) {
	requireUserSystemd(t)
	dir := h2GateRepository(t)
	id := fmt.Sprintf("gate-retry-%d", time.Now().UnixNano())
	req := h2GateRequest(dir, id)
	req.Task.Gate = &domain.TaskGate{Commands: []string{"grep -qx source source.txt && { sleep 20 >/dev/null 2>&1 </dev/null & }"}, Timeout: 10 * time.Second}
	unit := processScopeUnit(fmt.Sprintf("verify-%s-gate-0", id))
	t.Cleanup(func() {
		_ = exec.Command("systemctl", "--user", "kill", "--kill-who=all", "--signal=KILL", unit).Run()
	})
	for round := 1; round <= 2; round++ {
		storage := t.TempDir()
		result, err := (AttemptFinalizer{StorageRoot: storage, Processes: SystemdScopeRunner{}}).Finalize(context.Background(), req)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		cleanupImmutable(t, result.StorageDir)
		if !result.Completion.VerificationPassed {
			t.Fatalf("round %d: re-finalization of the same attempt failed: %s", round, result.Completion.Failure)
		}
	}
	if err := exec.Command("systemctl", "--user", "is-active", "--quiet", unit).Run(); err == nil {
		t.Fatalf("gate left %s running after the command exited", unit)
	}
}
