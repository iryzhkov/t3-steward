package backlog

import (
	"context"
	"errors"
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
// The kill of a unit that is not loaded fails, as the real systemctl does, and
// the state query proves the unit absent.
func TestSystemdScopeRunnerKillRemaining(t *testing.T) {
	for _, kill := range []bool{false, true} {
		t.Run(fmt.Sprintf("kill=%v", kill), func(t *testing.T) {
			root := t.TempDir()
			calls := filepath.Join(root, "calls")
			systemdRun := writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\necho run >> %q\n", calls))
			systemctl := writeExecutable(t, root, "systemctl", fmt.Sprintf("#!/bin/sh\necho \"$*\" >> %q\nif [ \"$2\" = show ]; then echo inactive; exit 0; fi\necho not loaded >&2\nexit 1\n", calls))
			_, err := (SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}).Run(context.Background(),
				ProcessRequest{ID: "verify-attempt-1-gate-0", Dir: root, Program: "/bin/sh", Args: []string{"-c", "true"}, KillRemaining: kill})
			if err != nil {
				t.Fatal(err)
			}
			unit := processScopeUnit("verify-attempt-1-gate-0")
			clear := "--user kill --kill-who=all --signal=KILL " + unit + "\n--user show --property=ActiveState --value " + unit
			want := "run"
			if kill {
				want = clear + "\nrun\n" + clear
			}
			if got := strings.TrimSpace(readAbsoluteTestFile(t, calls)); got != want {
				t.Fatalf("calls:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}

// When the scope cannot be shown to be empty, a process may still be running
// in it. Before the command that refuses to start it; after the command it
// turns even a successful exit into an error that keeps the systemctl output.
func TestSystemdScopeRunnerRefusesUnclearedScope(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			calls := filepath.Join(root, "calls")
			ran := filepath.Join(root, "ran")
			systemdRun := writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\ntouch %q\necho run >> %q\n", ran, calls))
			// Before: every call fails. After: the user manager answers until
			// the command has run, then fails.
			condition := "true"
			if phase == "after" {
				condition = fmt.Sprintf("[ -e %q ]", ran)
			}
			systemctl := writeExecutable(t, root, "systemctl", fmt.Sprintf("#!/bin/sh\necho \"$*\" >> %q\nif %s; then echo simulated-user-manager-failure >&2; exit 1; fi\nif [ \"$2\" = show ]; then echo inactive; exit 0; fi\nexit 1\n", calls, condition))
			var log strings.Builder
			result, err := (SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}).Run(context.Background(),
				ProcessRequest{ID: "verify-attempt-1-gate-0", Dir: root, Program: "/bin/sh", Args: []string{"-c", "true"}, Log: &log, KillRemaining: true})
			if err == nil {
				t.Fatalf("Run succeeded with an uncleared scope; calls:\n%s", readAbsoluteTestFile(t, calls))
			}
			var exitErr *ProcessExitError
			if errors.As(err, &exitErr) || result.ExitCode != 0 {
				t.Fatalf("cleanup failure reported as command exit: %v, %+v", err, result)
			}
			if !strings.Contains(err.Error(), "simulated-user-manager-failure") || !strings.Contains(log.String(), "simulated-user-manager-failure") {
				t.Fatalf("cleanup evidence lost: err=%v log=%q", err, log.String())
			}
			if _, statErr := os.Stat(ran); (statErr == nil) != (phase == "after") {
				t.Fatalf("phase %s: command ran = %v", phase, statErr == nil)
			}
		})
	}
}

// Waiting for a scope that stays active can outlast the gate command's own
// timeout. The failure used to be recorded as a gate command timeout, and
// the reason naming the uncleared scope was lost.
func TestGateScopeCleanupFailureIsNotReportedAsTimeout(t *testing.T) {
	dir := h2GateRepository(t)
	fake := t.TempDir()
	ran := filepath.Join(fake, "ran")
	run := writeExecutable(t, fake, "systemd-run", fmt.Sprintf("#!/bin/sh\ntouch %q\n", ran))
	ctl := writeExecutable(t, fake, "systemctl", fmt.Sprintf("#!/bin/sh\nif [ \"$2\" = show ]; then if [ -e %q ]; then echo active; else echo inactive; fi; fi\n", ran))
	req := h2GateRequest(dir, "attempt-1")
	req.Task.Gate = &domain.TaskGate{Commands: []string{"true"}, Timeout: 500 * time.Millisecond}
	result, err := (AttemptFinalizer{StorageRoot: t.TempDir(), Processes: SystemdScopeRunner{SystemdRunBinary: run, SystemctlBinary: ctl, ScopeCleanupTimeout: 1500 * time.Millisecond}}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	if result.Completion.VerificationPassed || !strings.Contains(result.Completion.Failure, "could not be cleared: state active") {
		t.Fatalf("scope cleanup failure reported as %q (passed=%v)", result.Completion.Failure, result.Completion.VerificationPassed)
	}
}

// A failed kill of a live scope is not an absent scope. A gate child survived
// a systemctl kill that failed, waited for the worker to open one declared
// output for hashing and rewrote another before its digest. The runner used
// to ignore the failure, so the worker passed and the coordinator succeeded
// the attempt with content the last gate command rejected. The scope, the
// child and systemctl are real; only every kill after the first is made to
// fail, so the child survives into hashing.
func TestGateKillFailureMustNotApproveBadOutput(t *testing.T) {
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
	id := "attempt-1"
	req := h2GateRequest(dir, id)
	req.WorkerID = "worker-a"
	systemctlPath, err := exec.LookPath("systemctl")
	if err != nil {
		t.Fatal(err)
	}
	fake := t.TempDir()
	ctl := writeExecutable(t, fake, "systemctl", fmt.Sprintf("#!/bin/sh\nif [ \"$2\" = kill ]; then\n  n=$(($(cat %[1]q 2>/dev/null || echo 0) + 1)); echo $n > %[1]q\n  if [ $n -gt 1 ]; then echo simulated-user-manager-kill-failure >&2; exit 1; fi\nfi\nexec %[2]q \"$@\"\n", filepath.Join(fake, "kills"), systemctlPath))
	agent := `(inotifywait -qq -e open big.bin; printf 'bad\n' > out.txt) >/dev/null 2>&1 </dev/null & sleep 0.3`
	req.Task.Gate = &domain.TaskGate{Commands: []string{agent, "grep -qx good out.txt"}, Timeout: 5 * time.Second}
	req.Task.Outputs = []domain.ArtifactDeclaration{{Name: "big.bin"}, {Name: "out.txt"}}
	t.Cleanup(func() {
		_ = exec.Command("systemctl", "--user", "kill", "--kill-who=all", "--signal=KILL", processScopeUnit(fmt.Sprintf("verify-%s-gate-0", id))).Run()
	})
	result, err := (AttemptFinalizer{StorageRoot: storage, Processes: SystemdScopeRunner{SystemctlBinary: ctl, ScopeCleanupTimeout: time.Second}}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	imported, _, _, _, _ := importFinalizedGate(t, req.Task, storage, result)
	progress := imported.Transition[0].Attempt.Progress
	if result.Completion.VerificationPassed || progress == domain.ProgressSucceeded {
		t.Fatalf("failed scope cleanup approved: worker verification=%v coordinator progress=%s", result.Completion.VerificationPassed, progress)
	}
	if !strings.Contains(result.Completion.Failure, "could not be cleared") || !strings.Contains(result.Completion.Failure, "simulated-user-manager-kill-failure") {
		t.Fatalf("failure does not name the uncleared scope: %q", result.Completion.Failure)
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
