package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A cancelled run returns within its cleanup bound even when the user manager
// never answers, kills the launcher itself rather than waiting for systemctl,
// and still reports that the scope could not be shown to be empty.
func TestCancelledRunReturnsWithinCleanupBoundWhenSystemctlHangs(t *testing.T) {
	root := t.TempDir()
	launcherPath := filepath.Join(root, "launcher.pid")
	run := writeExecutable(t, root, "run", fmt.Sprintf("#!/bin/sh\nprintf '%%s' \"$$\" > %q.tmp && mv %q.tmp %q\nexec sleep 30\n", launcherPath, launcherPath, launcherPath))
	ctl := writeExecutable(t, root, "ctl", "#!/bin/sh\nexec sleep 30\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		started := time.Now()
		_, err := (SystemdScopeRunner{SystemdRunBinary: run, SystemctlBinary: ctl, ScopeCleanupTimeout: 20 * time.Millisecond}).Run(
			ctx, ProcessRequest{ID: "cancel-bound", Dir: root, Program: "true"})
		done <- outcome{err, time.Since(started)}
	}()
	launcher := waitForPID(t, launcherPath)
	cancelled := time.Now()
	cancel()
	var got outcome
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled run did not return while systemctl hung")
	}
	if elapsed := time.Since(cancelled); elapsed > time.Second {
		t.Fatalf("cancellation took %s despite a 20ms cleanup bound", elapsed.Round(time.Millisecond))
	}
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("cancelled run error = %v, want context canceled", got.err)
	}
	if got.err == nil || !strings.Contains(got.err.Error(), "kill process scope "+processScopeUnit("cancel-bound")) {
		t.Fatalf("cancelled run error = %v, want an explicit scope containment failure", got.err)
	}
	waitForProcessExit(t, launcher)
}

// systemd collects a scope as soon as its last process exits, after which
// systemctl refuses to kill it as not loaded. Cancelling a command that left
// nothing behind must therefore kill the scope while the launcher still
// holds it, and report plain cancellation rather than a containment failure.
func TestCancelledRunKillsTheScopeBeforeItsLauncherExits(t *testing.T) {
	root := t.TempDir()
	launcherPath := filepath.Join(root, "launcher.pid")
	run := writeExecutable(t, root, "run", fmt.Sprintf("#!/bin/sh\nprintf '%%s' \"$$\" > %q.tmp && mv %q.tmp %q\nexec sleep 30\n", launcherPath, launcherPath, launcherPath))
	// The fake manager behaves like systemd: a scope whose process is gone
	// is no longer loaded.
	ctl := writeExecutable(t, root, "ctl", fmt.Sprintf("#!/bin/sh\nlauncher=$(cat %q)\nif kill -0 \"$launcher\" 2>/dev/null; then kill -KILL \"$launcher\"; exit 0; fi\necho \"Failed to kill unit: Unit not loaded.\" >&2\nexit 1\n", launcherPath))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (SystemdScopeRunner{SystemdRunBinary: run, SystemctlBinary: ctl, ScopeCleanupTimeout: 5 * time.Second}).Run(
			ctx, ProcessRequest{ID: "cancel-empty", Dir: root, Program: "true"})
		done <- err
	}()
	launcher := waitForPID(t, launcherPath)
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled run did not return")
	}
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "kill process scope") {
		t.Fatalf("cancelled run error = %v, want plain cancellation", err)
	}
	waitForProcessExit(t, launcher)
}

// A descendant that escaped the launcher and keeps its output open cannot
// hold a cancelled run past the cleanup bound when the scope kill also fails.
func TestCancelledRunReturnsWhenDescendantHoldsOutputAndScopeKillFails(t *testing.T) {
	root := t.TempDir()
	childPath := filepath.Join(root, "child.pid")
	run := writeExecutable(t, root, "run", fmt.Sprintf("#!/bin/sh\nsleep 30 &\nprintf '%%s' \"$!\" > %q.tmp && mv %q.tmp %q\nexec sleep 30\n", childPath, childPath, childPath))
	ctl := writeExecutable(t, root, "ctl", "#!/bin/sh\nexit 1\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (SystemdScopeRunner{SystemdRunBinary: run, SystemctlBinary: ctl, ScopeCleanupTimeout: 200 * time.Millisecond}).Run(
			ctx, ProcessRequest{ID: "cancel-held-output", Dir: root, Program: "true"})
		done <- err
	}()
	child := waitForPID(t, childPath)
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	cancelled := time.Now()
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled run waited for a descendant holding its output")
	}
	if elapsed := time.Since(cancelled); elapsed > 2*time.Second {
		t.Fatalf("cancellation took %s despite a 200ms cleanup bound", elapsed.Round(time.Millisecond))
	}
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "kill process scope") {
		t.Fatalf("cancelled run error = %v, want cancellation joined with a containment failure", err)
	}
}

// A cancelled run that gives up waiting for its launcher's output still owns
// that output: it closes its end before returning instead of leaving a reader
// behind, so a descendant that escaped the scope writes into a closed pipe.
func TestCancelledRunReleasesLauncherOutputBeforeReturning(t *testing.T) {
	root := t.TempDir()
	statusPath := filepath.Join(root, "write.status")
	childPath := filepath.Join(root, "child.pid")
	// The escaped descendant ignores SIGPIPE and records whether a write to
	// the launcher's output still succeeds after Run has returned.
	child := writeExecutable(t, root, "child", fmt.Sprintf("#!/bin/sh\ntrap '' PIPE\nsleep 1\necho late 2>/dev/null\necho $? > %q.tmp && mv %q.tmp %q\n", statusPath, statusPath, statusPath))
	run := writeExecutable(t, root, "run", fmt.Sprintf("#!/bin/sh\n%q &\nprintf '%%s' \"$!\" > %q.tmp && mv %q.tmp %q\nexec sleep 30\n", child, childPath, childPath, childPath))
	ctl := writeExecutable(t, root, "ctl", "#!/bin/sh\nexit 1\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (SystemdScopeRunner{SystemdRunBinary: run, SystemctlBinary: ctl, ScopeCleanupTimeout: 20 * time.Millisecond}).Run(
			ctx, ProcessRequest{ID: "cancel-release-output", Dir: root, Program: "true"})
		done <- err
	}()
	pid := waitForPID(t, childPath)
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled run waited for a descendant holding its output")
	}
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "kill process scope") {
		t.Fatalf("cancelled run error = %v, want cancellation joined with a containment failure", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, readErr := os.ReadFile(statusPath)
		if readErr == nil {
			if strings.TrimSpace(string(status)) == "0" {
				t.Fatal("descendant still wrote to the launcher's output after Run returned")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant never reported its late write: %v", readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Clearing a scope before and after a KillRemaining command is bounded by
// the cleanup timeout even when systemctl leaves a child holding its output,
// and still fails explicitly because the scope was not shown to be empty.
func TestKillRemainingCleanupReturnsWithinBoundWhenSystemctlChildHoldsOutput(t *testing.T) {
	for _, tc := range []struct {
		name string
		// hangAfterRun makes systemctl answer normally until the command
		// has run, so only the cleanup after it hangs.
		hangAfterRun bool
	}{
		{name: "before the command"},
		{name: "after the command", hangAfterRun: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			ranPath := filepath.Join(root, "ran")
			run := writeExecutable(t, root, "run", fmt.Sprintf("#!/bin/sh\ntouch %q\n", ranPath))
			// Without exec, the shell's sleep child keeps systemctl's output
			// open after the shell itself is killed.
			ctlScript := "#!/bin/sh\nsleep 5\n"
			if tc.hangAfterRun {
				ctlScript = fmt.Sprintf("#!/bin/sh\nif [ -e %q ]; then sleep 5; exit 0; fi\nif [ \"$2\" = show ]; then echo inactive; fi\n", ranPath)
			}
			ctl := writeExecutable(t, root, "ctl", ctlScript)
			started := time.Now()
			done := make(chan error, 1)
			go func() {
				_, err := (SystemdScopeRunner{SystemdRunBinary: run, SystemctlBinary: ctl, ScopeCleanupTimeout: 20 * time.Millisecond}).Run(
					context.Background(), ProcessRequest{ID: "kill-remaining-bound", Dir: root, Program: "true", KillRemaining: true})
				done <- err
			}()
			var err error
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("scope cleanup waited for systemctl's child")
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("scope cleanup took %s despite a 20ms cleanup bound", elapsed.Round(time.Millisecond))
			}
			var cleanupErr *ScopeCleanupError
			if !errors.As(err, &cleanupErr) {
				t.Fatalf("run error = %v, want a scope cleanup failure", err)
			}
			if _, statErr := os.Stat(ranPath); (statErr == nil) != tc.hangAfterRun {
				t.Fatalf("command ran = %v, want %v", statErr == nil, tc.hangAfterRun)
			}
		})
	}
}
