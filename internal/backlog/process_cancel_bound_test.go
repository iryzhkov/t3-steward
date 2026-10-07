package backlog

import (
	"context"
	"errors"
	"fmt"
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
