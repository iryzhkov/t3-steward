package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// Bounding systemctl's output pipes only applies once the cleanup context
// ends: a systemctl that answers and exits while a child it left behind still
// holds its output has still answered, so a scope it reports inactive is
// cleared rather than reported as a containment failure.
func TestKillRemainingCleanupAcceptsAnAnswerWhoseChildHoldsOutput(t *testing.T) {
	root := t.TempDir()
	run := writeExecutable(t, root, "run", "#!/bin/sh\nexit 0\n")
	ctl := writeExecutable(t, root, "ctl", "#!/bin/sh\nif [ \"$2\" = show ]; then echo inactive; fi\nsleep 0.3 &\nexit 0\n")
	result, err := (SystemdScopeRunner{SystemdRunBinary: run, SystemctlBinary: ctl, ScopeCleanupTimeout: 5 * time.Second}).Run(
		context.Background(), ProcessRequest{ID: "kill-remaining-linger", Dir: root, Program: "true", KillRemaining: true})
	if err != nil {
		t.Fatalf("run error = %v (output %q), want the scope cleared", err, result.Output)
	}
}

// Startup and pipe ownership are established before the 20ms operation begins.
func TestKillRemainingCleanupReturnsWithinBoundWhenSystemctlChildHoldsOutput(t *testing.T) {
	for _, delay := range []string{"0", "0.2"} {
		t.Run("startup-delay="+delay, func(t *testing.T) {
			root := t.TempDir()
			childPath := filepath.Join(root, "child.pid")
			releasePath := filepath.Join(root, "release")
			statusPath := filepath.Join(root, "write.status")
			child := writeExecutable(t, root, "child", fmt.Sprintf("#!/bin/sh\ntrap '' PIPE\nprintf '%%s' \"$$\" > %q\necho held-output-ready\nwhile [ ! -e %q ]; do sleep 0.01; done\necho late 2>/dev/null\necho $? > %q.tmp && mv %q.tmp %q\nexec sleep 30\n", childPath, releasePath, statusPath, statusPath, statusPath))
			ctl := writeExecutable(t, root, "ctl", fmt.Sprintf("#!/bin/bash\nset -m\nsleep %s\n%q &\nwait\n", delay, child))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			output := &heldOutputObserver{ready: make(chan struct{})}
			command := (SystemdScopeRunner{SystemctlBinary: ctl}).systemctlCommand(ctx, "--user", "kill", "--kill-who=all", "--signal=KILL", "t3-steward-held-output.scope")
			command.Stdout, command.Stderr = output, output
			setup := time.Now()
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			defer func() {
				cancel()
				if !waited {
					_ = command.Wait()
				}
			}()
			pid := waitForPID(t, childPath)
			t.Cleanup(func() {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				waitForProcessExit(t, pid)
			})
			select {
			case <-output.ready:
			case <-time.After(5 * time.Second):
				t.Fatal("systemctl output never observed held-output-ready")
			}
			parentGroup, parentErr := syscall.Getpgid(command.Process.Pid)
			group, groupErr := syscall.Getpgid(pid)
			if parentErr != nil || groupErr != nil || group != pid || group == parentGroup {
				t.Fatalf("held-output ownership: child group=%d parent group=%d errors=%v/%v", group, parentGroup, groupErr, parentErr)
			}
			if delay == "0.2" && time.Since(setup) < 200*time.Millisecond {
				t.Fatal("startup delay control did not delay setup")
			}
			t.Logf("held-output-ready: live child=%d separate group=%d setup=%s", pid, group, time.Since(setup))
			// Rescue a broken implementation without letting its Wait goroutine
			// or the escaped child survive the test. This is beyond the 1s bound.
			rescue := time.AfterFunc(2*time.Second, func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })
			defer rescue.Stop()
			started := time.Now()
			timer := time.AfterFunc(20*time.Millisecond, cancel)
			err := command.Wait()
			waited = true
			timer.Stop()
			elapsed := time.Since(started)
			if elapsed > time.Second {
				t.Fatalf("held-output Wait took %s despite a 20ms cancellation bound", elapsed.Round(time.Millisecond))
			}
			if ctx.Err() != context.Canceled || err == nil || command.ProcessState == nil {
				t.Fatalf("systemctl Wait error=%v context=%v state=%v, want reaped cancelled parent", err, ctx.Err(), command.ProcessState)
			}
			if group, err := syscall.Getpgid(pid); err != nil || group != pid {
				t.Fatalf("child did not survive parent cancellation and pipe closure: group=%d err=%v", group, err)
			}
			if err := os.WriteFile(releasePath, nil, 0600); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				status, err := os.ReadFile(statusPath)
				if err == nil {
					if strings.TrimSpace(string(status)) == "0" {
						t.Fatal("held-output child still wrote after systemctl Wait returned")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("live held-output child never reported closed pipe: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Logf("actual Wait=%s; live child observed closed output pipe", elapsed)
		})
	}
}

// exec copies stdout and stderr concurrently; readiness observes actual output
// without racing the command's copy goroutines.
type heldOutputObserver struct {
	mu    sync.Mutex
	text  strings.Builder
	ready chan struct{}
	once  sync.Once
}

func (o *heldOutputObserver) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.text.Write(p)
	if strings.Contains(o.text.String(), "held-output-ready\n") {
		o.once.Do(func() { close(o.ready) })
	}
	return len(p), nil
}
