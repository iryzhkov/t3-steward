package providercontainment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// This exercises an actual independent user service. The process is a bounded
// fixture, not an AI provider; kernel boundary tests qualify the separate mount
// launcher. Enable explicitly on a host with a functioning user systemd manager.
func TestSupervisorSystemdSurvivesCallerCancellation(t *testing.T) {
	if os.Getenv("T3_STEWARD_REQUIRE_SUPERVISOR_TESTS") != "1" {
		t.Skip("requires user systemd manager")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("required supervisor qualification needs Linux")
	}
	manager, launch, _ := supervisorFixture(t)
	manager.command = nil
	executable := filepath.Join(t.TempDir(), "supervisor-fixture")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexec /bin/sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	manager.Executable = executable
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	first, err := manager.Start(ctx, launch)
	cancel()
	defer func() {
		ctx, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if _, err := manager.Stop(ctx, launch); err != nil {
			t.Errorf("fixture cleanup: %v", err)
		}
	}()
	if err != nil || first.InvocationID == "" {
		t.Fatalf("start: %+v %v", first, err)
	}
	recovered := Supervisor{Root: manager.Root, Executable: manager.Executable}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	same, err := recovered.Start(ctx, launch)
	if err != nil || same.InvocationID != first.InvocationID || same.State != "active/running" || same.Stopped {
		t.Fatalf("caller cancellation/reconstruction lost process: first=%+v same=%+v err=%v", first, same, err)
	}
	stopped, err := recovered.Stop(ctx, launch)
	if err != nil || !stopped.Stopped {
		t.Fatalf("stop: %+v %v", stopped, err)
	}
}

type supervisorFake struct {
	mu          sync.Mutex
	starts      int
	stops       int
	description string
	missing     bool
	loseStart   bool
	loseStop    bool
}

func (f *supervisorFake) command(_ context.Context, command string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if command == "systemd-run" {
		f.starts++
		for _, arg := range args {
			if strings.HasPrefix(arg, "--description=") {
				f.description = strings.TrimPrefix(arg, "--description=")
			}
		}
		if f.loseStart {
			return nil, errors.New("lost response after accepted launch")
		}
		return nil, nil
	}
	if args[1] == "stop" {
		f.stops++
		if f.loseStop {
			return nil, errors.New("lost stop response")
		}
		return nil, nil
	}
	if f.missing {
		return []byte("LoadState=not-found\n"), nil
	}
	return []byte("LoadState=loaded\nDescription=" + f.description + "\nActiveState=active\nSubState=running\nInvocationID=same-execution\n"), nil
}

func supervisorFixture(t *testing.T) (Supervisor, Launch, *supervisorFake) {
	t.Helper()
	root := t.TempDir()
	// macOS temp paths may have a canonical /private prefix.
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	fake := &supervisorFake{}
	return Supervisor{Root: root, Executable: "/steward", command: fake.command}, Launch{ExecutionID: "execution-one", Spec: Spec{WorkerID: "worker-one", Command: []string{"/bin/true"}}}, fake
}

func TestSupervisorLostLaunchResponseNeverRepeatsExecution(t *testing.T) {
	manager, launch, fake := supervisorFixture(t)
	fake.loseStart = true
	if _, err := manager.Start(context.Background(), launch); err == nil {
		t.Fatal("expected uncertain reply")
	}
	// Reconstruct the manager, as a restarted worker would.
	recovered := Supervisor{Root: manager.Root, Executable: manager.Executable, command: fake.command}
	for i := 0; i < 3; i++ {
		got, err := recovered.Start(context.Background(), launch)
		if err != nil || got.InvocationID != "same-execution" || got.Stopped {
			t.Fatalf("recovery: %+v %v", got, err)
		}
	}
	if fake.starts != 1 {
		t.Fatalf("duplicate launch: %d", fake.starts)
	}
	changed := launch
	changed.Spec.Command = []string{"/bin/false"}
	if _, err := recovered.Start(context.Background(), changed); err == nil {
		t.Fatal("changed execution accepted")
	}
	fake.missing = true
	got, err := recovered.Start(context.Background(), launch)
	if err != nil || got.State != "recovery-required" || got.Stopped || fake.starts != 1 {
		t.Fatalf("missing unit freed/repeated execution: %+v %v", got, err)
	}
}

func TestSupervisorIncompleteIntentNeverLaunches(t *testing.T) {
	manager, launch, fake := supervisorFixture(t)
	_, dir, _, _, err := manager.identity(launch)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(context.Background(), launch); err == nil {
		t.Fatal("incomplete intent accepted")
	}
	if fake.starts != 0 {
		t.Fatal("repeated uncertain effect")
	}
}

func TestSupervisorConcurrentStartsReserveOnce(t *testing.T) {
	manager, launch, fake := supervisorFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = manager.Start(context.Background(), launch) }()
	}
	wg.Wait()
	if fake.starts != 1 {
		t.Fatalf("got %d starts", fake.starts)
	}
	got, err := manager.Observe(context.Background(), launch)
	if err != nil || got.InvocationID == "" {
		t.Fatalf("lost launch: %+v %v", got, err)
	}
}

func TestSupervisorStopReceiptRequiresConfirmedStop(t *testing.T) {
	manager, launch, fake := supervisorFixture(t)
	if _, err := manager.Start(context.Background(), launch); err != nil {
		t.Fatal(err)
	}
	fake.loseStop = true
	got, err := manager.Stop(context.Background(), launch)
	if err == nil || got.Stopped {
		t.Fatalf("false release: %+v %v", got, err)
	}
	fake.loseStop = false
	got, err = manager.Stop(context.Background(), launch)
	if err != nil || !got.Stopped {
		t.Fatalf("stop: %+v %v", got, err)
	}
	fake.missing = true
	recovered := Supervisor{Root: manager.Root, Executable: manager.Executable, command: fake.command}
	got, err = recovered.Start(context.Background(), launch)
	if err != nil || !got.Stopped || fake.starts != 1 {
		t.Fatalf("durable stopped receipt: %+v %v", got, err)
	}
}

func TestSupervisorRefusesForeignUnitAndExposedState(t *testing.T) {
	manager, launch, fake := supervisorFixture(t)
	if _, err := manager.Start(context.Background(), launch); err != nil {
		t.Fatal(err)
	}
	fake.description = "other-execution"
	if _, err := manager.Stop(context.Background(), launch); err == nil {
		t.Fatal("foreign unit stopped")
	}
	if fake.stops != 0 {
		t.Fatal("foreign effect")
	}
	exposed := launch
	exposed.Spec.RuntimePaths = []string{filepath.Dir(manager.Root)}
	if _, err := manager.Start(context.Background(), exposed); err == nil {
		t.Fatal("journal exposed to provider")
	}
}
