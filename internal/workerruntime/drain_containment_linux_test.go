//go:build linux

package workerruntime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/testutil"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestDrainSocketOwnershipDescriptorCloseOnExec(t *testing.T) {
	owner, err := LockWorkerSocket(filepath.Join(t.TempDir(), "worker.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, owner.Fd(), syscall.F_GETFD, 0)
	if errno != 0 || flags&syscall.FD_CLOEXEC == 0 {
		t.Fatalf("ownership descriptor not close-on-exec: flags=%d err=%v", flags, errno)
	}
}

// Supervisor's command boundary is replaced locally; the processes it stops
// are real isolated process groups, including descendants. No service is used.
func TestDrainForcedContainmentStopsAllProcessesDespiteCaptureFailure(t *testing.T) {
	for _, tc := range []struct {
		name           string
		timeout        bool
		captureFailure bool
	}{
		{name: "capture-and-refusal", captureFailure: true},
		{name: "capture-and-deadline", timeout: true, captureFailure: true},
		{name: "isolated-deadline", timeout: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			root := testutil.RealTempDir(t)
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			t.Setenv("T3_DRAIN_TEST_ROOT", root)
			t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
			for name, script := range map[string]string{
				"systemd-run": "#!/bin/sh\nexit 0\n",
				"systemctl": `#!/bin/bash
for arg in "$@"; do [[ "$arg" == t3-contained-* ]] && unit="$arg"; done
dir="$T3_DRAIN_TEST_ROOT/$unit"
if [[ " $* " == *" stop "* ]]; then
  if [[ -f "$dir/refuse" ]]; then
    if [[ "$(cat "$dir/refuse")" == timeout ]]; then exec sleep 2; fi
    exit 1
  fi
  kill -KILL -- "-$(cat "$dir/process.pid")" || exit 1
  exit 0
fi
digest="$(sha256sum "$dir/intent.json" | cut -d' ' -f1)"
printf 'LoadState=loaded\nDescription=t3-containment:%s\nActiveState=active\nSubState=running\nInvocationID=test\n' "$digest"
`,
			} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: root, Executable: "/bin/true"}, Timeout: 500 * time.Millisecond}
			pkg := testPackage()
			path, err := manager.recordPath(pkg)
			if err != nil {
				t.Fatal(err)
			}
			type ownedProcess struct {
				cmd    *exec.Cmd
				child  int
				launch providercontainment.Launch
				dir    string
			}
			var processes []ownedProcess
			for _, id := range []string{pkg.Identity.ThreadID, pkg.Identity.ThreadID + ":verify-1", pkg.Identity.ThreadID + ":verify-2"} {
				launch := providercontainment.Launch{ExecutionID: id, Spec: providercontainment.Spec{WorkerID: pkg.WorkerID}}
				if _, err := manager.Supervisor.Start(ctx, launch); err != nil {
					t.Fatal(err)
				}
				key := sha256.Sum256([]byte(pkg.WorkerID + "\x00" + id))
				dir := filepath.Join(root, "t3-contained-"+hex.EncodeToString(key[:])+".service")
				cmd := exec.Command("sh", "-c", "sleep 30 & echo $!; wait")
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					_ = cmd.Wait()
				})
				scanner := bufio.NewScanner(stdout)
				if !scanner.Scan() {
					t.Fatal("child did not report descendant")
				}
				child, err := strconv.Atoi(scanner.Text())
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "process.pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
					t.Fatal(err)
				}
				plan := containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: launch}
				suffix := ".verify." + id[len(pkg.Identity.ThreadID):]
				if id == pkg.Identity.ThreadID {
					suffix = ".preparation"
				}
				if err := privateJSON(path+suffix, plan); err != nil {
					t.Fatal(err)
				}
				processes = append(processes, ownedProcess{cmd: cmd, child: child, launch: launch, dir: dir})
			}
			// Corrupt capture must keep the acknowledgement fail closed, but must not
			// keep provider or unrelated verifier processes running.
			if tc.captureFailure {
				if err := os.WriteFile(path+".capture", []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := privateJSON(path+".capture", containedCapture{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Archive: []byte("{}")}); err != nil {
				t.Fatal(err)
			}
			var refusal []byte
			if tc.timeout {
				refusal = []byte("timeout")
			}
			if err := os.WriteFile(filepath.Join(processes[1].dir, "refuse"), refusal, 0o600); err != nil {
				t.Fatal(err)
			}
			expired, cancel := context.WithCancel(ctx)
			cancel()
			requestCtx := ctx
			if tc.captureFailure {
				requestCtx = expired
			}
			started := time.Now()
			if err := manager.Quiesce(requestCtx, pkg, true); err == nil {
				t.Fatal("capture/stop failure was acknowledged")
			}
			if tc.timeout && time.Since(started) < manager.Timeout {
				t.Fatal("timeout owner did not exhaust its bounded stop opportunity")
			}
			for i, process := range processes {
				obs, err := manager.Supervisor.Observe(ctx, process.launch)
				if err != nil {
					t.Fatal(err)
				}
				if obs.Stopped != (i != 1) {
					t.Fatalf("independent containment %d: %+v", i, obs)
				}
				if i != 1 {
					assertDrainProcessGone(t, process.child)
				}
			}
			if err := os.Remove(filepath.Join(processes[1].dir, "refuse")); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path + ".capture"); err != nil {
				t.Fatal(err)
			}
			if err := privateJSON(path+".capture", containedCapture{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Archive: []byte("{}")}); err != nil {
				t.Fatal(err)
			}
			if err := manager.Quiesce(ctx, pkg, true); err != nil {
				t.Fatal(err)
			}
			for _, process := range processes {
				assertDrainProcessGone(t, process.child)
				obs, err := manager.Supervisor.Observe(ctx, process.launch)
				if err != nil || !obs.Stopped {
					t.Fatalf("durable stop receipt missing: %+v %v", obs, err)
				}
			}
			// Reopening the owning manager must replay exact receipts, not issue a
			// second signal against a potentially reused process id.
			manager = ContainedT3{Supervisor: providercontainment.Supervisor{Root: root, Executable: "/bin/true"}}
			if err := manager.Quiesce(ctx, pkg, true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Only collection is faked: the pre-dispatch stop traverses the real local
// driver and contained manager rooted by service construction.
type drainPreparationDriver struct {
	*fakeDriver
	local *LocalDriver
}

func (d drainPreparationDriver) StopPreparation(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	return d.local.StopPreparation(ctx, pkg)
}

func TestDrainBeforeFirstContainedPreparation(t *testing.T) {
	ctx := context.Background()
	settings := testWorkerServiceSettings(t)
	service, err := NewWorkerService(ctx, WorkerServiceOptions{
		Settings: settings, WorkerID: "normandy", WorkerEpoch: "worker-1", T3: &recordingT3{},
		CoordinatorEpoch: 9, ProtocolCredentials: &staticProtocolResolver{credentials: testProtocolCredentials()},
		Now: func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	local := service.Exchange.Runtime.driver.(*LocalDriver)
	manager := local.ScopedT3.(ContainedT3)
	root := manager.Supervisor.Root
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("service did not initialize private root: %v %v", info, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("service started execution before claims: %v %v", entries, err)
	}
	runtime := newClaimedRuntime(t, t.TempDir(), &fakeDriver{})
	runtime.driver = drainPreparationDriver{fakeDriver: &fakeDriver{}, local: local}
	if err := runtime.journal.update(func(state *journalState) error {
		record := state.Attempts["assignment-1"]
		record.Package.Package.Environment.DirectoryBindings = []directoryresource.Binding{{}}
		state.Attempts["assignment-1"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	command := testCommand(t, runtime, domain.WorkerCommandStop, "stop-before-preparation")
	acks, err := runtime.DeliverCommands(ctx, workerproto.CommandDelivery{Commands: []domain.WorkerCommand{command}})
	if err != nil || len(acks.Acknowledgements) != 1 || !acks.Acknowledgements[0].Accepted || acks.Acknowledgements[0].Detail != "" {
		t.Fatalf("preparation stop deferred: %+v %v", acks, err)
	}
	if err := runtime.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if record.Phase != PhaseFailed {
		t.Fatalf("never-started stop did not settle: %+v", record)
	}
	// Removal of initialized custody must remain an error, never a proof that
	// the owned process did not start.
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if err := local.StopPreparation(ctx, record.Package.Package); err == nil {
		t.Fatal("missing owned storage was treated as stopped")
	}
}

func assertDrainProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		// A killed orphan can remain a zombie until init reaps it; it cannot
		// execute or retain file descriptors.
		if os.IsNotExist(err) || (err == nil && strings.Contains(string(raw), ") Z ")) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant %d remains executable", pid)
		}
		time.Sleep(time.Millisecond)
	}
}
