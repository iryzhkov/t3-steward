package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

func TestContainedVerificationPreservesTinyGateTimeout(t *testing.T) {
	verifier, workspace := containedVerificationFixture(t)
	plan, err := verifier.manager.preparation(verifier.pkg)
	if err != nil {
		t.Fatal(err)
	}
	spec := plan.Launch.Spec
	spec.Control, spec.ControlPort, spec.ProviderHosts = nil, 0, nil
	spec.Command = []string{"/usr/bin/timeout", "--kill-after=5s", "0.000001000s", "/bin/sh", "-c", "true"}
	launch := providercontainment.Launch{ExecutionID: verifier.pkg.Identity.ThreadID + ":verify-tiny-gate-0", Spec: spec}
	seedContainedVerificationResult(t, verifier.manager.Supervisor, launch, 0)
	_, err = verifier.Run(context.Background(), backlog.ProcessRequest{ID: "verify-tiny-gate-0", Dir: workspace, Program: "/bin/sh", Args: []string{"-c", "true"}, Timeout: time.Microsecond})
	if err != nil {
		t.Fatalf("tiny bounded durable timeout rejected: %v", err)
	}
	if got := containedVerificationTimeout(time.Nanosecond); got != "0.000000001s" {
		t.Fatalf("tiny deadline=%s", got)
	}
	if got := containedVerificationTimeout(time.Second); got != "1.000s" {
		t.Fatalf("existing durable invocation changed: %s", got)
	}
}

func containedVerificationFixture(t *testing.T) (containedVerifier, string) {
	t.Helper()
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	pkg := testPackage()
	pkg.Limits.VerificationTimeout = time.Minute
	manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: filepath.Join(root, "journal"), Executable: "/bin/true"}}
	launch := providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID, Spec: providercontainment.Spec{
		WorkerID: pkg.WorkerID, Directories: pkg.Environment.DirectoryBindings,
		Workspace: directoryresource.Identity{Registration: directoryresource.Registration{Path: workspace}},
	}}
	seedContainedVerificationResult(t, manager.Supervisor, launch, 0)
	path, err := manager.recordPath(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err = privateJSON(path+".preparation", containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: launch}); err != nil {
		t.Fatal(err)
	}
	return containedVerifier{manager: manager, pkg: pkg}, workspace
}

// Seed a stopped, durable supervisor invocation; no systemd or live service is used.
// Start must replay this exact launch, and Result must prove custody from its receipt.
func seedContainedVerificationResult(t *testing.T, supervisor providercontainment.Supervisor, launch providercontainment.Launch, code int) {
	t.Helper()
	key := sha256.Sum256([]byte(launch.Spec.WorkerID + "\x00" + launch.ExecutionID))
	dir := filepath.Join(supervisor.Root, "t3-contained-"+hex.EncodeToString(key[:])+".service")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	intent, err := json.Marshal(struct {
		Launch     providercontainment.Launch `json:"launch"`
		Executable string                     `json:"executable"`
	}{launch, supervisor.Executable})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(intent)
	digest := hex.EncodeToString(sum[:])
	for name, data := range map[string][]byte{"intent.json": intent, "stopped": []byte(digest)} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	result, err := json.Marshal(providercontainment.CommandResult{Digest: digest, InvocationID: "fixture-invocation", ExitCode: code})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "command-result.json"), result, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizerContainedVerificationUmaskContract(t *testing.T) {
	verifier, workspace := containedVerificationFixture(t)
	command := "printf 'literal $0 and ; characters'"
	plan, err := verifier.manager.preparation(verifier.pkg)
	if err != nil {
		t.Fatal(err)
	}
	spec := plan.Launch.Spec
	spec.Control, spec.ControlPort, spec.ProviderHosts = nil, 0, nil
	spec.Command = []string{"/usr/bin/timeout", "--kill-after=5s", "60.000s", "/bin/sh", "-c", `umask 022 && exec "$0" "$@"`, "/bin/sh", "-c", command}
	launch := providercontainment.Launch{ExecutionID: verifier.pkg.Identity.ThreadID + ":verify-attempt-1-0", Spec: spec}
	seedContainedVerificationResult(t, verifier.manager.Supervisor, launch, 0)
	storage := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.Walk(storage, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() {
				return os.Chmod(path, 0700)
			}
			return err
		})
	})
	finalized, err := (backlog.AttemptFinalizer{StorageRoot: filepath.Join(storage, "artifacts"), Processes: verifier}).Finalize(context.Background(), backlog.AttemptFinalization{
		Task:         domain.Task{ID: "task-1", Name: "test", Verification: []string{command}},
		Attempt:      domain.Attempt{ID: "attempt-1", TaskID: "task-1", WorkflowRunID: "run-1"},
		WorkspaceDir: workspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("contained Finalize rejected its verification wrapper: %v", err)
	}
	if !finalized.Completion.VerificationPassed || finalized.Completion.Failure != "" {
		t.Fatalf("completion=%+v", finalized.Completion)
	}
	if len(finalized.Artifacts) != 1 || finalized.Artifacts[0].Kind != domain.ArtifactVerification {
		t.Fatalf("artifacts=%+v", finalized.Artifacts)
	}
	// The durable launch remains byte-for-byte bound to the exact wrapper and command.
	recorded, err := os.ReadFile(filepath.Join(verifier.manager.Supervisor.Root, unitForContainedFixture(launch), "intent.json"))
	if err != nil {
		t.Fatal(err)
	}
	var actual struct {
		Launch providercontainment.Launch `json:"launch"`
	}
	if err = json.Unmarshal(recorded, &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual.Launch.Spec.Command, spec.Command) {
		t.Fatalf("command=%q", actual.Launch.Spec.Command)
	}
}

func TestContainedVerificationUsesBoundedGateTimeout(t *testing.T) {
	verifier, workspace := containedVerificationFixture(t)
	plan, err := verifier.manager.preparation(verifier.pkg)
	if err != nil {
		t.Fatal(err)
	}
	spec := plan.Launch.Spec
	spec.Control, spec.ControlPort, spec.ProviderHosts = nil, 0, nil
	spec.Command = []string{"/usr/bin/timeout", "--kill-after=5s", "2.000s", "/bin/sh", "-c", "true"}
	seedContainedVerificationResult(t, verifier.manager.Supervisor, providercontainment.Launch{
		ExecutionID: verifier.pkg.Identity.ThreadID + ":verify-test-gate-0", Spec: spec,
	}, 0)
	_, err = verifier.Run(context.Background(), backlog.ProcessRequest{
		ID: "verify-test-gate-0", Dir: workspace, Program: "/bin/sh", Args: []string{"-c", "true"}, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("gate timeout not honored: %v", err)
	}
	for _, timeout := range []time.Duration{-time.Second, 2 * time.Minute} {
		_, err = verifier.Run(context.Background(), backlog.ProcessRequest{
			ID: "verify-invalid-gate", Dir: workspace, Program: "/bin/sh", Args: []string{"-c", "true"}, Timeout: timeout,
		})
		if err == nil {
			t.Fatalf("accepted invalid gate timeout %v", timeout)
		}
	}
}

func TestFinalizerContainedGateRetainsSummaryAndFailure(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			verifier, workspace := containedVerificationFixture(t)
			for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-qm", "fixture"}} {
				command := exec.Command("git", args...)
				command.Dir = workspace
				if out, err := command.CombinedOutput(); err != nil {
					t.Fatalf("fixture git: %s %v", out, err)
				}
			}
			plan, err := verifier.manager.preparation(verifier.pkg)
			if err != nil {
				t.Fatal(err)
			}
			spec := plan.Launch.Spec
			spec.Control, spec.ControlPort, spec.ProviderHosts = nil, 0, nil
			spec.Command = []string{"/usr/bin/timeout", "--kill-after=5s", "2.000s", "/bin/sh", "-c", `umask 022 && exec "$0" "$@"`, "/bin/sh", "-c", "true"}
			seedContainedVerificationResult(t, verifier.manager.Supervisor, providercontainment.Launch{ExecutionID: verifier.pkg.Identity.ThreadID + ":verify-attempt-1-gate-0", Spec: spec}, code)
			storage := t.TempDir()
			t.Cleanup(func() {
				_ = filepath.Walk(storage, func(path string, info os.FileInfo, err error) error {
					if err == nil && info.IsDir() {
						return os.Chmod(path, 0700)
					}
					return err
				})
			})
			finalizer := backlog.AttemptFinalizer{StorageRoot: storage, Processes: verifier, GateTimeoutMax: time.Minute, GateContained: true, GateToolchainIdentity: "contained unavailable"}
			result, err := finalizer.Finalize(context.Background(), backlog.AttemptFinalization{
				Task:    domain.Task{ID: "task-1", Name: "test", Gate: &domain.TaskGate{Commands: []string{"true"}, Timeout: 2 * time.Second}},
				Attempt: domain.Attempt{ID: "attempt-1", TaskID: "task-1", WorkflowRunID: "run-1"}, WorkspaceDir: workspace, ExplicitSuccess: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if (result.Completion.Failure == "") != (code == 0) {
				t.Fatalf("completion=%+v", result.Completion)
			}
			names := map[string]bool{}
			for _, artifact := range result.Artifacts {
				names[artifact.Name] = true
				data, err := os.ReadFile(filepath.Join(storage, artifact.StoragePath))
				if err != nil {
					t.Fatal(err)
				}
				if artifact.Name == "gate/log.txt" && !strings.Contains(string(data), "fixture-invocation") {
					t.Fatalf("summary log=%s", data)
				}
				if artifact.Name == "gate" {
					var report backlog.GateReport
					if err = json.Unmarshal(data, &report); err != nil {
						t.Fatal(err)
					}
					if report.OutputLimitation == "" || report.Passed != (code == 0) || len(report.Commands) != 1 || report.Commands[0].ExitCode != code {
						t.Fatalf("report=%+v", report)
					}
				}
			}
			if !names["gate"] || !names["gate/log.txt"] {
				t.Fatalf("artifacts=%+v", result.Artifacts)
			}
		})
	}
}

func unitForContainedFixture(launch providercontainment.Launch) string {
	key := sha256.Sum256([]byte(launch.Spec.WorkerID + "\x00" + launch.ExecutionID))
	return "t3-contained-" + hex.EncodeToString(key[:]) + ".service"
}

func TestContainedVerificationRejectsOtherWrappers(t *testing.T) {
	for _, args := range [][]string{
		{"-c", `umask 022 && exec "$0" "$@"`, "/bin/bash", "-c", "true"},
		{"-c", `umask 000 && exec "$0" "$@"`, "/bin/sh", "-c", "true"},
		{"-c", `umask 022 && exec "$0" "$@"`, "/bin/sh", "-x", "true"},
		{"-c", `umask 022 && exec "$0" "$@"`, "/bin/sh", "-c", "true", "extra"},
	} {
		verifier, workspace := containedVerificationFixture(t)
		_, err := verifier.Run(context.Background(), backlog.ProcessRequest{ID: "verify-test", Dir: workspace, Program: "/bin/sh", Args: args})
		if err == nil {
			t.Fatalf("accepted unsupported args %q", args)
		}
	}
}

func TestContainedVerificationPlainShellContract(t *testing.T) {
	verifier, workspace := containedVerificationFixture(t)
	plan, err := verifier.manager.preparation(verifier.pkg)
	if err != nil {
		t.Fatal(err)
	}
	spec := plan.Launch.Spec
	spec.Control, spec.ControlPort, spec.ProviderHosts = nil, 0, nil
	spec.Command = []string{"/usr/bin/timeout", "--kill-after=5s", "60.000s", "/bin/sh", "-c", "true"}
	seedContainedVerificationResult(t, verifier.manager.Supervisor, providercontainment.Launch{ExecutionID: verifier.pkg.Identity.ThreadID + ":verify-test", Spec: spec}, 0)
	if _, err = verifier.Run(context.Background(), backlog.ProcessRequest{ID: "verify-test", Dir: workspace, Program: "/bin/sh", Args: []string{"-c", "true"}}); err != nil {
		t.Fatal(err)
	}
}
