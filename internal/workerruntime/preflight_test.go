package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type preflightStubRunner struct {
	output   string
	exit     int
	failure  error
	requests []backlog.ProcessRequest
}

func (r *preflightStubRunner) Run(_ context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	r.requests = append(r.requests, request)
	result := backlog.ProcessResult{Output: r.output, ExitCode: r.exit}
	if r.failure != nil {
		return result, r.failure
	}
	if r.exit != 0 {
		return result, &backlog.ProcessExitError{ExitCode: r.exit, Err: errors.New("stub exit")}
	}
	return result, nil
}

func preflightTestPackage(steps ...workerproto.PreflightStep) workerproto.ExecutionPackage {
	pkg := testPackage()
	if len(steps) != 0 {
		pkg.Preflight = steps
		pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityPreflight}
	}
	return pkg
}

func preflightCheckStep(policy string) workerproto.PreflightStep {
	return workerproto.PreflightStep{
		ID: "unit_tests", Kind: "check", Command: []string{"go", "test", "./..."},
		FailurePolicy: policy, Include: "summary", MaxOutputBytes: 4096, Timeout: time.Minute,
	}
}

func preflightDriver(t *testing.T, pkg workerproto.ExecutionPackage, runner backlog.PreflightRunner) (*LocalDriver, *recordingT3, string) {
	t.Helper()
	root := t.TempDir()
	control := &recordingT3{projectID: "project-uuid", resolveProject: "project-uuid"}
	driver := &LocalDriver{
		Config:    LocalDriverConfig{ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs"), StopTimeout: time.Second},
		T3:        control,
		Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt")},
		Publisher: &recordingPublisher{},
		Preflight: runner,
		Now:       func() time.Time { return runtimeTestNow },
	}
	if _, err := driver.cacheArtifact(context.Background(), pkg.Prompt, pkg.Limits.MaxArtifactBytes); err != nil {
		t.Fatalf("cache prompt: %v", err)
	}
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return driver, control, workspace
}

func TestCreateThreadWithoutPreflightKeepsTheExistingPrompt(t *testing.T) {
	pkg := preflightTestPackage()
	driver, control, workspace := preflightDriver(t, pkg, &preflightStubRunner{})

	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatalf("create thread: %v", err)
	}
	if len(control.created) != 1 || !strings.HasPrefix(control.created[0].Prompt, "prompt\n\n## How this task ends\n") {
		t.Fatalf("a task without preflight must launch with today's prompt, got %+v", control.created)
	}
	if _, err := os.Stat(filepath.Join(driver.preflightDir(pkg), "state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no preflight declared, but state was written: %v", err)
	}
}

func TestCreateThreadRequirePassFailureCreatesNoSession(t *testing.T) {
	pkg := preflightTestPackage(preflightCheckStep("require-pass"))
	runner := &preflightStubRunner{output: "FAIL internal/backlog\n", exit: 2}
	driver, control, workspace := preflightDriver(t, pkg, runner)

	err := driver.CreateThread(context.Background(), pkg, workspace)
	if err == nil || !strings.Contains(err.Error(), "preflight blocked the provider session") {
		t.Fatalf("error = %v, want a blocking preflight", err)
	}
	// The session must never exist, rather than exist and exit.
	if len(control.created) != 0 {
		t.Fatalf("a blocked preflight created %d provider sessions", len(control.created))
	}
	if len(runner.requests) != 1 {
		t.Fatalf("preflight requests = %+v", runner.requests)
	}
	state, err := driver.loadPreflightState(pkg)
	if err != nil || len(state.Receipts) != 1 || state.Receipts[0].ExitCode != 2 {
		t.Fatalf("blocked evidence was not retained: state=%+v err=%v", state, err)
	}
}

func TestCreateThreadUnrunnablePreflightCreatesNoSession(t *testing.T) {
	pkg := preflightTestPackage(preflightCheckStep("record"))
	runner := &preflightStubRunner{failure: errors.New("exec: \"go\": executable file not found")}
	driver, control, workspace := preflightDriver(t, pkg, runner)

	err := driver.CreateThread(context.Background(), pkg, workspace)
	if err == nil || !strings.Contains(err.Error(), "preflight blocked the provider session") {
		t.Fatalf("error = %v, want a blocking preflight", err)
	}
	if len(control.created) != 0 {
		t.Fatalf("an unrunnable preflight created %d provider sessions", len(control.created))
	}
}

func TestCreateThreadRecordFailureLaunchesWithTheFailingBaseline(t *testing.T) {
	pkg := preflightTestPackage(preflightCheckStep("record"))
	runner := &preflightStubRunner{output: "FAIL internal/backlog\n", exit: 2}
	driver, control, workspace := preflightDriver(t, pkg, runner)

	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatalf("create thread: %v", err)
	}
	if len(control.created) != 1 {
		t.Fatalf("record policy must still launch, got %d sessions", len(control.created))
	}
	prompt := control.created[0].Prompt
	for _, want := range []string{"prompt-envelope/v1", "## objective\nprompt", "unit_tests failed exit=2", "FAIL internal/backlog"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt is missing %q:\n%s", want, prompt)
		}
	}
}

func TestPreflightFullOutputIsRetrievableThroughItsReference(t *testing.T) {
	step := workerproto.PreflightStep{
		ID: "status", Kind: "context", Command: []string{"git", "status"},
		FailurePolicy: "record", Include: "reference", MaxOutputBytes: 16, Timeout: time.Minute,
	}
	full := strings.Repeat("modified: internal/backlog/preflight.go\n", 8)
	pkg := preflightTestPackage(step)
	runner := &preflightStubRunner{output: full}
	driver, control, workspace := preflightDriver(t, pkg, runner)

	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatalf("create thread: %v", err)
	}
	state, err := driver.loadPreflightState(pkg)
	if err != nil || len(state.Receipts) != 1 {
		t.Fatalf("state = %+v err = %v", state, err)
	}
	receipt := state.Receipts[0]
	if !receipt.Truncated || len(receipt.Stdout) != 16 {
		t.Fatalf("receipt excerpt = %#v", receipt)
	}
	reference := backlog.PreflightReference(receipt.Identity.StepID, receipt.IdentityDigest)
	if !strings.Contains(control.created[0].Prompt, reference) {
		t.Fatalf("prompt does not carry the reference %q:\n%s", reference, control.created[0].Prompt)
	}
	artifact, path, err := driver.PreflightArtifact(pkg, reference)
	if err != nil {
		t.Fatalf("resolve reference: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stored output: %v", err)
	}
	if string(raw) != full || artifact.Size != int64(len(full)) {
		t.Fatalf("stored output = %d bytes, want the full %d", len(raw), len(full))
	}
}

func TestPreflightReusesAnUnchangedBaselineOnRetry(t *testing.T) {
	pkg := preflightTestPackage(preflightCheckStep("record"))
	runner := &preflightStubRunner{output: "ok\n"}
	driver, control, workspace := preflightDriver(t, pkg, runner)

	if err := driver.CreateThread(context.Background(), pkg, workspace); err != nil {
		t.Fatalf("first attempt: %v", err)
	}
	retry := pkg
	retry.Identity.AttemptID = "attempt-2"
	retry.Identity.AssignmentEpoch = 3
	if err := driver.CreateThread(context.Background(), retry, workspace); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(runner.requests) != 1 {
		t.Fatalf("an unchanged baseline was rerun: %d executions", len(runner.requests))
	}
	if len(control.created) != 2 {
		t.Fatalf("sessions = %d, want one per attempt", len(control.created))
	}

	// A changed input digest is different evidence and must be re-established.
	changed := retry
	changed.StaticInputs = []workerproto.ArtifactObject{testArtifact("input-1", "inputs/context.md", "context")}
	if err := driver.CreateThread(context.Background(), changed, workspace); err != nil {
		t.Fatalf("changed inputs: %v", err)
	}
	if len(runner.requests) != 2 {
		t.Fatalf("changed inputs did not invalidate the receipt: %d executions", len(runner.requests))
	}
}
