package backlog

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func scopeArguments(t *testing.T, runner SystemdScopeRunner, limits ProcessLimits) []string {
	t.Helper()
	root := t.TempDir()
	argsPath := filepath.Join(root, "run.args")
	runner.SystemdRunBinary = writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\n", argsPath))
	runner.SystemctlBinary = writeExecutable(t, root, "systemctl", "#!/bin/sh\nexit 99\n")
	var log bytes.Buffer
	if _, err := runner.Run(context.Background(), ProcessRequest{
		ID: "attempt-1-verify", Dir: root, Program: "/bin/sh", Args: []string{"-c", "true"}, Log: &log, Limits: limits,
	}); err != nil {
		t.Fatalf("run scope: %v", err)
	}
	return strings.Split(strings.TrimSpace(readAbsoluteTestFile(t, argsPath)), "\n")
}

func argumentsBeforeCommand(args []string) []string {
	for i, arg := range args {
		if arg == "--" {
			return args[:i]
		}
	}
	return args
}

func TestScopeRunnerAddsParallelismAndLimits(t *testing.T) {
	delegated := func() (bool, bool) { return true, true }
	environment := func(name string) (string, bool) {
		if name == "GOFLAGS" {
			return "-mod=mod -trimpath", true
		}
		return "", false
	}
	sized := ProcessLimitsFor(domain.ResourceDemand{CPUUnits: 3.5, MemoryMB: 6000})
	args := argumentsBeforeCommand(scopeArguments(t, SystemdScopeRunner{LookupEnv: environment, Controllers: delegated}, sized))
	for _, want := range []string{
		"--setenv=GOMAXPROCS=4",
		"--setenv=GOFLAGS=-mod=mod -trimpath -p=4",
		"--setenv=MAKEFLAGS=-j4",
		"--setenv=CARGO_BUILD_JOBS=4",
		"--property=CPUQuota=400%",
		"--property=MemoryMax=6000M",
		"--property=MemorySwapMax=0",
	} {
		if !containsString(args, want) {
			t.Fatalf("sized scope arguments miss %q:\n%s", want, strings.Join(args, "\n"))
		}
	}

	// Without an existing GOFLAGS the value is the parallelism alone, and a
	// fractional reservation still gets one job.
	light := ProcessLimitsFor(domain.ResourceDemand{CPUUnits: .5})
	args = argumentsBeforeCommand(scopeArguments(t, SystemdScopeRunner{LookupEnv: func(string) (string, bool) { return "", false }, Controllers: delegated}, light))
	for _, want := range []string{"--setenv=GOMAXPROCS=1", "--setenv=GOFLAGS=-p=1", "--setenv=MAKEFLAGS=-j1", "--setenv=CARGO_BUILD_JOBS=1", "--property=CPUQuota=100%"} {
		if !containsString(args, want) {
			t.Fatalf("light scope arguments miss %q:\n%s", want, strings.Join(args, "\n"))
		}
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "--property=Memory") {
			t.Fatalf("scope without a memory size got %q", arg)
		}
	}

	for _, arg := range argumentsBeforeCommand(scopeArguments(t, SystemdScopeRunner{LookupEnv: environment, Controllers: delegated}, ProcessLimits{})) {
		if strings.HasPrefix(arg, "--setenv=") || strings.HasPrefix(arg, "--property=CPUQuota") || strings.HasPrefix(arg, "--property=Memory") {
			t.Fatalf("unsized scope got %q", arg)
		}
	}

	// A user manager without delegated cpu and memory controllers runs the
	// command with the environment and without the properties it cannot honour.
	undelegated := argumentsBeforeCommand(scopeArguments(t, SystemdScopeRunner{LookupEnv: environment, Controllers: func() (bool, bool) { return false, false }}, sized))
	if !containsString(undelegated, "--setenv=GOMAXPROCS=4") {
		t.Fatalf("undelegated scope lost its environment:\n%s", strings.Join(undelegated, "\n"))
	}
	for _, arg := range undelegated {
		if strings.HasPrefix(arg, "--property=CPUQuota") || strings.HasPrefix(arg, "--property=Memory") {
			t.Fatalf("undelegated scope got %q", arg)
		}
	}
}

func TestProcessLimitsTravelInContextAndPrompt(t *testing.T) {
	limits := ProcessLimitsFor(domain.ResourceDemand{CPUUnits: 4, MemoryMB: 6000})
	if got := ProcessLimitsFromContext(WithProcessLimits(context.Background(), limits)); got != limits {
		t.Fatalf("limits from context = %+v", got)
	}
	if got := ProcessLimitsFromContext(context.Background()); got != (ProcessLimits{}) {
		t.Fatalf("empty context limits = %+v", got)
	}
	outputs := []domain.ArtifactDeclaration{{Name: "report.md"}}
	if got := FirstTurnPromptWithLimits("prompt", outputs, ProcessLimits{}); got != FirstTurnPrompt("prompt", outputs) {
		t.Fatalf("unsized prompt changed:\n%s", got)
	}
	got := FirstTurnPromptWithLimits("prompt", outputs, limits)
	if !strings.HasPrefix(got, TaskCompletionSupplement(outputs)+"\n") || !strings.HasSuffix(got, "\n\nprompt") {
		t.Fatalf("sized prompt does not keep the contract first and the prompt last:\n%s", got)
	}
	line := strings.TrimPrefix(strings.TrimSuffix(got, "\n\nprompt"), TaskCompletionSupplement(outputs)+"\n")
	if strings.Contains(line, "\n") {
		t.Fatalf("parallelism instruction is not one line: %q", line)
	}
	for _, want := range []string{"GOMAXPROCS=4", "GOFLAGS=-p=4", "MAKEFLAGS=-j4", "CARGO_BUILD_JOBS=4"} {
		if !strings.Contains(line, want) {
			t.Fatalf("parallelism instruction %q does not name %q", line, want)
		}
	}
}
