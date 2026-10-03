//go:build qualification

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestCoordinatorLocalMultiProcessWorkflow is the S17 production-binding
// acceptance gate. It deliberately executes the workflow and restart suites in
// a separate go test process. Those suites use only disposable SQLite,
// artifact, custody, and workspace roots; the worker suite adds a second
// process behind the authenticated coordinator/worker protocol boundary.
//
// Keeping this as an aggregate gate makes the exact S17 command exercise the
// complete dependency/artifact/verification/pause/resume/retry/schedule path
// together with the persistence/effect replay fences which prevent duplicate
// dispatch after coordinator or worker restart.
//
// It builds only with the qualification tag, which the nightly workflow sets:
// every test it names also runs in the ordinary pass of its own package.
func TestCoordinatorLocalMultiProcessWorkflow(t *testing.T) {
	repositoryRoot := coordinatorTestRepositoryRoot(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	names := []string{
		"TestBacklogV2EndToEndLocalWorkflowHardening",
		"TestFleetCoordinatorWithholdsNewWorkAtFinalQuotaBoundary",
		"TestFleetCoordinatorCommitsPlanAndReplaysLostCommandResponse",
		"TestFleetCoordinatorRecoversLostDispatchAcknowledgementWithoutDuplicateExecution",
		"TestReconcileAssignmentDispatchRecoversLostCreateResponse",
		"TestReconcileAssignmentDispatchRetriesSameIdentityAfterRestart",
		"TestReconcilePendingThrottleCommandsReplaysAdminPauseAfterRestart",
		"TestReconcileThrottleDeliveriesReplaysLostDeliveryWithStableCommand",
		"TestCoordinatorRestartFencesOldClaimsAndRetainsPlan",
		"TestWorkerCommandsAreFencedAcrossWorkerAndCoordinatorRestarts",
		"TestRuntimeRestartAtDurableCommandBoundaries",
		"TestRuntimeRestartReconcilesEveryInFlightBoundary",
		"TestLocalCoordinatorStubWorkerMultiProcess",
	}
	packages := []string{"./internal/backlog", "./internal/store/sqlite", "./internal/workerruntime"}
	requireQualificationTests(t, ctx, repositoryRoot, packages, names)
	arguments := append([]string{"test"}, packages...)
	arguments = append(arguments, "-run", "^("+strings.Join(names, "|")+")$", "-count=1")
	command := exec.CommandContext(ctx, "go", arguments...)
	command.Dir = repositoryRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("disposable coordinator workflow subprocess failed: %v\n%s", err, output)
	}
}

// missingQualificationTests lists the named tests that no package in packages
// defines, using go test -list with the qualification tag. A wrapper that runs
// a name matching nothing would otherwise pass while running nothing.
func missingQualificationTests(ctx context.Context, root string, packages, tests []string) ([]string, error) {
	arguments := append([]string{"test", "-tags", "qualification", "-list", "^(?:" + strings.Join(tests, "|") + ")$"}, packages...)
	command := exec.CommandContext(ctx, "go", arguments...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("list qualification tests: %v\n%s", err, output)
	}
	listed := make(map[string]bool)
	for _, line := range strings.Split(string(output), "\n") {
		listed[strings.TrimSpace(line)] = true
	}
	var missing []string
	for _, test := range tests {
		if !listed[test] {
			missing = append(missing, test)
		}
	}
	return missing, nil
}

// requireQualificationTests fails the test if any named test is missing.
func requireQualificationTests(t *testing.T, ctx context.Context, root string, packages, tests []string) {
	t.Helper()
	missing, err := missingQualificationTests(ctx, root, packages, tests)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("qualification names tests that %v do not define: %v", packages, missing)
	}
}

// The wrappers' guard reports a name that matches no test.
func TestMissingQualificationTestsReportsUnknownNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	missing, err := missingQualificationTests(ctx, coordinatorTestRepositoryRoot(t), []string{"./internal/workerproto"},
		[]string{"TestExchangeValidationAndIdempotency", "TestNoSuchQualificationTest"})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != "TestNoSuchQualificationTest" {
		t.Fatalf("missing = %v, want only the unknown name", missing)
	}
}

func coordinatorTestRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve coordinator acceptance-test source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	// Worktrees may carry any directory name; the module file identifies the root.
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("unexpected repository root %q: %v", root, err)
	}
	return root
}
