package main

import (
	"context"
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
func TestCoordinatorLocalMultiProcessWorkflow(t *testing.T) {
	repositoryRoot := coordinatorTestRepositoryRoot(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tests := strings.Join([]string{
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
	}, "|")
	command := exec.CommandContext(
		ctx, "go", "test",
		"./internal/backlog", "./internal/store/sqlite", "./internal/workerruntime",
		"-run", "^("+tests+")$", "-count=1",
	)
	command.Dir = repositoryRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("disposable coordinator workflow subprocess failed: %v\n%s", err, output)
	}
}

func coordinatorTestRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve coordinator acceptance-test source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	if filepath.Base(root) != "t3-steward" {
		t.Fatalf("unexpected repository root %q", root)
	}
	return root
}
