#!/bin/sh
# Runs the tests that used to fail only on a loaded host, repeatedly and under
# the race detector, while busy loops saturate the CPUs the tests run on.
#
#   STRESS_CPUS   CPU list the tests and the load share (taskset syntax);
#                 default 0-3. Without taskset the load spreads over all CPUs.
#   STRESS_LOAD   busy loops to start; default twice the CPUs in STRESS_CPUS.
#   STRESS_COUNT  -count for the cleanup tests; default 20.
#   STRESS_PHASE  growth, bounds or all (default): the growth-bounded and
#                 cleanup tests, the tests with a scaled absolute bound, or
#                 both.
#
# It is not part of `make test`; `make test-stress` runs it.
set -eu

cpus=${STRESS_CPUS:-0-3}
count=${STRESS_COUNT:-20}
pin=
if command -v taskset >/dev/null 2>&1; then
	pin="taskset -c $cpus"
fi
# nproc counts the CPUs this process may run on, so under taskset it counts
# STRESS_CPUS.
width=$($pin nproc)
load=${STRESS_LOAD:-$((width * 2))}

loops=
cleanup() {
	# shellcheck disable=SC2086
	[ -z "$loops" ] || kill $loops 2>/dev/null || true
}
trap cleanup EXIT INT TERM
i=0
while [ "$i" -lt "$load" ]; do
	$pin sh -c 'while :; do :; done' &
	loops="$loops $!"
	i=$((i + 1))
done
if [ -n "$pin" ]; then
	echo "stress: $load busy loops on CPUs $cpus"
else
	echo "stress: $load busy loops on all CPUs"
fi

run() {
	echo "stress: go test -race $*"
	# shellcheck disable=SC2086
	GOMAXPROCS=$width $pin go test -race "$@"
}

phase=${STRESS_PHASE:-all}

if [ "$phase" = all ] || [ "$phase" = growth ]; then
	run -count="$count" -run 'TestForgedRecordInAnOrdinaryOutputPublishesNothing|TestStagedImportRetriesAfterAFailedPush|TestWorkerGitCommandsKeepMaintenanceInTheForeground' ./internal/backlog
	run -count=5 ./internal/testtiming
	run -count=5 -run 'TestSecretScanRedaction(LinearInMatches|GrowthBoundRejectsQuadraticRedaction)' ./internal/workerruntime
	run -count=5 -run 'TestCommitOutput(ValidationIsLinear|GrowthBoundRejectsAScanPerMark)' ./internal/workerproto
fi

# The tests whose absolute bound is scaled by testtiming.Bound.
if [ "$phase" = all ] || [ "$phase" = bounds ]; then
	run -count=3 -run 'TestRepositoryProbeIsBoundedForAWorkerThatNeverAnswers|TestRepositoryRefResolutionTimeout|TestCoordinatorLedgerNeverBlocksTheBoundary|TestQuotaTelemetryRecorderNeverBlocksCoordinator|TestDaemonsWaitForT3DiscoveryAtStart' ./cmd/t3-steward
	run -count=3 -run 'TestFileLockWaitHonorsContext|TestSystemdScopeRunnerKillRemainingDirectClearScope20ms|TestResolveExactRefTimesOut|TestCancelledRunReturns|TestKillRemainingCleanupReturnsWithinBound|TestWorkspacePreparerTimesOutAndCleansSetup' ./internal/backlog
	run -count=3 -run 'TestSSHArtifactPartialReadCloseAborts' ./internal/backlogadmin
	run -count=3 -run 'TestEnsureProjectCreationBudgetIsBounded' ./internal/control/t3
	run -count=3 -run 'TestCommandSinkTimeoutKillsTheProcessGroup' ./internal/ownernotify
	run -count=3 ./internal/procgroup
	run -count=3 -run 'TestQuotaTelemetrySourceIsQueryOnly' ./internal/store/sqlite
	run -count=3 -run 'TestAwaitURL' ./internal/t3api
	run -count=3 -run 'TestCommandWait|TestNodeSummaryStuckReadSpoilsOnlyItsCell|TestNodeWakeSummaryDegradesWithoutBlockingDelivery' ./internal/wait
	run -count=3 -run 'TestWorkerProbeTimesOutRatherThanHanging|TestWorkerRefResolutionStructuredOutcomes|TestHungObservationsAreBoundedAndEveryAttemptIsProbed|TestRefusedFirstTurnStartFailsTheAttemptWithT3sReason|TestWatchdogDrainDoesNotWaitAcrossAttempts|TestSnapshotStopsGitThatWaitsOnTheWorkTree' ./internal/workerruntime
fi
