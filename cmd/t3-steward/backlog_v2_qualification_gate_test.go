//go:build qualification

package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestBacklogV2ProductionQualification is the repeatable, no-external-effects
// portion of the S19 gate. Each group runs in a fresh process and delegates to
// tests that create only temporary coordinator, worker, credential, artifact,
// bundle, workspace, and backup roots. The worker topology group also starts a
// separate restricted worker process behind the authenticated protocol.
//
// The authorized multi-host observation and canary are deliberately not part
// of this test: they require operator-selected hosts and credentials.
//
// It builds only with the qualification tag (go test -tags qualification),
// which the nightly workflow sets: every test it names also runs in the
// ordinary pass, so repeating them in nested go test processes adds only the
// fresh-process grouping and several recompiles to every pull request.
func TestBacklogV2ProductionQualification(t *testing.T) {
	repositoryRoot := coordinatorTestRepositoryRoot(t)
	groups := []struct {
		name     string
		packages []string
		tests    []string
	}{
		{
			name:     "separate_process_topology",
			packages: []string{"./cmd/t3-steward", "./internal/workerruntime"},
			tests: []string{
				"TestCoordinatorLocalMultiProcessWorkflow",
				"TestRunBacklogV2CoordinatorServesAuthenticatedLocalAdmin",
				"TestRunBacklogV2CoordinatorAcceptsNativeArchiveSubmissionAndReplay",
				"TestLocalCoordinatorStubWorkerMultiProcess",
			},
		},
		{
			name:     "transport_loss_reorder_duplicate_and_clock_skew",
			packages: []string{"./internal/workerproto"},
			tests: []string{
				"TestClientPoisonsAmbiguousSessionAndRejectsWrongSnapshot",
				"TestExchangeValidationAndIdempotency",
				"TestSSHTransportDropRetryTimeoutCancellationAndLimits",
				"TestSSHArtifactTransportAuthenticatesMetadataAndBoundsRawSuffix",
			},
		},
		{
			name:     "restart_replay_and_rollback",
			packages: []string{"./internal/backlog", "./internal/backlogadmin", "./internal/store/sqlite", "./internal/workerruntime"},
			tests: []string{
				"TestFleetCoordinatorCommitsPlanAndReplaysLostCommandResponse",
				"TestFleetCoordinatorRecoversLostDispatchAcknowledgementWithoutDuplicateExecution",
				"TestReconcileAssignmentDispatchRetriesSameIdentityAfterRestart",
				"TestExecutePendingCommandsSurvivesRestartAndReplaysRetry",
				"TestCoordinatorRestartFencesOldClaimsAndRetainsPlan",
				"TestAssignmentPlanRollsBackWhenAnyAttemptIsStale",
				"TestRuntimeRestartReconcilesEveryInFlightBoundary",
				"TestProtocolServerUsesReplayStoreAcrossRestart",
			},
		},
		{
			name:     "stale_quota_schedule_and_legacy_exclusion",
			packages: []string{"./cmd/t3-steward", "./internal/backlog"},
			tests: []string{
				"TestRunBacklogV2RefusesLegacyCoordinatorOverlapBeforeStateOpen",
				"TestCoordinatorQuotaReconcilerPersistsClosedAdmissionWithoutEvidence",
				"TestQuotaBridgeDeduplicatesSharedPoolAndFailsClosedWhenStale",
				"TestScheduleTimerCatchUpOverlapAndRestart",
			},
		},
		{
			name:     "artifact_corruption_backup_restore_and_recovery",
			packages: []string{"./cmd/t3-steward", "./internal/backlog", "./internal/backupsnapshot", "./internal/store/sqlite", "./internal/workerruntime"},
			tests: []string{
				"TestImportCoordinatorWorkerResultDoesNotAcknowledgeCorruptFetch",
				"TestCoordinatorArtifactsTransferAcrossWorkerRestartAndVerifyChecksum",
				"TestSnapshotBackupRestoreRoundTrip",
				"TestSnapshotVerifyRefusesCorruptIncompleteAndMismatched",
				"TestUnknownRecoveryIsEvidenceRevisionAndReplayFenced",
				"TestStaleEpochAndCorruptArtifactFailClosed",
			},
		},
	}
	for _, group := range groups {
		group := group
		t.Run(group.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			expression := "^(?:" + strings.Join(group.tests, "|") + ")$"
			arguments := append([]string{"test"}, group.packages...)
			// The nested runs carry the qualification tag, so the
			// separate_process_topology group reaches
			// TestCoordinatorLocalMultiProcessWorkflow, which is gated the same way.
			arguments = append(arguments, "-tags", "qualification", "-run", expression, "-count=1")
			command := exec.CommandContext(ctx, "go", arguments...)
			command.Dir = repositoryRoot
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("qualification subprocess failed: %v\n%s", err, output)
			}
		})
	}
}
