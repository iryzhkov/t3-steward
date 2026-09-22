package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignRecoveryRetryBuildsStructuredRequest(t *testing.T) {
	var got domain.RecoveryRetryRequest
	var identity supervisorIdentity
	var output bytes.Buffer
	cli := campaignCLI{stdout: &output, retryRecoveryAs: func(_ context.Context, supplied supervisorIdentity, request domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error) {
		identity, got = supplied, request
		return domain.RecoveryRetryReceipt{OperationID: request.OperationID, IncidentID: request.IncidentID, AttemptID: "attempt-2", AttemptNumber: 2}, nil
	}}
	args := []string{"retry", "run-1", "--incident", "incident-1", "--activation-id", "activation-1", "--activation", "3",
		"--operation-id", "op-1", "--expected-incident-revision", "4", "--source-attempt", "attempt-1",
		"--source-attempt-revision", "7", "--instruction-artifact", "instruction:digest-a",
		"--checkpoint-artifact", "checkpoint:digest-b", "--failure-fingerprint", "failure",
		"--evidence-fingerprint", "evidence", "--strategy-fingerprint", "strategy",
		supervisorCredentialFlag, "secretref:f03-admin/repairer"}
	if err := cli.runRecovery(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if identity.CredentialReference != "secretref:f03-admin/repairer" || got.ActivationID != "activation-1" ||
		got.ActivationEpoch != 3 || got.ExpectedIncidentRevision != 4 || got.SourceAttemptRevision != 7 ||
		len(got.CheckpointArtifacts) != 1 || got.CheckpointArtifacts[0].ArtifactID != "checkpoint" {
		t.Fatalf("identity=%+v request=%+v", identity, got)
	}
	if output.Len() == 0 {
		t.Fatal("missing receipt")
	}
}
