package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const campaignRecoveryUsage = "Usage: t3-steward campaign recovery retry RUN --incident ID --activation-id ID --activation EPOCH --operation-id KEY --expected-incident-revision N --graph-revision N --source-attempt ID --source-attempt-revision N --instruction-artifact ID:DIGEST --failure-fingerprint VALUE --evidence-fingerprint VALUE --strategy-fingerprint VALUE [--checkpoint-artifact ID:DIGEST]\n"

func parseRecoveryArtifact(value string) (domain.ArtifactDigest, error) {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return domain.ArtifactDigest{}, fmt.Errorf("artifact must be ID:DIGEST")
	}
	return domain.ArtifactDigest{ArtifactID: parts[0], Digest: parts[1]}, nil
}

func (c campaignCLI) runRecovery(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		_, err := fmt.Fprint(c.stdout, campaignRecoveryUsage)
		return err
	}
	if args[0] != "retry" || len(args) < 2 {
		return errors.New("recovery requires retry and a run id")
	}
	request := domain.RecoveryRetryRequest{RunID: args[1], RequestedAt: time.Now().UTC()}
	var credential string
	values := make(map[string]string)
	for index := 2; index < len(args); index++ {
		flag := args[index]
		if index+1 >= len(args) {
			return fmt.Errorf("%s needs a value", flag)
		}
		index++
		value := args[index]
		if flag == "--checkpoint-artifact" {
			artifact, err := parseRecoveryArtifact(value)
			if err != nil {
				return fmt.Errorf("%s: %w", flag, err)
			}
			request.CheckpointArtifacts = append(request.CheckpointArtifacts, artifact)
			continue
		}
		if _, exists := values[flag]; exists {
			return fmt.Errorf("%s may be named only once", flag)
		}
		values[flag] = value
	}
	required := []string{"--incident", "--activation-id", "--activation", "--operation-id", "--expected-incident-revision", "--graph-revision", "--source-attempt", "--source-attempt-revision", "--instruction-artifact", "--failure-fingerprint", "--evidence-fingerprint", "--strategy-fingerprint"}
	for _, flag := range required {
		if strings.TrimSpace(values[flag]) == "" {
			return fmt.Errorf("recovery retry requires %s", flag)
		}
	}
	var err error
	request.IncidentID = values["--incident"]
	request.ActivationID = values["--activation-id"]
	request.OperationID = values["--operation-id"]
	request.SourceAttemptID = values["--source-attempt"]
	request.ActivationEpoch, err = strconv.ParseInt(values["--activation"], 10, 64)
	if err != nil {
		return fmt.Errorf("--activation: %w", err)
	}
	request.ExpectedIncidentRevision, err = strconv.ParseInt(values["--expected-incident-revision"], 10, 64)
	if err != nil {
		return fmt.Errorf("--expected-incident-revision: %w", err)
	}
	request.GraphRevision, err = strconv.ParseInt(values["--graph-revision"], 10, 64)
	if err != nil {
		return fmt.Errorf("--graph-revision: %w", err)
	}
	request.SourceAttemptRevision, err = strconv.ParseInt(values["--source-attempt-revision"], 10, 64)
	if err != nil {
		return fmt.Errorf("--source-attempt-revision: %w", err)
	}
	request.InstructionArtifact, err = parseRecoveryArtifact(values["--instruction-artifact"])
	if err != nil {
		return fmt.Errorf("--instruction-artifact: %w", err)
	}
	request.Diagnostic = domain.RecoveryDiagnosticIdentity{
		FailureFingerprint:  values["--failure-fingerprint"],
		EvidenceFingerprint: values["--evidence-fingerprint"],
		StrategyFingerprint: values["--strategy-fingerprint"],
	}
	credential = values[supervisorCredentialFlag]
	identity, err := resolveSupervisorIdentity(credential)
	if err != nil {
		return err
	}
	if c.retryRecoveryAs == nil {
		return errors.New("coordinator recovery retry transport is unavailable")
	}
	receipt, err := c.retryRecoveryAs(ctx, identity, request)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(c.stdout, string(encoded))
	return err
}
