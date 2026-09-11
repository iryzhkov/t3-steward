package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type unknownRecoveryInvocation struct {
	request backlogadmin.UnknownRecoveryRequest
	asJSON  bool
}

func parseUnknownRecovery(args []string) (unknownRecoveryInvocation, error) {
	const usage = "recovery usage: recover <assignment> --outcome stopped|failed --coordinator-epoch N --assignment-epoch N --attempt-revision N --evidence-id ID --evidence-sha256 HEX --reason TEXT"
	if len(args) < 2 || args[0] != "recover" {
		return unknownRecoveryInvocation{}, errors.New(usage)
	}
	invocation := unknownRecoveryInvocation{request: backlogadmin.UnknownRecoveryRequest{AssignmentID: args[1]}}
	seen := make(map[string]bool)
	for index := 2; index < len(args); index++ {
		option := args[index]
		if seen[option] {
			return unknownRecoveryInvocation{}, fmt.Errorf("recovery option %s was provided more than once", option)
		}
		seen[option] = true
		if option == "--json" {
			invocation.asJSON = true
			continue
		}
		if index+1 >= len(args) {
			return unknownRecoveryInvocation{}, fmt.Errorf("recovery option %s needs a value", option)
		}
		index++
		value := args[index]
		switch option {
		case "--outcome":
			invocation.request.Outcome = domain.UnknownRecoveryOutcome(value)
		case "--coordinator-epoch":
			parsed, err := parseRecoveryNumber(option, value, false)
			if err != nil {
				return unknownRecoveryInvocation{}, err
			}
			invocation.request.CoordinatorEpoch = parsed
		case "--assignment-epoch":
			parsed, err := parseRecoveryNumber(option, value, false)
			if err != nil {
				return unknownRecoveryInvocation{}, err
			}
			invocation.request.ExpectedAssignmentEpoch = parsed
		case "--attempt-revision":
			parsed, err := parseRecoveryNumber(option, value, true)
			if err != nil {
				return unknownRecoveryInvocation{}, err
			}
			invocation.request.ExpectedAttemptRevision = parsed
		case "--evidence-id":
			invocation.request.EvidenceID = value
		case "--evidence-sha256":
			invocation.request.EvidenceSHA256 = value
		case "--reason":
			invocation.request.Reason = value
		case "--recovery-id":
			invocation.request.ID = value
		default:
			return unknownRecoveryInvocation{}, fmt.Errorf("unknown recovery option %q", option)
		}
	}
	for label, value := range map[string]string{
		"assignment": invocation.request.AssignmentID, "evidence id": invocation.request.EvidenceID,
		"reason": invocation.request.Reason,
	} {
		if value == "" || strings.TrimSpace(value) != value {
			return unknownRecoveryInvocation{}, fmt.Errorf("recovery %s must be nonempty and trimmed", label)
		}
	}
	for _, option := range []string{
		"--outcome", "--coordinator-epoch", "--assignment-epoch", "--attempt-revision",
		"--evidence-id", "--evidence-sha256", "--reason",
	} {
		if !seen[option] {
			return unknownRecoveryInvocation{}, fmt.Errorf("recovery option %s is required", option)
		}
	}
	if seen["--recovery-id"] && invocation.request.ID == "" {
		return unknownRecoveryInvocation{}, errors.New("recovery id must be nonempty and trimmed")
	}
	if invocation.request.ID != "" && strings.TrimSpace(invocation.request.ID) != invocation.request.ID {
		return unknownRecoveryInvocation{}, errors.New("recovery id must be trimmed")
	}
	if invocation.request.Outcome != domain.UnknownRecoveryStopped && invocation.request.Outcome != domain.UnknownRecoveryFailed {
		return unknownRecoveryInvocation{}, fmt.Errorf("unsupported recovery outcome %q", invocation.request.Outcome)
	}
	if invocation.request.CoordinatorEpoch < 1 || invocation.request.ExpectedAssignmentEpoch < 1 {
		return unknownRecoveryInvocation{}, errors.New("coordinator and assignment epochs are required and must be positive")
	}
	if len(invocation.request.EvidenceSHA256) != 64 {
		return unknownRecoveryInvocation{}, errors.New("evidence SHA-256 must contain 64 hexadecimal characters")
	}
	if _, err := hex.DecodeString(invocation.request.EvidenceSHA256); err != nil {
		return unknownRecoveryInvocation{}, errors.New("evidence SHA-256 must contain 64 hexadecimal characters")
	}
	return invocation, nil
}

func parseRecoveryNumber(option, value string, allowZero bool) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 || (!allowZero && parsed == 0) {
		return 0, fmt.Errorf("invalid %s value %q", option, value)
	}
	return parsed, nil
}

func (c backlogAdminCLI) runUnknownRecovery(ctx context.Context, args []string) error {
	if c.recovery == nil {
		return backlogadmin.ErrReadOnly
	}
	invocation, err := parseUnknownRecovery(args)
	if err != nil {
		return err
	}
	if invocation.request.ID == "" {
		generate := c.newCommandID
		if generate == nil {
			generate = newAdminCommandID
		}
		invocation.request.ID, err = generate()
		if err != nil {
			return fmt.Errorf("create recovery id: %w", err)
		}
	}
	decision, err := c.recovery.RecoverUnknown(ctx, c.principal, invocation.request)
	if err != nil {
		return err
	}
	if invocation.asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(decision)
	}
	outcome := "applied"
	if decision.Replay {
		outcome = "replayed"
	}
	_, err = fmt.Fprintf(c.stdout, "recovery %s assignment %s outcome %s (%s)\n", decision.Recovery.ID, decision.Assignment.ID, decision.Recovery.Outcome, outcome)
	return err
}
