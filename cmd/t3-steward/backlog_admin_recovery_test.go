package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fakeRecoveryService struct {
	principal backlogadmin.Principal
	request   backlogadmin.UnknownRecoveryRequest
	decision  domain.UnknownAssignmentRecoveryDecision
	err       error
}

func (f *fakeRecoveryService) RecoverUnknown(_ context.Context, principal backlogadmin.Principal, request backlogadmin.UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error) {
	f.principal = principal
	f.request = request
	return f.decision, f.err
}

func validRecoveryArgs() []string {
	return []string{
		"recover", "assignment-1", "--outcome", "stopped", "--coordinator-epoch", "7",
		"--assignment-epoch", "3", "--attempt-revision", "4", "--evidence-id", "incident-1",
		"--evidence-sha256", strings.Repeat("a", 64), "--reason", "verified stopped",
	}
}

func TestParseUnknownRecoveryRequiresAllFencesAndEvidence(t *testing.T) {
	invocation, err := parseUnknownRecovery(append(validRecoveryArgs(), "--recovery-id", "recovery-1", "--json"))
	if err != nil {
		t.Fatal(err)
	}
	request := invocation.request
	if request.ID != "recovery-1" || request.AssignmentID != "assignment-1" ||
		request.CoordinatorEpoch != 7 || request.ExpectedAssignmentEpoch != 3 ||
		request.ExpectedAttemptRevision != 4 || request.Outcome != domain.UnknownRecoveryStopped ||
		request.EvidenceID != "incident-1" || request.EvidenceSHA256 != strings.Repeat("a", 64) ||
		request.Reason != "verified stopped" || !invocation.asJSON {
		t.Fatalf("invocation = %+v", invocation)
	}

	invalid := [][]string{
		validRecoveryArgs()[:len(validRecoveryArgs())-2],
		append(validRecoveryArgs(), "--outcome", "failed"),
		append(validRecoveryArgs(), "--recovery-id", ""),
	}
	badDigest := validRecoveryArgs()
	badDigest[len(badDigest)-3] = strings.Repeat("z", 64)
	invalid = append(invalid, badDigest)
	for _, args := range invalid {
		if _, err := parseUnknownRecovery(args); err == nil {
			t.Fatalf("parse unexpectedly accepted %q", args)
		}
	}
}

func TestRunUnknownRecoveryUsesGeneratedIdAndRendersReplay(t *testing.T) {
	fake := &fakeRecoveryService{decision: domain.UnknownAssignmentRecoveryDecision{
		Recovery:   domain.UnknownAssignmentRecovery{ID: "recovery-generated", Outcome: domain.UnknownRecoveryStopped},
		Assignment: domain.Assignment{ID: "assignment-1"}, Replay: true,
	}}
	var output bytes.Buffer
	principal := backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}
	cli := backlogAdminCLI{
		recovery: fake, principal: principal, stdout: &output,
		newCommandID: func() (string, error) { return "recovery-generated", nil },
	}
	if err := cli.runBacklog(context.Background(), validRecoveryArgs()); err != nil {
		t.Fatal(err)
	}
	if fake.principal.ID != "operator" || fake.request.ID != "recovery-generated" || fake.request.AssignmentID != "assignment-1" {
		t.Fatalf("principal/request = %+v %+v", fake.principal, fake.request)
	}
	if !strings.Contains(output.String(), "(replayed)") {
		t.Fatalf("output = %q", output.String())
	}

	cli.newCommandID = func() (string, error) { return "", errors.New("entropy unavailable") }
	if err := cli.runUnknownRecovery(context.Background(), validRecoveryArgs()); err == nil || !strings.Contains(err.Error(), "entropy unavailable") {
		t.Fatalf("generation error = %v", err)
	}
}
