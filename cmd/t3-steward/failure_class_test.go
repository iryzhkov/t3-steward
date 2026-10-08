package main

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFailureClassAndAutomaticRetryText(t *testing.T) {
	if got := failureClassText(nil); got != "" {
		t.Fatalf("nil attempt = %q", got)
	}
	if got := failureClassText(&domain.Attempt{Progress: domain.ProgressSucceeded}); got != "" {
		t.Fatalf("succeeded attempt = %q", got)
	}
	legacy := &domain.Attempt{Progress: domain.ProgressFailed, Failure: "verification command failed (1): make test"}
	if got := failureClassText(legacy); got != "code/verification-failed" {
		t.Fatalf("unrecorded classification = %q", got)
	}
	recorded := &domain.Attempt{Progress: domain.ProgressFailed, Failure: "x", FailureClass: domain.FailureInfrastructure, FailureReason: domain.ReasonWorkspaceMissing}
	if got := failureClassText(recorded); got != "infrastructure/workspace-missing" {
		t.Fatalf("recorded classification = %q", got)
	}
	if got := automaticRetryText(legacy); got != "" {
		t.Fatalf("operator attempt = %q", got)
	}
	retry := &domain.Attempt{AutomaticRetry: &domain.AutomaticRetry{
		SourceAttemptID: "attempt-1", Class: domain.FailureInfrastructure, Code: domain.ReasonProviderTurnFailed,
		Ordinal: 1, Budget: 2, NotBefore: time.Date(2026, 10, 8, 12, 2, 0, 0, time.UTC),
	}}
	want := "automatic retry 1 of 2 of attempt attempt-1 after infrastructure/provider-turn-failed, not before 2026-10-08T12:02:00Z"
	if got := automaticRetryText(retry); got != want {
		t.Fatalf("automatic retry = %q", got)
	}
}
