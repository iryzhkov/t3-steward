package domain

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The reasons below are the texts Steward records today, copied from where
// they are written, so a change to one of them that would silently move it
// to another class fails here.
func TestClassifyFailureCoversEveryClass(t *testing.T) {
	t.Parallel()
	cases := []struct {
		reason string
		want   FailureClassification
	}{
		{"preparation failed 3 times; first error: clone: exit 128; last error: clone: exit 128", FailureClassification{FailureInfrastructure, ReasonPreparationFailed}},
		{"preparation returned an empty workspace", FailureClassification{FailureInfrastructure, ReasonPreparationFailed}},
		{"T3 thread creation failed: dial unix: connection refused", FailureClassification{FailureInfrastructure, ReasonThreadStartFailed}},
		{"T3 thread never started: no turn after 10m", FailureClassification{FailureInfrastructure, ReasonThreadStartFailed}},
		{"T3 refused to start the provider turn: provider unavailable", FailureClassification{FailureInfrastructure, ReasonTurnStartRefused}},
		{"provider turn did not complete successfully: rate limited", FailureClassification{FailureInfrastructure, ReasonProviderTurnFailed}},
		{"paused by quota watchdog: five-hour window at 97%; provider session is not ready without an active turn or error", FailureClassification{FailureInfrastructure, ReasonQuotaPause}},
		{"provider session is not ready without an active turn or error", FailureClassification{FailureInfrastructure, ReasonSessionNotReady}},
		{"thread completion identity is missing or mismatched", FailureClassification{FailureInfrastructure, ReasonThreadIdentity}},
		{"provider completion timestamps are missing or invalid", FailureClassification{FailureInfrastructure, ReasonThreadIdentity}},
		{"workspace is missing; outputs cannot be collected", FailureClassification{FailureInfrastructure, ReasonWorkspaceMissing}},
		{"preserved result digest mismatch: declared \"report.md\" was 0a1b, now 9f8e", FailureClassification{FailureInfrastructure, ReasonPreservedResultMismatch}},
		{"contained run was killed for exceeding available memory", FailureClassification{FailureInfrastructure, ReasonHostMemory}},
		{"unknown execution resolved as failed by reviewed evidence", FailureClassification{FailureInfrastructure, ReasonExecutionUnknown}},
		{`assignment "a-1" settled as "released" while attempt "x" was parked on a task-bound wait; the execution cannot be resumed`, FailureClassification{FailureInfrastructure, ReasonExecutionAbandoned}},
		{`assignment "a-1" no longer owns parked attempt "x"`, FailureClassification{FailureInfrastructure, ReasonExecutionAbandoned}},

		{"missing declared output: handoff.md (the turn ended before it was written)", FailureClassification{FailureProtocol, ReasonMissingOutput}},
		{"missing verification evidence: verification/002.json", FailureClassification{FailureProtocol, ReasonMissingEvidence}},
		{"missing gate evidence", FailureClassification{FailureProtocol, ReasonMissingEvidence}},
		{"agent reported unfinished work: BACKLOG STATUS: continue", FailureClassification{FailureProtocol, ReasonUnfinishedWork}},
		{"review_output verification failed: verdict line is missing", FailureClassification{FailureProtocol, ReasonReviewOutputInvalid}},
		{"result import rejected: artifact \"x\": conflict", FailureClassification{FailureProtocol, ReasonResultRejected}},
		{"thread still has pending input, approval, or background work", FailureClassification{FailureProtocol, ReasonPendingThreadWork}},
		{"missing explicit success", FailureClassification{FailureProtocol, ReasonMissingSuccess}},

		{"verification command failed (2): make test", FailureClassification{FailureCode, ReasonVerificationFailed}},
		{"verification failed", FailureClassification{FailureCode, ReasonVerificationFailed}},
		{"gate command failed (1): go vet ./...: exit status 1", FailureClassification{FailureCode, ReasonGateFailed}},

		{"permanent collection secret failure: result secret scan refused results/x", FailureClassification{FailurePolicy, ReasonSecretScan}},
		{"failed result withheld by secret scan; publishing redacted failure", FailureClassification{FailurePolicy, ReasonSecretScan}},
		{"permanent collection size failure: artifact exceeds 64 MiB", FailureClassification{FailurePolicy, ReasonResultSize}},
		{"task timeout expired; execution stopped", FailureClassification{FailurePolicy, ReasonTaskTimeout}},
		{"task timeout expired before dispatch", FailureClassification{FailurePolicy, ReasonTaskTimeout}},
		{"contained run exceeded its 4096 MB memory reservation", FailureClassification{FailurePolicy, ReasonMemoryLimit}},
		{"review gate unreviewed-head: HEAD moved after the accepted round", FailureClassification{FailurePolicy, ReasonReviewGate}},

		{"stopped by the coordinator before dispatch", FailureClassification{FailureCancelled, ReasonCoordinatorStop}},

		{"something nobody has seen before", FailureClassification{FailureUnknown, ReasonUnrecognized}},
		{"", FailureClassification{FailureUnknown, ReasonUnrecognized}},
	}
	seen := map[FailureClass]bool{}
	for _, c := range cases {
		if got := ClassifyFailure(c.reason); got != c.want {
			t.Errorf("ClassifyFailure(%q) = %s, want %s", c.reason, got, c.want)
		}
		seen[c.want.Class] = true
	}
	for _, class := range []FailureClass{FailureInfrastructure, FailureProtocol, FailureCode, FailurePolicy, FailureCancelled, FailureUnknown} {
		if !seen[class] {
			t.Errorf("no case covers class %s", class)
		}
	}
}

// A reason joining several failures takes the class least willing to retry.
func TestClassifyFailurePrefersTheSaferClassForJoinedReasons(t *testing.T) {
	t.Parallel()
	cases := map[string]FailureClass{
		"provider turn did not complete successfully; verification command failed (1): make test":          FailureCode,
		"verification command failed (1): make test; missing declared output: handoff.md":                  FailureCode,
		"missing declared output: handoff.md; permanent collection secret failure: result secret scan ...": FailurePolicy,
		"provider session is not ready without an active turn or error; missing declared output: x":        FailureProtocol,
	}
	for reason, want := range cases {
		if got := ClassifyFailure(reason).Class; got != want {
			t.Errorf("ClassifyFailure(%q) class = %s, want %s", reason, got, want)
		}
	}
}

func TestClassifyAttemptFailure(t *testing.T) {
	t.Parallel()
	if _, ok := ClassifyAttemptFailure(Attempt{Progress: ProgressSucceeded, Failure: "verification failed"}); ok {
		t.Fatal("a succeeded attempt was classified")
	}
	if _, ok := ClassifyAttemptFailure(Attempt{Progress: ProgressActive}); ok {
		t.Fatal("an active attempt was classified")
	}
	if got, ok := ClassifyAttemptFailure(Attempt{Progress: ProgressCancelled}); !ok || got != (FailureClassification{FailureCancelled, ReasonCancelled}) {
		t.Fatalf("cancelled = %s, %t", got, ok)
	}
	got, ok := ClassifyAttemptFailure(Attempt{Progress: ProgressFailed, Failure: "T3 thread creation failed: x"})
	if !ok || got.Class != FailureInfrastructure || got.String() != "infrastructure/thread-start-failed" {
		t.Fatalf("failed = %s, %t", got, ok)
	}
	// The recorded classification is authoritative over the reason text.
	recorded := Attempt{Progress: ProgressFailed, Failure: "T3 thread creation failed: x", FailureClass: FailurePolicy, FailureReason: ReasonSecretScan}
	if got, _ := ClassifyAttemptFailure(recorded); got != (FailureClassification{FailurePolicy, ReasonSecretScan}) {
		t.Fatalf("recorded = %s", got)
	}
}

func TestOnlyInfrastructureIsRetryable(t *testing.T) {
	t.Parallel()
	for _, class := range []FailureClass{FailureProtocol, FailureCode, FailurePolicy, FailureCancelled, FailureUnknown} {
		if class.Retryable() {
			t.Errorf("%s is retryable", class)
		}
	}
	if !FailureInfrastructure.Retryable() {
		t.Error("infrastructure is not retryable")
	}
}

func TestEffectiveRetryPolicyAndBackoff(t *testing.T) {
	t.Parallel()
	if got := EffectiveRetryPolicy(Task{}, 3); got != (TaskRetryPolicy{Infrastructure: DefaultInfrastructureRetries, Backoff: DefaultRetryBackoff}) {
		t.Fatalf("default = %+v", got)
	}
	declared := Task{Retry: &TaskRetryPolicy{Infrastructure: 5, Backoff: time.Minute}}
	if got := EffectiveRetryPolicy(declared, 3); got.Infrastructure != 3 || got.Backoff != time.Minute {
		t.Fatalf("capped = %+v", got)
	}
	if got := EffectiveRetryPolicy(declared, 0); got.Infrastructure != 0 {
		t.Fatalf("disabled coordinator = %+v", got)
	}
	if got := EffectiveRetryPolicy(Task{Retry: &TaskRetryPolicy{Infrastructure: 0}}, 3); got.Infrastructure != 0 || got.Backoff != DefaultRetryBackoff {
		t.Fatalf("opted out = %+v", got)
	}
	policy := TaskRetryPolicy{Backoff: 20 * time.Minute}
	for ordinal, want := range map[int]time.Duration{1: 20 * time.Minute, 2: 40 * time.Minute, 3: time.Hour, 9: time.Hour} {
		if got := policy.RetryDelay(ordinal); got != want {
			t.Errorf("RetryDelay(%d) = %s, want %s", ordinal, got, want)
		}
	}
	for _, invalid := range []TaskRetryPolicy{{Infrastructure: -1}, {Infrastructure: MaxInfrastructureRetries + 1}, {Backoff: -time.Second}, {Backoff: 2 * time.Hour}} {
		if invalid.Validate() == nil {
			t.Errorf("%+v validated", invalid)
		}
	}
	if err := (TaskRetryPolicy{Infrastructure: MaxInfrastructureRetries, Backoff: time.Hour}).Validate(); err != nil {
		t.Fatal(err)
	}
}

// The documented table is the code's table: every row appears in
// docs/failure-classification.md with its class and code.
func TestFailureClassificationTableIsDocumented(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "failure-classification.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	for _, rule := range FailureClassificationTable {
		row := "| `" + strings.TrimSpace(rule.Match) + "` | " + string(rule.Class) + " | " + string(rule.Code) + " |"
		if !strings.Contains(doc, row) {
			t.Errorf("docs/failure-classification.md lacks the row %s", row)
		}
	}
}
