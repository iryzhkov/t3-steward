package backlog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var preflightClock = time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

type stubResponse struct {
	err    error
	output string
	exit   int
}

type stubRunner struct {
	responses map[string]stubResponse
	calls     []ProcessRequest
	defaults  stubResponse
}

func (r *stubRunner) Run(_ context.Context, request ProcessRequest) (ProcessResult, error) {
	r.calls = append(r.calls, request)
	key := strings.Join(append([]string{request.Program}, request.Args...), " ")
	response, ok := r.responses[key]
	if !ok {
		response = r.defaults
	}
	result := ProcessResult{Output: response.output, ExitCode: response.exit}
	if response.exit != 0 {
		return result, &ProcessExitError{ExitCode: response.exit, Err: errors.New("stub exit")}
	}
	return result, response.err
}

func preflightStep(step ManifestPreflightStep) ManifestPreflightStep {
	preflight := ManifestPreflight{Steps: []ManifestPreflightStep{step}}
	applyPreflightDefaults(&preflight)
	return preflight.Steps[0]
}

func preflightEngine(runner PreflightRunner, now time.Time) PreflightEngine {
	return PreflightEngine{Runner: runner, Now: func() time.Time { return now }}
}

func preflightRequest(steps ...ManifestPreflightStep) PreflightRequest {
	return PreflightRequest{
		TaskID:            "implement",
		AttemptID:         "attempt-1",
		WorkspaceDir:      "/srv/workspace",
		EnvironmentDigest: "sha256:env-1",
		SourceRevision:    "commit-1",
		WorkerID:          "homelab",
		InputDigests:      map[string]string{"plan.md": "sha256:plan-1"},
		Steps:             steps,
		Freshness:         time.Hour,
	}
}

func TestPreflightReusesEvidenceOnExactIdentityMatch(t *testing.T) {
	runner := &stubRunner{defaults: stubResponse{output: "ok"}}
	engine := preflightEngine(runner, preflightClock)
	step := preflightStep(ManifestPreflightStep{ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build", "./..."}})
	request := preflightRequest(step)

	first, err := engine.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if len(runner.calls) != 1 || len(first.Reused) != 0 {
		t.Fatalf("first pass calls=%d reused=%v", len(runner.calls), first.Reused)
	}

	request.Cached = first.Receipts
	second, err := engine.Run(context.Background(), request)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("exact identity match re-ran the command: calls=%d", len(runner.calls))
	}
	if len(second.Reused) != 1 || second.Reused[0] != "go_build" {
		t.Fatalf("reused = %v", second.Reused)
	}
	if !second.MayLaunch() {
		t.Fatalf("outcome = %#v", second)
	}
}

func TestPreflightInvalidatesEvidenceOnAnyIdentityChange(t *testing.T) {
	step := preflightStep(ManifestPreflightStep{ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build", "./..."}})
	base := preflightRequest(step)
	runner := &stubRunner{defaults: stubResponse{output: "ok"}}
	cached, err := preflightEngine(runner, preflightClock).Run(context.Background(), base)
	if err != nil {
		t.Fatalf("seed run: %v", err)
	}

	tests := []struct {
		mutate func(*PreflightRequest)
		name   string
		at     time.Time
	}{
		{name: "changed input", at: preflightClock, mutate: func(r *PreflightRequest) {
			r.InputDigests = map[string]string{"plan.md": "sha256:plan-2"}
		}},
		{name: "changed source", at: preflightClock, mutate: func(r *PreflightRequest) {
			r.SourceRevision = "commit-2"
		}},
		{name: "changed environment", at: preflightClock, mutate: func(r *PreflightRequest) {
			r.EnvironmentDigest = "sha256:env-2"
		}},
		{name: "changed command", at: preflightClock, mutate: func(r *PreflightRequest) {
			r.Steps = []ManifestPreflightStep{preflightStep(ManifestPreflightStep{
				ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build", "./internal/..."},
			})}
		}},
		{name: "expired freshness", at: preflightClock.Add(2 * time.Hour), mutate: func(_ *PreflightRequest) {}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.Cached = cached.Receipts
			test.mutate(&request)
			fresh := &stubRunner{defaults: stubResponse{output: "ok"}}
			report, err := preflightEngine(fresh, test.at).Run(context.Background(), request)
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if len(report.Reused) != 0 {
				t.Fatalf("stale evidence reused: %v", report.Reused)
			}
			if len(fresh.calls) != 1 {
				t.Fatalf("expected a fresh execution, calls=%d", len(fresh.calls))
			}
		})
	}
}

func TestPreflightFailurePolicyOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		policy  string
		want    PreflightOutcome
		wantKey PreflightBlockKind
	}{
		{name: "record launches with the failing baseline", policy: PreflightPolicyRecord, want: PreflightReady, wantKey: PreflightBlockNone},
		{name: "require-pass blocks the launch", policy: PreflightPolicyRequirePass, want: PreflightBlocked, wantKey: PreflightBlockRequirePass},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &stubRunner{defaults: stubResponse{output: "FAIL internal/backlog", exit: 2}}
			step := preflightStep(ManifestPreflightStep{
				ID: "unit_tests", Kind: PreflightKindCheck,
				Command: []string{"go", "test", "./..."}, FailurePolicy: test.policy,
			})
			report, err := preflightEngine(runner, preflightClock).Run(context.Background(), preflightRequest(step))
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if report.Outcome != test.want || report.Block != test.wantKey || report.MayLaunch() != (test.want == PreflightReady) {
				t.Fatalf("report = %#v", report)
			}
			receipt, ok := report.Receipt("unit_tests")
			if !ok || receipt.Status != domain.PreflightFailed || receipt.ExitCode != 2 {
				t.Fatalf("receipt = %#v ok=%v", receipt, ok)
			}
			if !strings.Contains(receipt.Stdout, "FAIL") {
				t.Fatalf("failing baseline is not visible: %#v", receipt)
			}
		})
	}
}

func TestPreflightContextStepRequiredVersusOptional(t *testing.T) {
	tests := []struct {
		name      string
		policy    string
		required  bool
		want      PreflightOutcome
		wantBlock PreflightBlockKind
	}{
		{name: "optional failure records a fact and continues", policy: PreflightPolicyRecord, required: false, want: PreflightReady, wantBlock: PreflightBlockNone},
		{name: "required failure under record stays visible", policy: PreflightPolicyRecord, required: true, want: PreflightReady, wantBlock: PreflightBlockNone},
		{name: "required failure under require-pass blocks", policy: PreflightPolicyRequirePass, required: true, want: PreflightBlocked, wantBlock: PreflightBlockRequirePass},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &stubRunner{defaults: stubResponse{output: "fatal: not a git repository", exit: 128}}
			step := preflightStep(ManifestPreflightStep{
				ID: "head", Kind: PreflightKindContext, Probe: ProbeGitHead,
				FailurePolicy: test.policy, Required: test.required,
			})
			report, err := preflightEngine(runner, preflightClock).Run(context.Background(), preflightRequest(step))
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if report.Outcome != test.want || report.Block != test.wantBlock {
				t.Fatalf("report = %#v", report)
			}
			// Both outcomes stay visible: a probe that failed is a fact, not an
			// absence.
			fact, ok := report.Bundle.Fact("head")
			if !ok || !fact.Failed || fact.Required != test.required {
				t.Fatalf("fact = %#v ok=%v", fact, ok)
			}
			if receipt, found := report.Receipt("head"); !found || receipt.Status != domain.PreflightFailed {
				t.Fatalf("receipt = %#v found=%v", receipt, found)
			}
		})
	}
}

func TestPreflightBoundsAndRedactsBeforeProducingFacts(t *testing.T) {
	t.Run("truncation is recorded", func(t *testing.T) {
		runner := &stubRunner{defaults: stubResponse{output: strings.Repeat("x", 512)}}
		step := preflightStep(ManifestPreflightStep{
			ID: "status", Kind: PreflightKindContext, Probe: ProbeGitStatus, MaxOutputBytes: 32,
		})
		report, err := preflightEngine(runner, preflightClock).Run(context.Background(), preflightRequest(step))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		receipt, _ := report.Receipt("status")
		if !receipt.Truncated || len(receipt.Stdout) != 32 {
			t.Fatalf("receipt = %#v", receipt)
		}
		fact, _ := report.Bundle.Fact("status")
		if !fact.Truncated || len(fact.Value) != 32 {
			t.Fatalf("fact = %#v", fact)
		}
	})

	t.Run("pattern redaction precedes the fact", func(t *testing.T) {
		runner := &stubRunner{defaults: stubResponse{output: "api_key: supersecretvalue\nok\n"}}
		step := preflightStep(ManifestPreflightStep{
			ID: "status", Kind: PreflightKindContext, Probe: ProbeGitStatus,
		})
		report, err := preflightEngine(runner, preflightClock).Run(context.Background(), preflightRequest(step))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		receipt, _ := report.Receipt("status")
		fact, _ := report.Bundle.Fact("status")
		for _, value := range []string{receipt.Stdout, fact.Value} {
			if strings.Contains(value, "supersecretvalue") || !strings.Contains(value, RedactionPlaceholder) {
				t.Fatalf("secret survived redaction: %q", value)
			}
		}
		if !receipt.Redacted || !fact.Redacted {
			t.Fatalf("redaction not reported: receipt=%#v fact=%#v", receipt, fact)
		}
	})

	t.Run("declared literals are redacted", func(t *testing.T) {
		runner := &stubRunner{defaults: stubResponse{output: "connected with hunter2\n"}}
		engine := preflightEngine(runner, preflightClock)
		engine.Redactor = Redactor{Literals: []string{"hunter2"}}
		step := preflightStep(ManifestPreflightStep{ID: "status", Kind: PreflightKindContext, Probe: ProbeGitStatus})
		report, err := engine.Run(context.Background(), preflightRequest(step))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		fact, _ := report.Bundle.Fact("status")
		if strings.Contains(fact.Value, "hunter2") || !fact.Redacted {
			t.Fatalf("fact = %#v", fact)
		}
	})
}

func TestPreflightUnknownProbeFailsLoudly(t *testing.T) {
	runner := &stubRunner{defaults: stubResponse{output: "unused"}}
	step := preflightStep(ManifestPreflightStep{ID: "mystery", Kind: PreflightKindContext, Probe: "no_such_probe"})
	report, err := preflightEngine(runner, preflightClock).Run(context.Background(), preflightRequest(step))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if report.MayLaunch() || report.Block != PreflightBlockExecution {
		t.Fatalf("report = %#v", report)
	}
	receipt, _ := report.Receipt("mystery")
	if !strings.Contains(receipt.Error, `unknown preflight probe "no_such_probe"`) {
		t.Fatalf("receipt = %#v", receipt)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("unknown probe ran a process: %#v", runner.calls)
	}
}

func TestBuiltinProbes(t *testing.T) {
	runner := &stubRunner{responses: map[string]stubResponse{
		"git rev-parse HEAD":        {output: "c0ffee\n"},
		"git status --porcelain=v1": {output: " M internal/backlog/preflight.go\n"},
		"git diff --stat":           {output: " 1 file changed\n"},
		"go --version":              {output: "go version go1.26.0 linux/amd64\n"},
	}}
	request := preflightRequest(
		preflightStep(ManifestPreflightStep{ID: "head", Kind: PreflightKindContext, Probe: ProbeGitHead}),
		preflightStep(ManifestPreflightStep{ID: "status", Kind: PreflightKindContext, Probe: ProbeGitStatus}),
		preflightStep(ManifestPreflightStep{ID: "diff", Kind: PreflightKindContext, Probe: ProbeGitDiffSummary}),
		preflightStep(ManifestPreflightStep{ID: "tools", Kind: PreflightKindContext, Probe: ProbeToolVersions}),
		preflightStep(ManifestPreflightStep{ID: "identity", Kind: PreflightKindContext, Probe: ProbeWorkerIdentity}),
	)
	request.Tools = []string{"go"}
	report, err := preflightEngine(runner, preflightClock).Run(context.Background(), request)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !report.MayLaunch() || len(report.Bundle.Facts) != 5 {
		t.Fatalf("report = %#v", report)
	}
	want := map[string]string{
		"head":     "c0ffee",
		"status":   "internal/backlog/preflight.go",
		"diff":     "1 file changed",
		"tools":    "go version go1.26.0",
		"identity": "worker: homelab",
	}
	for id, substring := range want {
		fact, ok := report.Bundle.Fact(id)
		if !ok || !strings.Contains(fact.Value, substring) {
			t.Fatalf("fact %s = %#v", id, fact)
		}
		if fact.ReceiptDigest == "" {
			t.Fatalf("fact %s has no provenance", id)
		}
	}
}

func TestPreflightRejectsMalformedRequests(t *testing.T) {
	step := preflightStep(ManifestPreflightStep{ID: "go_build", Kind: PreflightKindCheck, Command: []string{"go", "build"}})
	tests := []struct {
		mutate func(*PreflightRequest)
		name   string
		want   string
	}{
		{name: "missing task", want: "task id", mutate: func(r *PreflightRequest) { r.TaskID = "" }},
		{name: "missing environment", want: "environment digest", mutate: func(r *PreflightRequest) { r.EnvironmentDigest = "" }},
		{name: "relative workspace", want: "absolute workspace", mutate: func(r *PreflightRequest) { r.WorkspaceDir = "workspace" }},
		{name: "invalid step", want: "must set command or probe", mutate: func(r *PreflightRequest) {
			r.Steps = []ManifestPreflightStep{preflightStep(ManifestPreflightStep{ID: "empty", Kind: PreflightKindCheck})}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := preflightRequest(step)
			test.mutate(&request)
			_, err := preflightEngine(&stubRunner{}, preflightClock).Run(context.Background(), request)
			assertErrorContains(t, err, test.want)
		})
	}
}

func TestPreflightUnrunnableCommandBlocksUnderEveryPolicy(t *testing.T) {
	for _, policy := range []string{PreflightPolicyRecord, PreflightPolicyRequirePass} {
		t.Run(policy, func(t *testing.T) {
			runner := &stubRunner{defaults: stubResponse{err: errors.New("exec: \"go\": executable file not found")}}
			step := preflightStep(ManifestPreflightStep{
				ID: "go_build", Kind: PreflightKindCheck,
				Command: []string{"go", "build"}, FailurePolicy: policy,
			})
			report, err := preflightEngine(runner, preflightClock).Run(context.Background(), preflightRequest(step))
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if report.MayLaunch() || report.Block != PreflightBlockExecution || report.BlockedStep != "go_build" {
				t.Fatalf("report = %#v", report)
			}
			receipt, ok := report.Receipt("go_build")
			if !ok || receipt.Status != domain.PreflightErrored || receipt.Error == "" {
				t.Fatalf("receipt = %#v ok=%v", receipt, ok)
			}
		})
	}
}
