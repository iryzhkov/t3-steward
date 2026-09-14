package backlog

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func goldenPromptRequest() PromptRequest {
	return PromptRequest{
		TaskID:             "implement",
		Objective:          "Add resource declarations to the workflow manifest.",
		AcceptanceCriteria: []string{"go test ./internal/backlog/... passes", "no behaviour changes outside the manifest"},
		Constraints:        []string{"do not edit placement.go"},
		Inputs:             []domain.PromptInput{{Name: "plan.md", Location: "inputs/plan.md", Digest: "sha256:plan-1"}},
		RequiredOutputs:    []string{"findings.md"},
		RequiredEffects:    []string{"none"},
		SafetyRules:        []string{"never push to a remote"},
		Report: PreflightReport{
			Outcome: PreflightReady,
			Receipts: []domain.PreflightReceipt{
				{Identity: domain.PreflightIdentity{StepID: "go_build"}, Status: domain.PreflightPassed},
				{Identity: domain.PreflightIdentity{StepID: "unit_tests"}, Status: domain.PreflightFailed, ExitCode: 2, Stdout: "FAIL internal/backlog\n"},
			},
			Bundle: domain.ContextBundle{Facts: []domain.ContextFact{
				{ID: "head", Include: domain.FactIncludeSummary, Value: "c0ffee", ReceiptDigest: "sha256:head-1"},
				{ID: "status", Include: domain.FactIncludeReference, Reference: "preflight/status@sha256:status-1", ReceiptDigest: "sha256:status-1"},
				{ID: "noise", Include: domain.FactIncludeOmit, Value: "unrelated plan history", ReceiptDigest: "sha256:noise-1"},
			}},
		},
	}
}

const goldenPrompt = `prompt-envelope/v1
task: implement

## objective
Add resource declarations to the workflow manifest.

## acceptance criteria
- go test ./internal/backlog/... passes
- no behaviour changes outside the manifest

## constraints
- do not edit placement.go

## inputs
- plan.md at inputs/plan.md (sha256:plan-1)

## required outputs
- findings.md

## required effects
- none

## safety rules
- never push to a remote

## preflight
- go_build passed exit=0
- unit_tests failed exit=2
  FAIL internal/backlog

## context
- head: c0ffee
- status -> preflight/status@sha256:status-1
`

func TestPromptEnvelopeGolden(t *testing.T) {
	envelope, err := BuildPromptEnvelope(goldenPromptRequest())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if got := envelope.Render(); got != goldenPrompt {
		t.Fatalf("prompt mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, goldenPrompt)
	}
	if envelope.Version != domain.PromptEnvelopeVersion || envelope.ByteCap != DefaultPromptByteCap {
		t.Fatalf("envelope identity = %#v", envelope)
	}
	if envelope.Size() != len(goldenPrompt) || envelope.EstimatedTokens() != (len(goldenPrompt)+3)/4 {
		t.Fatalf("size = %d tokens = %d", envelope.Size(), envelope.EstimatedTokens())
	}
	// An omitted fact never reaches the prompt, and a passing check contributes
	// its status line only.
	if strings.Contains(goldenPrompt, "unrelated plan history") || strings.Contains(goldenPrompt, "noise") {
		t.Fatal("omitted fact reached the prompt")
	}
}

func TestPromptEnvelopeIsDeterministic(t *testing.T) {
	first, err := BuildPromptEnvelope(goldenPromptRequest())
	if err != nil {
		t.Fatalf("first build: %v", err)
	}
	reversed := goldenPromptRequest()
	facts := reversed.Report.Bundle.Facts
	for left, right := 0, len(facts)-1; left < right; left, right = left+1, right-1 {
		facts[left], facts[right] = facts[right], facts[left]
	}
	second, err := BuildPromptEnvelope(reversed)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	if first.Render() != second.Render() {
		t.Fatal("fact order changed the rendered prompt")
	}
}

func TestPromptEnvelopeRejectsMissingMandatoryFields(t *testing.T) {
	tests := []struct {
		mutate func(*PromptRequest)
		name   string
		want   string
	}{
		{name: "no task", want: "task id", mutate: func(r *PromptRequest) { r.TaskID = "" }},
		{name: "no objective", want: "objective", mutate: func(r *PromptRequest) { r.Objective = " " }},
		{name: "duplicate fact", want: `duplicates fact "head"`, mutate: func(r *PromptRequest) {
			r.Report.Bundle.Facts = append(r.Report.Bundle.Facts, domain.ContextFact{
				ID: "head", Include: domain.FactIncludeSummary, Value: "again", ReceiptDigest: "sha256:head-2",
			})
		}},
		{name: "duplicate input", want: `duplicates input "plan.md"`, mutate: func(r *PromptRequest) {
			r.Inputs = append(r.Inputs, domain.PromptInput{Name: "plan.md", Location: "inputs/other.md", Digest: "sha256:plan-2"})
		}},
		{name: "input without digest", want: "name, location and digest", mutate: func(r *PromptRequest) {
			r.Inputs = []domain.PromptInput{{Name: "plan.md", Location: "inputs/plan.md"}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := goldenPromptRequest()
			test.mutate(&request)
			_, err := BuildPromptEnvelope(request)
			assertErrorContains(t, err, test.want)
		})
	}
}

func TestPromptEnvelopeKeepsUnboundedOutputOut(t *testing.T) {
	t.Run("an oversized fact is bounded then demoted", func(t *testing.T) {
		request := goldenPromptRequest()
		request.Report.Bundle.Facts = []domain.ContextFact{{
			ID: "build_log", Include: domain.FactIncludeSummary, Value: strings.Repeat("log line\n", 8192),
			Reference: "preflight/build_log@sha256:log-1", ReceiptDigest: "sha256:log-1",
		}}
		request.ByteCap = 1024
		envelope, err := BuildPromptEnvelope(request)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if envelope.Size() > envelope.ByteCap {
			t.Fatalf("size %d exceeds cap %d", envelope.Size(), envelope.ByteCap)
		}
		fact := envelope.Facts[0]
		if fact.Include != domain.FactIncludeReference || fact.Value != "" {
			t.Fatalf("fact = %#v", fact)
		}
		if !strings.Contains(envelope.Render(), "preflight/build_log@sha256:log-1") {
			t.Fatal("demoted fact lost its reference")
		}
	})

	t.Run("a fact inlined under the cap is bounded to the fact limit", func(t *testing.T) {
		request := goldenPromptRequest()
		request.Report.Bundle.Facts = []domain.ContextFact{{
			ID: "status", Include: domain.FactIncludeSummary, Value: strings.Repeat("y", 4096),
			Reference: "preflight/status@sha256:status-1", ReceiptDigest: "sha256:status-1",
		}}
		envelope, err := BuildPromptEnvelope(request)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		fact := envelope.Facts[0]
		if len(fact.Value) != PromptFactBytes || !fact.Truncated {
			t.Fatalf("fact = %d bytes truncated=%v", len(fact.Value), fact.Truncated)
		}
	})

	t.Run("a failing excerpt is bounded", func(t *testing.T) {
		request := goldenPromptRequest()
		request.Report.Receipts = []domain.PreflightReceipt{{
			Identity: domain.PreflightIdentity{StepID: "unit_tests"}, Status: domain.PreflightFailed,
			ExitCode: 1, Stdout: strings.Repeat("z", 8192),
		}}
		envelope, err := BuildPromptEnvelope(request)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		result := envelope.Preflight[0]
		if len(result.Excerpt) != PromptExcerptBytes || !result.Truncated {
			t.Fatalf("excerpt = %d bytes truncated=%v", len(result.Excerpt), result.Truncated)
		}
	})

	t.Run("an impossible budget fails loudly", func(t *testing.T) {
		request := goldenPromptRequest()
		// Only reference facts remain, so there is nothing left to demote and
		// the mandatory fields alone cannot fit.
		request.Report.Bundle.Facts = []domain.ContextFact{{
			ID: "status", Include: domain.FactIncludeReference,
			Reference: "preflight/status@sha256:status-1", ReceiptDigest: "sha256:status-1",
		}}
		request.ByteCap = 64
		_, err := BuildPromptEnvelope(request)
		assertErrorContains(t, err, "over its 64 byte cap")
	})

	t.Run("a summary fact without a reference cannot be demoted silently", func(t *testing.T) {
		request := goldenPromptRequest()
		request.Report.Bundle.Facts = []domain.ContextFact{{
			ID: "build_log", Include: domain.FactIncludeSummary, Value: strings.Repeat("log line\n", 8192),
			ReceiptDigest: "sha256:log-1",
		}}
		request.ByteCap = 512
		_, err := BuildPromptEnvelope(request)
		assertErrorContains(t, err, `cannot demote fact "build_log" without a reference`)
	})
}

func TestPromptEnvelopeRedactsEveryTextField(t *testing.T) {
	request := goldenPromptRequest()
	request.Objective = "Use api_key: supersecretvalue to reach the service"
	request.Constraints = []string{"authorization: Bearer abcdefghijklmnop"}
	request.Report.Bundle.Facts = []domain.ContextFact{{
		ID: "env", Include: domain.FactIncludeSummary, Value: "TOKEN=ghp_abcdefghijklmnopqrstuvwxyz01",
		Reference: "preflight/env@sha256:env-1", ReceiptDigest: "sha256:env-1",
	}}
	envelope, err := BuildPromptEnvelope(request)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	rendered := envelope.Render()
	for _, secret := range []string{"supersecretvalue", "abcdefghijklmnop", "ghp_abcdefghijklmnopqrstuvwxyz01"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("secret %q reached the prompt:\n%s", secret, rendered)
		}
	}
	if !strings.Contains(rendered, RedactionPlaceholder) {
		t.Fatalf("nothing was redacted:\n%s", rendered)
	}
}
