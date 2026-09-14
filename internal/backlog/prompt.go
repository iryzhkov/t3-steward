package backlog

import (
	"fmt"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Prompt budget constants. The byte cap is deterministic: the same envelope
// inputs always produce the same bytes and therefore the same decision about
// whether they fit.
const (
	// DefaultPromptByteCap bounds the rendered initial prompt.
	DefaultPromptByteCap = 16 << 10
	// PromptExcerptBytes bounds one preflight excerpt inside the prompt. Full
	// output stays a referenced artifact.
	PromptExcerptBytes = 512
	// PromptFactBytes bounds one inlined context fact.
	PromptFactBytes = 1024
)

// PromptRequest is everything the envelope may contain. Anything not named here
// cannot reach the prompt: there is no field for plan history, for a repeated
// repository description or for a log body. A bounded inline value reaches the
// agent only as a context fact a task explicitly declared with include summary.
type PromptRequest struct {
	TaskID             string
	Objective          string
	AcceptanceCriteria []string
	Constraints        []string
	Inputs             []domain.PromptInput
	RequiredOutputs    []string
	RequiredEffects    []string
	SafetyRules        []string
	Report             PreflightReport
	Redactor           Redactor
	ByteCap            int
}

// BuildPromptEnvelope assembles the versioned initial prompt.
//
// Assembly is deterministic: facts are ordered by ID, preflight results keep
// their declared step order, and every bound is a constant. When the rendered
// envelope does not fit its byte cap, summary facts are demoted to references,
// newest ID first, and the demotion is visible in the envelope itself. If it
// still does not fit, assembly fails loudly rather than dropping a mandatory
// field or overflowing the budget silently.
func BuildPromptEnvelope(request PromptRequest) (domain.PromptEnvelope, error) {
	byteCap := request.ByteCap
	if byteCap <= 0 {
		byteCap = DefaultPromptByteCap
	}
	redactor := request.Redactor
	if len(redactor.Patterns) == 0 && len(redactor.Literals) == 0 {
		redactor = DefaultRedactor()
	}
	objective, _ := redactor.Redact(request.Objective)
	envelope := domain.PromptEnvelope{
		Version:            domain.PromptEnvelopeVersion,
		TaskID:             request.TaskID,
		Objective:          objective,
		AcceptanceCriteria: redactAll(redactor, request.AcceptanceCriteria),
		Constraints:        redactAll(redactor, request.Constraints),
		Inputs:             request.Inputs,
		RequiredOutputs:    request.RequiredOutputs,
		RequiredEffects:    request.RequiredEffects,
		SafetyRules:        redactAll(redactor, request.SafetyRules),
		ByteCap:            byteCap,
	}
	for _, receipt := range request.Report.Receipts {
		envelope.Preflight = append(envelope.Preflight, promptResult(redactor, receipt))
	}
	envelope.Facts = promptFacts(redactor, request.Report.Bundle.Facts)
	if err := envelope.Validate(); err != nil {
		return domain.PromptEnvelope{}, err
	}
	if err := fitPromptEnvelope(&envelope); err != nil {
		return domain.PromptEnvelope{}, err
	}
	return envelope, nil
}

// promptResult compacts one receipt. A passing step contributes its status line
// only; an excerpt appears where it can be acted on.
func promptResult(redactor Redactor, receipt domain.PreflightReceipt) domain.PromptPreflightResult {
	result := domain.PromptPreflightResult{
		StepID:    receipt.Identity.StepID,
		Status:    receipt.Status,
		ExitCode:  receipt.ExitCode,
		Truncated: receipt.Truncated,
		Redacted:  receipt.Redacted,
	}
	if receipt.Status == domain.PreflightPassed {
		return result
	}
	excerpt := receipt.Stdout
	if excerpt == "" {
		excerpt = receipt.Error
	}
	excerpt, redacted := redactor.Redact(excerpt)
	excerpt, truncated := boundOutput(excerpt, PromptExcerptBytes)
	result.Excerpt = excerpt
	result.Redacted = result.Redacted || redacted
	result.Truncated = result.Truncated || truncated
	return result
}

func promptFacts(redactor Redactor, facts []domain.ContextFact) []domain.PromptFact {
	ordered := append([]domain.ContextFact(nil), facts...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].ID < ordered[right].ID })
	var result []domain.PromptFact
	for _, fact := range ordered {
		if fact.Include == domain.FactIncludeOmit {
			continue
		}
		prompt := domain.PromptFact{
			ID:        fact.ID,
			Include:   fact.Include,
			Reference: fact.Reference,
			Truncated: fact.Truncated,
			Redacted:  fact.Redacted,
			Failed:    fact.Failed,
		}
		if fact.Include == domain.FactIncludeSummary {
			value, redacted := redactor.Redact(fact.Value)
			value, truncated := boundOutput(value, PromptFactBytes)
			prompt.Value = value
			prompt.Redacted = prompt.Redacted || redacted
			prompt.Truncated = prompt.Truncated || truncated
		}
		result = append(result, prompt)
	}
	return result
}

// fitPromptEnvelope enforces the byte cap. Mandatory fields are never dropped,
// so the only lever is demoting an inlined fact to the reference the agent can
// fetch on demand.
func fitPromptEnvelope(envelope *domain.PromptEnvelope) error {
	for index := len(envelope.Facts) - 1; envelope.Size() > envelope.ByteCap && index >= 0; index-- {
		fact := &envelope.Facts[index]
		if fact.Include != domain.FactIncludeSummary {
			continue
		}
		if fact.Reference == "" {
			return fmt.Errorf("prompt envelope for task %s cannot demote fact %q without a reference", envelope.TaskID, fact.ID)
		}
		fact.Include = domain.FactIncludeReference
		fact.Value = ""
	}
	if size := envelope.Size(); size > envelope.ByteCap {
		return fmt.Errorf("prompt envelope for task %s is %d bytes over its %d byte cap", envelope.TaskID, size-envelope.ByteCap, envelope.ByteCap)
	}
	return nil
}

func redactAll(redactor Redactor, values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, len(values))
	for index, value := range values {
		result[index], _ = redactor.Redact(value)
	}
	return result
}
