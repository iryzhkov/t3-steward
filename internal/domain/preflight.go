package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// PreflightIdentity binds preflight evidence to the exact bytes it was produced
// from: the command or probe that ran, the environment it ran in, the source
// revision it observed and the digest of every mounted input. Two runs share an
// identity only when all of those agree, which is what stops a receipt made
// from one set of bytes being reported for another.
type PreflightIdentity struct {
	InputDigests      map[string]string `json:"inputDigests,omitempty"`
	StepID            string            `json:"stepId"`
	Probe             string            `json:"probe,omitempty"`
	EnvironmentDigest string            `json:"environmentDigest"`
	SourceRevision    string            `json:"sourceRevision"`
	Command           []string          `json:"command,omitempty"`
}

// Digest is the content address of the identity. It is stable across map
// ordering: input digests are sorted by name and every component is
// length-prefixed, so two distinct identities cannot serialize to equal bytes.
func (i PreflightIdentity) Digest() string {
	var builder strings.Builder
	write := func(label, value string) {
		fmt.Fprintf(&builder, "%s:%d:%s\n", label, len(value), value)
	}
	write("step", i.StepID)
	write("probe", i.Probe)
	write("environment", i.EnvironmentDigest)
	write("source", i.SourceRevision)
	for index, argument := range i.Command {
		write(fmt.Sprintf("command[%d]", index), argument)
	}
	names := make([]string, 0, len(i.InputDigests))
	for name := range i.InputDigests {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		write("input:"+name, i.InputDigests[name])
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

// Equal reports whether two identities describe the same evidence.
func (i PreflightIdentity) Equal(other PreflightIdentity) bool {
	return i.Digest() == other.Digest()
}

// Validate rejects an identity that cannot address evidence.
func (i PreflightIdentity) Validate() error {
	if strings.TrimSpace(i.StepID) == "" {
		return errors.New("preflight identity requires a step id")
	}
	if len(i.Command) == 0 && i.Probe == "" {
		return fmt.Errorf("preflight identity %s requires a command or a probe", i.StepID)
	}
	if len(i.Command) != 0 && i.Probe != "" {
		return fmt.Errorf("preflight identity %s has both a command and a probe", i.StepID)
	}
	if i.EnvironmentDigest == "" {
		return fmt.Errorf("preflight identity %s requires an environment digest", i.StepID)
	}
	for name, digest := range i.InputDigests {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(digest) == "" {
			return fmt.Errorf("preflight identity %s has an unnamed or undigested input", i.StepID)
		}
	}
	return nil
}

// PreflightStatus is the outcome of one executed step.
type PreflightStatus string

const (
	// PreflightPassed means the step ran and reported success.
	PreflightPassed PreflightStatus = "passed"
	// PreflightFailed means the step ran and reported failure. The evidence is
	// valid; what it proves is a failing baseline.
	PreflightFailed PreflightStatus = "failed"
	// PreflightErrored means the step could not be run or its result could not
	// be preserved. This is never evidence about the baseline.
	PreflightErrored PreflightStatus = "errored"
)

// PreflightReceipt is the durable record of one executed step.
//
// Stdout and Stderr are bounded excerpts. A runner that merges the two streams,
// as the contained process runner does, fills Stdout and leaves Stderr empty
// rather than inventing a split that the worker never observed.
type PreflightReceipt struct {
	StartedAt      time.Time         `json:"startedAt"`
	CompletedAt    time.Time         `json:"completedAt"`
	Identity       PreflightIdentity `json:"identity"`
	IdentityDigest string            `json:"identityDigest"`
	Kind           string            `json:"kind"`
	Status         PreflightStatus   `json:"status"`
	Stdout         string            `json:"stdout,omitempty"`
	Stderr         string            `json:"stderr,omitempty"`
	Error          string            `json:"error,omitempty"`
	FreshFor       time.Duration     `json:"freshFor"`
	ExitCode       int               `json:"exitCode"`
	Truncated      bool              `json:"truncated"`
	Redacted       bool              `json:"redacted"`
}

// Validate rejects a receipt that cannot be trusted as evidence.
func (r PreflightReceipt) Validate() error {
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	if r.IdentityDigest != r.Identity.Digest() {
		return fmt.Errorf("preflight receipt %s digest does not match its identity", r.Identity.StepID)
	}
	switch r.Status {
	case PreflightPassed, PreflightFailed, PreflightErrored:
	default:
		return fmt.Errorf("preflight receipt %s has invalid status %q", r.Identity.StepID, r.Status)
	}
	if r.Status == PreflightErrored && r.Error == "" {
		return fmt.Errorf("preflight receipt %s is errored without a reason", r.Identity.StepID)
	}
	if r.StartedAt.IsZero() || r.CompletedAt.Before(r.StartedAt) {
		return fmt.Errorf("preflight receipt %s has no coherent time window", r.Identity.StepID)
	}
	if r.FreshFor < 0 {
		return fmt.Errorf("preflight receipt %s has a negative freshness window", r.Identity.StepID)
	}
	return nil
}

// Fresh reports whether the receipt is still inside its declared freshness
// window at the given time. A zero window is never fresh, so evidence is reused
// only when an author asked for reuse.
func (r PreflightReceipt) Fresh(now time.Time) bool {
	if r.FreshFor <= 0 {
		return false
	}
	return !now.After(r.CompletedAt.Add(r.FreshFor))
}

// Reusable reports whether this receipt may stand in for a new execution of the
// given identity. Reuse requires an exact identity match inside the freshness
// window, and evidence that never ran is never reusable.
func (r PreflightReceipt) Reusable(identity PreflightIdentity, now time.Time) bool {
	if r.Status == PreflightErrored {
		return false
	}
	if r.IdentityDigest != identity.Digest() {
		return false
	}
	return r.Fresh(now)
}

// Prompt inclusion modes for one collected fact.
const (
	FactIncludeSummary   = "summary"
	FactIncludeReference = "reference"
	FactIncludeOmit      = "omit"
)

// ContextFact is one bounded, redacted observation produced by a context step.
// A failed optional step still produces a fact, with Failed set, so that a probe
// which did not work is visible rather than absent.
type ContextFact struct {
	ProducedAt    time.Time `json:"producedAt"`
	ID            string    `json:"id"`
	Label         string    `json:"label,omitempty"`
	Value         string    `json:"value,omitempty"`
	Reference     string    `json:"reference,omitempty"`
	Include       string    `json:"include"`
	ReceiptDigest string    `json:"receiptDigest"`
	Required      bool      `json:"required"`
	Failed        bool      `json:"failed"`
	Truncated     bool      `json:"truncated"`
	Redacted      bool      `json:"redacted"`
}

// ContextBundle is the ordered set of facts collected for one attempt. Raw
// output stays in a referenced artifact; only bounded values live here.
type ContextBundle struct {
	CreatedAt time.Time     `json:"createdAt"`
	TaskID    string        `json:"taskId"`
	AttemptID string        `json:"attemptId"`
	Facts     []ContextFact `json:"facts,omitempty"`
}

// Validate rejects a bundle whose facts are ambiguous or unprovenanced.
func (b ContextBundle) Validate() error {
	seen := make(map[string]struct{}, len(b.Facts))
	for _, fact := range b.Facts {
		if strings.TrimSpace(fact.ID) == "" {
			return errors.New("context fact requires an id")
		}
		if _, duplicate := seen[fact.ID]; duplicate {
			return fmt.Errorf("context bundle duplicates fact %q", fact.ID)
		}
		seen[fact.ID] = struct{}{}
		switch fact.Include {
		case FactIncludeSummary, FactIncludeReference, FactIncludeOmit:
		default:
			return fmt.Errorf("context fact %q has invalid include %q", fact.ID, fact.Include)
		}
		if fact.ReceiptDigest == "" {
			return fmt.Errorf("context fact %q has no receipt provenance", fact.ID)
		}
	}
	return nil
}

// Fact returns one fact by ID.
func (b ContextBundle) Fact(id string) (ContextFact, bool) {
	for _, fact := range b.Facts {
		if fact.ID == id {
			return fact, true
		}
	}
	return ContextFact{}, false
}

// PromptEnvelopeVersion is the wire version of the initial agent prompt. A
// change to the rendered shape raises it.
const PromptEnvelopeVersion = 1

// PromptInput names one mounted input by location and digest. The bytes stay
// mounted; the prompt says where they are, not what they contain.
type PromptInput struct {
	Name     string `json:"name"`
	Location string `json:"location"`
	Digest   string `json:"digest"`
}

// PromptPreflightResult is the compact form of one receipt inside a prompt.
type PromptPreflightResult struct {
	StepID    string          `json:"stepId"`
	Status    PreflightStatus `json:"status"`
	Excerpt   string          `json:"excerpt,omitempty"`
	ExitCode  int             `json:"exitCode"`
	Truncated bool            `json:"truncated"`
	Redacted  bool            `json:"redacted"`
}

// PromptFact is the compact form of one context fact inside a prompt.
type PromptFact struct {
	ID        string `json:"id"`
	Value     string `json:"value,omitempty"`
	Reference string `json:"reference,omitempty"`
	Include   string `json:"include"`
	Truncated bool   `json:"truncated"`
	Redacted  bool   `json:"redacted"`
	Failed    bool   `json:"failed"`
}

// PromptEnvelope is the versioned initial prompt. It carries task-start
// requirements, mounted input locations, required results, safety rules and a
// compact preflight result. Unrelated plan history, duplicated repository
// context and full logs are not part of it: those stay referenced artifacts the
// agent fetches on demand.
type PromptEnvelope struct {
	TaskID             string                  `json:"taskId"`
	Objective          string                  `json:"objective"`
	AcceptanceCriteria []string                `json:"acceptanceCriteria,omitempty"`
	Constraints        []string                `json:"constraints,omitempty"`
	Inputs             []PromptInput           `json:"inputs,omitempty"`
	RequiredOutputs    []string                `json:"requiredOutputs,omitempty"`
	RequiredEffects    []string                `json:"requiredEffects,omitempty"`
	SafetyRules        []string                `json:"safetyRules,omitempty"`
	Preflight          []PromptPreflightResult `json:"preflight,omitempty"`
	Facts              []PromptFact            `json:"facts,omitempty"`
	Version            int                     `json:"version"`
	ByteCap            int                     `json:"byteCap"`
}

// Validate rejects an envelope that is missing a mandatory field or that
// carries an ambiguous duplicate.
func (e PromptEnvelope) Validate() error {
	if e.Version != PromptEnvelopeVersion {
		return fmt.Errorf("prompt envelope version must be %d", PromptEnvelopeVersion)
	}
	if strings.TrimSpace(e.TaskID) == "" {
		return errors.New("prompt envelope requires a task id")
	}
	if strings.TrimSpace(e.Objective) == "" {
		return errors.New("prompt envelope requires an objective")
	}
	if e.ByteCap <= 0 {
		return errors.New("prompt envelope requires a positive byte cap")
	}
	inputs := make(map[string]struct{}, len(e.Inputs))
	for _, input := range e.Inputs {
		if input.Name == "" || input.Location == "" || input.Digest == "" {
			return errors.New("prompt envelope input requires a name, location and digest")
		}
		if _, duplicate := inputs[input.Name]; duplicate {
			return fmt.Errorf("prompt envelope duplicates input %q", input.Name)
		}
		inputs[input.Name] = struct{}{}
	}
	facts := make(map[string]struct{}, len(e.Facts))
	for _, fact := range e.Facts {
		if _, duplicate := facts[fact.ID]; duplicate {
			return fmt.Errorf("prompt envelope duplicates fact %q", fact.ID)
		}
		facts[fact.ID] = struct{}{}
		if fact.Include == FactIncludeOmit {
			return fmt.Errorf("prompt envelope includes omitted fact %q", fact.ID)
		}
	}
	steps := make(map[string]struct{}, len(e.Preflight))
	for _, result := range e.Preflight {
		if _, duplicate := steps[result.StepID]; duplicate {
			return fmt.Errorf("prompt envelope duplicates preflight step %q", result.StepID)
		}
		steps[result.StepID] = struct{}{}
	}
	return nil
}

// Render produces the prompt bytes. It is deterministic: the same envelope
// renders the same bytes on every host and every run.
func (e PromptEnvelope) Render() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "prompt-envelope/v%d\n", e.Version)
	fmt.Fprintf(&builder, "task: %s\n\n", e.TaskID)
	fmt.Fprintf(&builder, "## objective\n%s\n", strings.TrimSpace(e.Objective))
	renderList(&builder, "acceptance criteria", e.AcceptanceCriteria)
	renderList(&builder, "constraints", e.Constraints)
	if len(e.Inputs) > 0 {
		builder.WriteString("\n## inputs\n")
		for _, input := range e.Inputs {
			fmt.Fprintf(&builder, "- %s at %s (%s)\n", input.Name, input.Location, input.Digest)
		}
	}
	renderList(&builder, "required outputs", e.RequiredOutputs)
	renderList(&builder, "required effects", e.RequiredEffects)
	renderList(&builder, "safety rules", e.SafetyRules)
	if len(e.Preflight) > 0 {
		builder.WriteString("\n## preflight\n")
		for _, result := range e.Preflight {
			fmt.Fprintf(&builder, "- %s %s exit=%d%s\n", result.StepID, result.Status, result.ExitCode, renderMarks(result.Truncated, result.Redacted))
			if result.Excerpt != "" {
				fmt.Fprintf(&builder, "  %s\n", strings.ReplaceAll(strings.TrimRight(result.Excerpt, "\n"), "\n", "\n  "))
			}
		}
	}
	if len(e.Facts) > 0 {
		builder.WriteString("\n## context\n")
		for _, fact := range e.Facts {
			marks := renderMarks(fact.Truncated, fact.Redacted)
			if fact.Failed {
				marks += " (failed)"
			}
			if fact.Include == FactIncludeReference {
				fmt.Fprintf(&builder, "- %s -> %s%s\n", fact.ID, fact.Reference, marks)
				continue
			}
			fmt.Fprintf(&builder, "- %s: %s%s\n", fact.ID, strings.ReplaceAll(strings.TrimRight(fact.Value, "\n"), "\n", " "), marks)
		}
	}
	return builder.String()
}

func renderList(builder *strings.Builder, label string, values []string) {
	if len(values) == 0 {
		return
	}
	fmt.Fprintf(builder, "\n## %s\n", label)
	for _, value := range values {
		fmt.Fprintf(builder, "- %s\n", value)
	}
}

func renderMarks(truncated, redacted bool) string {
	switch {
	case truncated && redacted:
		return " (truncated, redacted)"
	case truncated:
		return " (truncated)"
	case redacted:
		return " (redacted)"
	default:
		return ""
	}
}

// Size is the rendered byte count.
func (e PromptEnvelope) Size() int { return len(e.Render()) }

// EstimatedTokens is a deterministic, model-independent estimate used by quota
// admission. Four bytes per token is the conventional English approximation;
// it is a budget input, not a billing figure.
func (e PromptEnvelope) EstimatedTokens() int { return (e.Size() + 3) / 4 }
