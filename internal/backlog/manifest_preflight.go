package backlog

import (
	"fmt"
	"strings"
	"time"
)

// Preflight step kinds. A check step establishes baseline evidence and decides
// pass or fail; a context step collects facts and does not decide pass or fail
// unless it is explicitly marked required.
const (
	PreflightKindCheck   = "check"
	PreflightKindContext = "context"
)

// Preflight failure policies. record launches with the failing baseline
// reported; require-pass blocks the launch.
const (
	PreflightPolicyRecord      = "record"
	PreflightPolicyRequirePass = "require-pass"
)

// Preflight prompt inclusion modes for a step result.
const (
	PreflightIncludeSummary   = "summary"
	PreflightIncludeReference = "reference"
	PreflightIncludeOmit      = "omit"
)

// Defaults for the bounded fields of a preflight step. The output bound is
// 64 KiB, which is large enough for a build or test summary and small enough
// that a runaway command cannot become prompt bulk; the timeout is two minutes,
// on the assumption that a baseline check is fast and a slow one is a mistake
// worth reporting rather than waiting out.
const (
	DefaultPreflightMaxOutputBytes = 64 << 10
	DefaultPreflightTimeout        = 2 * time.Minute
)

// ManifestPreflight declares the bounded work a worker performs after the
// environment is ready and before a provider session opens. This package
// parses, defaults and validates the declaration only; executing it, caching
// its evidence and building a prompt from it belong to the implementing lane.
type ManifestPreflight struct {
	Steps []ManifestPreflightStep `yaml:"steps"`
}

// ManifestPreflightStep is one ordered step. Exactly one of Command or Probe
// names the work: Command is an argv list run inside the task's environment,
// Probe names a built-in probe whose registry the implementing lane owns.
type ManifestPreflightStep struct {
	ID             string        `yaml:"id"`
	Kind           string        `yaml:"kind"`
	Command        []string      `yaml:"command"`
	Probe          string        `yaml:"probe"`
	FailurePolicy  string        `yaml:"failure_policy"`
	Include        string        `yaml:"include"`
	MaxOutputBytes int           `yaml:"max_output_bytes"`
	Timeout        time.Duration `yaml:"timeout"`
	// Required marks a context step whose failure must block the launch. It is
	// only meaningful for a context step: a check step already states that
	// through failure_policy.
	Required bool `yaml:"required"`
}

// effectivePreflight merges a workflow-level declaration into a task-level one.
// A task that declares any step replaces the inherited list wholesale rather
// than merging step by step, because the steps are ordered and positional: two
// lists cannot be interleaved without inventing an order neither author wrote.
// This follows the route-inheritance precedent in applyManifestDefaults, where
// task routes replace workflow routes instead of extending them.
func effectivePreflight(workflow, task ManifestPreflight) ManifestPreflight {
	if len(task.Steps) > 0 {
		return ManifestPreflight{Steps: clonePreflightSteps(task.Steps)}
	}
	return ManifestPreflight{Steps: clonePreflightSteps(workflow.Steps)}
}

func clonePreflightSteps(steps []ManifestPreflightStep) []ManifestPreflightStep {
	if len(steps) == 0 {
		return nil
	}
	result := make([]ManifestPreflightStep, len(steps))
	copy(result, steps)
	for i := range result {
		if steps[i].Command != nil {
			result[i].Command = append([]string(nil), steps[i].Command...)
		}
	}
	return result
}

// applyPreflightDefaults fills the defaulted fields of every step. It is
// idempotent, so a merged list that was already defaulted at workflow level is
// unchanged by a second pass.
func applyPreflightDefaults(preflight *ManifestPreflight) {
	for i := range preflight.Steps {
		step := &preflight.Steps[i]
		if step.FailurePolicy == "" {
			step.FailurePolicy = PreflightPolicyRecord
		}
		if step.Include == "" {
			step.Include = PreflightIncludeSummary
		}
		if step.MaxOutputBytes == 0 {
			step.MaxOutputBytes = DefaultPreflightMaxOutputBytes
		}
		if step.Timeout == 0 {
			step.Timeout = DefaultPreflightTimeout
		}
	}
}

// validatePreflight rejects a preflight declaration, naming the offending step
// by its declared ID, or by its position when the ID itself is the problem.
func validatePreflight(label string, preflight ManifestPreflight) error {
	seen := make(map[string]struct{}, len(preflight.Steps))
	for i, step := range preflight.Steps {
		position := fmt.Sprintf("%s step[%d]", label, i)
		if step.ID == "" {
			return fmt.Errorf("%s requires an id", position)
		}
		if !manifestNamePattern.MatchString(step.ID) {
			return fmt.Errorf("%s has invalid id %q", position, step.ID)
		}
		if _, duplicate := seen[step.ID]; duplicate {
			return fmt.Errorf("%s duplicates step id %q", label, step.ID)
		}
		seen[step.ID] = struct{}{}

		stepLabel := fmt.Sprintf("%s step %q", label, step.ID)
		if step.Kind != PreflightKindCheck && step.Kind != PreflightKindContext {
			return fmt.Errorf("%s kind %q is not one of check, context", stepLabel, step.Kind)
		}
		switch {
		case len(step.Command) == 0 && step.Probe == "":
			return fmt.Errorf("%s must set command or probe", stepLabel)
		case len(step.Command) > 0 && step.Probe != "":
			return fmt.Errorf("%s sets both command and probe, which are mutually exclusive", stepLabel)
		}
		for argument, value := range step.Command {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s command[%d] is empty", stepLabel, argument)
			}
		}
		if step.Probe != "" && !manifestNamePattern.MatchString(step.Probe) {
			return fmt.Errorf("%s has invalid probe %q", stepLabel, step.Probe)
		}
		if step.FailurePolicy != PreflightPolicyRecord && step.FailurePolicy != PreflightPolicyRequirePass {
			return fmt.Errorf("%s failure_policy %q is not one of record, require-pass", stepLabel, step.FailurePolicy)
		}
		// A context step collects facts. Letting it block a launch without
		// being declared required would turn an optional observation into a
		// gate by accident, so the author has to say required: true.
		if step.Kind == PreflightKindContext && step.FailurePolicy == PreflightPolicyRequirePass && !step.Required {
			return fmt.Errorf("%s is a context step with failure_policy require-pass but is not marked required", stepLabel)
		}
		switch step.Include {
		case PreflightIncludeSummary, PreflightIncludeReference, PreflightIncludeOmit:
		default:
			return fmt.Errorf("%s include %q is not one of summary, reference, omit", stepLabel, step.Include)
		}
		if step.MaxOutputBytes <= 0 {
			return fmt.Errorf("%s max_output_bytes must be positive, got %d", stepLabel, step.MaxOutputBytes)
		}
		if step.Timeout <= 0 {
			return fmt.Errorf("%s timeout must be positive, got %s", stepLabel, step.Timeout)
		}
	}
	return nil
}
