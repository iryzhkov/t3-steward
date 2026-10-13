package campaign

import (
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

const compileCleanSourceCheck = "git diff --quiet && git diff --cached --quiet"

// Fixed compiler destinations only; authored inputs never become shell text.
type compileReportPaths struct {
	Continuation, Handoff, Verification, Review string
}

func compilerReports() compileReportPaths {
	return compileReportPaths{"artifacts/continuation.md", "artifacts/handoff.md", "artifacts/verification.log", "artifacts/review.md"}
}

func (p compileReportPaths) outputs(review bool) []string {
	paths := []string{p.Continuation, p.Handoff, p.Verification}
	if review {
		paths = append(paths, p.Review)
	}
	return paths
}

// Commands are read-only, emit only fixed names/reasons, and fail closed on
// Git errors. Untracked regular reports from this attempt are allowed.
func compileReportCheck(fresh bool) string {
	parts := []string{
		"if [ -L artifacts ] || { [ -e artifacts ] && [ ! -d artifacts ]; }; then printf '%s\\n' 'report-path collision: artifacts parent'; exit 1; fi",
	}
	if !fresh {
		parts = append(parts,
			"if [ ! -d .git ] && [ ! -f .git ]; then printf '%s\\n' 'report-path check: Git workspace required'; exit 1; fi",
			"inside=$(git rev-parse --is-inside-work-tree 2>/dev/null) && prefix=$(git rev-parse --show-prefix 2>/dev/null) && [ \"$inside\" = true ] && [ -z \"$prefix\" ] || { printf '%s\\n' 'report-path check: Git workspace failed'; exit 1; }",
			"entries=$(git ls-files --stage -- ':(top,literal)artifacts' 2>/dev/null) || { printf '%s\\n' 'report-path check: Git index failed'; exit 1; }",
			"tab=$(printf '\\t'); case \"$entries\" in *\"$tab\"artifacts) printf '%s\\n' 'report-path collision: artifacts index entry'; exit 1;; esac")
	}
	paths := compilerReports().outputs(true)
	parts = append(parts, "for p in "+strings.Join(paths, " ")+"; do",
		"if [ -L \"$p\" ] || { [ -e \"$p\" ] && [ ! -f \"$p\" ]; }; then printf 'report-path collision: %s nonregular destination\\n' \"$p\"; exit 1; fi")
	if !fresh {
		parts = append(parts,
			"entries=$(git ls-files -- ':(top,literal)'\"$p\" 2>/dev/null) || { printf '%s\\n' 'report-path check: Git index failed'; exit 1; }",
			"if [ -n \"$entries\" ]; then printf 'report-path collision: %s index entry\\n' \"$p\"; exit 1; fi")
	}
	parts = append(parts, "done")
	return strings.Replace(strings.Join(parts, "; "), "; do; ", "; do ", 1)
}

// Project the supported ManifestPreflight into the compiler's minimal YAML,
// omitting its unused probe/context fields without changing the public owner.
type compiledPreflight backlog.ManifestPreflight

func (p compiledPreflight) MarshalYAML() (any, error) {
	type step struct {
		ID             string        `yaml:"id"`
		Kind           string        `yaml:"kind"`
		Command        []string      `yaml:"command,flow"`
		FailurePolicy  string        `yaml:"failure_policy"`
		MaxOutputBytes int           `yaml:"max_output_bytes"`
		Timeout        time.Duration `yaml:"timeout"`
	}
	steps := make([]step, 0, len(p.Steps))
	for _, s := range p.Steps {
		steps = append(steps, step{s.ID, s.Kind, s.Command, s.FailurePolicy, s.MaxOutputBytes, s.Timeout})
	}
	return struct {
		Steps []step `yaml:"steps"`
	}{steps}, nil
}

func compileReportPreflight(check string) *compiledPreflight {
	return &compiledPreflight{Steps: []backlog.ManifestPreflightStep{{
		ID: "report-path-collision", Kind: backlog.PreflightKindCheck,
		Command: []string{"sh", "-c", check}, FailurePolicy: backlog.PreflightPolicyRequirePass,
		MaxOutputBytes: 4096, Timeout: 2 * time.Minute,
	}}}
}

func compileReportChecks(authored []string, check string, fresh bool) []string {
	result := append([]string(nil), authored...)
	generated := []string{check}
	if !fresh {
		generated = append(generated, compileCleanSourceCheck)
	}
	for _, command := range generated {
		found := false
		for _, existing := range result {
			if existing == command {
				found = true
				break
			}
		}
		if !found {
			result = append(result, command)
		}
	}
	return result
}
