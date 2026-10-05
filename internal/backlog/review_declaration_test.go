package backlog

import (
	"strings"
	"testing"
)

func declaredManifestYAML() string {
	return "version: 2\nname: declared\nenvironment: {project: t3-steward, ref: " + strings.Repeat("c", 40) + "}\ninputs: [inputs/criteria.md]\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n    review_requirements:\n      version: 1\n      risk: routine\n      criteria_file: inputs/criteria.md\n      required_reviewers: 2\n      min_provider_families: 2\n      members:\n        - {id: one, role: independent, route: codex/org/sol, required: true}\n        - {id: two, role: independent, route: other/model, required: true}\n"
}
func TestReviewDeclarationParserAndIngestBefore(t *testing.T) {
	raw := declaredManifestYAML()
	if _, err := ParseManifest([]byte(raw)); err != nil {
		t.Fatalf("valid review declaration must parse: %v", err)
	}
	bundle := validBundle(t)
	writeBundleFile(t, bundle, "inputs/criteria.md", "retained criteria")
	rewriteBundleManifest(t, bundle, raw)
	if _, err := LoadManifest(bundle); err != nil {
		t.Fatalf("valid submitted criteria must load: %v", err)
	}
}
