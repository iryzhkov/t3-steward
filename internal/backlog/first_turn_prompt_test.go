package backlog

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestFirstTurnTaskContract(t *testing.T) {
	for _, tc := range []struct {
		name, prompt string
		outputs      []domain.ArtifactDeclaration
	}{
		{"executor", "Implement the bounded change.\nKeep this author text intact.", []domain.ArtifactDeclaration{{Name: "handoff.md"}, {Name: "implementation", Commit: &domain.CommitOutput{Revision: "HEAD"}}}},
		{"review", "Review the producer commit; do not edit source.", []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "verdict.json"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := FirstTurnPrompt(tc.prompt, tc.outputs)
			if !strings.HasPrefix(got, "## Steward task contract\n") || !strings.HasSuffix(got, tc.prompt) {
				t.Fatal("contract must precede the unchanged author prompt", got)
			}
			for _, want := range []string{
				"continuation.md", "workspace root", "Keep the tree clean",
				"Do not submit nested campaigns, tasks or reviews",
				"Same-provider native subagents", "bounded reading or sub-work",
				"never replace a declared review", "their output is your responsibility",
				"Never end a turn while a command you started is still running",
				"start it detached with output to a file", "poll in the foreground until it exits",
				"If this is a review task, do not edit source; write the declared verdict outputs",
				"no task-bound wait registered", "not a request for extra turns",
				"t3-steward wait add --task current",
				"longer than a few minutes must be a task-bound wait",
				"releases this\ntask's executor and quota slots", "never a blocking `--wait` command",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q", want)
				}
			}
			for _, output := range tc.outputs {
				if !strings.Contains(got, "`"+output.Name+"`") {
					t.Errorf("missing declared output %s", output.Name)
				}
			}
			preamble := strings.TrimSuffix(got, tc.prompt)
			if lines := strings.Count(preamble, "\n"); lines > 40 {
				t.Errorf("contract too long: %d lines", lines)
			}
		})
	}
}
