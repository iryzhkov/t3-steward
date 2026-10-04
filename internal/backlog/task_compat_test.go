package backlog

import (
	"os"
	"testing"
)

// Historical frontmatter decoding remains available for modern campaign prompt
// compatibility. Decoding bytes does not adapt files into executable workflows.
func TestRetainedFrontmatterFixtures(t *testing.T) {
	for _, tc := range []struct {
		path, project, title, instance, model string
		maxTurns                              int
	}{
		{"testdata/t3-backlog-default.md", "t3-steward development", "Continue backlog orchestrator implementation", "", "", 6},
		{"testdata/t3-backlog-all-fields.md", "project: quoted", "Review \"scheduler\"", "codex", "gpt-5.6-sol", 7},
		{"testdata/t3-job-enqueue.md", "nightly maintenance", "Scheduled review", "claudeAgent", "claude-opus-5", 5},
	} {
		t.Run(tc.path, func(t *testing.T) {
			raw, err := os.ReadFile(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got.Project != tc.project || got.Title != tc.title || got.Instance != tc.instance || got.Model != tc.model || got.MaxTurns != tc.maxTurns {
				t.Fatalf("frontmatter = %+v", got)
			}
			if got.Prompt == "" {
				t.Fatal("lost prompt body")
			}
		})
	}
}
