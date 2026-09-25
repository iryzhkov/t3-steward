package backlog

import (
	"strings"
	"testing"
)

// A fresh workspace has no Git repository, so a declared commit failed only
// when the task finished, after its quota was spent. validate refuses it.
func TestFreshManifestRefusesCommits(t *testing.T) {
	const raw = `
version: 2
name: fresh-commits
environment:
  project: scratch
  type: fresh
tasks:
  implement:
    prompt_file: prompts/implement.md
    commits:
      - name: implementation
`
	_, err := ParseManifest([]byte(raw))
	if err == nil || !strings.Contains(err.Error(), "commits need a Git workspace") {
		t.Fatalf("parse = %v", err)
	}
	_, err = ParseManifest([]byte(strings.Replace(raw, "type: fresh", "type: git", 1)))
	if err != nil {
		t.Fatalf("git workspace refused commits: %v", err)
	}
}
