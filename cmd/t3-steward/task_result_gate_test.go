package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestTaskResultCollectsGateEvidenceAndCacheProvenance(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		f := newTaskResultFixture(t)
		raw := `{"passed":true,"cached":true,"originalAttempt":"attempt-original","treeHash":"tree-identity","logArtifact":"gate/log.txt"}`
		for _, entry := range []struct{ id, name, media, body string }{{"gate-attempt-task-1", "gate", "application/json", raw}, {"gate-log-attempt-task-1", "gate/log.txt", "text/plain", "gate full output"}} {
			f.detail.Artifacts = append(f.detail.Artifacts, backlogadmin.Artifact{Metadata: backlogadmin.ArtifactMetadata{
				ID: entry.id, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-task-1", Kind: domain.ArtifactGate, Name: entry.name, MediaType: entry.media, Size: int64(len(entry.body)),
			}})
			f.content[entry.id] = entry.body
		}
		args := []string{"run-1"}
		if asJSON {
			args = append(args, "--json")
		}
		if err := f.run(args...); err != nil {
			t.Fatal(err)
		}
		if got := readFile(t, f.resultsDir(), "run-1", "task", "gate", "report.json"); got != raw {
			t.Fatalf("gate report=%q", got)
		}
		if got := readFile(t, f.resultsDir(), "run-1", "task", "gate", "log.txt"); got != "gate full output" {
			t.Fatalf("gate log=%q", got)
		}
		if asJSON {
			if !strings.Contains(f.stdout.String(), `"cached": true`) || !strings.Contains(f.stdout.String(), `"originalAttempt": "attempt-original"`) {
				t.Fatalf("missing inline provenance: %s", f.stdout.String())
			}
		} else if !strings.Contains(f.stdout.String(), "cached") || !strings.Contains(f.stdout.String(), "attempt-original") {
			t.Fatalf("missing text provenance: %s", f.stdout.String())
		}
	}
}
