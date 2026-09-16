package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const supervisedIngestionManifest = `
version: 2
name: bundle
class: required
environment: {project: t3-steward}
routes:
  - {host: normandy, instance: codex, model: gpt-5.6-sol, quota_pool: openai}
supervision:
  route: {instance: claudeAgent, model: claude-fable-5-1, quota_pool: claude-main}
  prompt_file: prompts/overseer.md
gates:
  review:
    after: [inspect]
    before: [implement]
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    outputs: [findings.md]
  implement:
    prompt_file: prompts/implement.md
    needs: [inspect]
    inputs_from: {inspect: [findings.md]}
`

// Two submissions of the same supervised campaign declare the same gate name.
// The durable gate identity must be the run's, not the authored word, or the
// second ingestion silently rewrites the first run's gate row.
func TestSupervisedIngestionMintsRunScopedGateIdentities(t *testing.T) {
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	next := 0
	storageRoot := filepath.Join(t.TempDir(), "artifacts")
	ingester := BundleIngester{
		StorageRoot: storageRoot,
		Store:       store,
		Now:         func() time.Time { return now },
		NewID:       func() string { next++; return fmt.Sprintf("%02d", next) },
	}
	t.Cleanup(func() { _ = removeIngestedTree(storageRoot) })

	ctx := context.Background()
	submit := func() (string, map[string]string) {
		t.Helper()
		bundle := validBundle(t)
		writeBundleFile(t, bundle, "prompts/implement.md", "implement prompt")
		writeBundleFile(t, bundle, "prompts/overseer.md", "overseer prompt")
		rewriteBundleManifest(t, bundle, supervisedIngestionManifest)
		submission, err := ingester.Ingest(ctx, bundle)
		if err != nil {
			t.Fatalf("ingest supervised bundle: %v", err)
		}
		taskIDByName := map[string]string{}
		for _, task := range submission.Records.Tasks {
			if task.WorkflowID == submission.WorkflowID {
				taskIDByName[task.Name] = task.ID
			}
		}
		return submission.RunID, taskIDByName
	}
	firstRun, firstTasks := submit()
	secondRun, secondTasks := submit()
	if firstRun == secondRun {
		t.Fatalf("both submissions produced run %q", firstRun)
	}

	check := func(runID string, own, foreign map[string]string) {
		t.Helper()
		snapshot, err := store.LoadSupervisionSnapshot(ctx, runID)
		if err != nil {
			t.Fatalf("snapshot of %q: %v", runID, err)
		}
		if !snapshot.Supervised || len(snapshot.Gates) != 1 {
			t.Fatalf("run %q sees %d gates (supervised=%v)", runID, len(snapshot.Gates), snapshot.Supervised)
		}
		gate := snapshot.Gates[0]
		if gate.RunID != runID || gate.Definition.ID != runID+":review" {
			t.Fatalf("run %q gate identity = %q (run %q)", runID, gate.Definition.ID, gate.RunID)
		}
		if gate.Definition.Name != "review" {
			t.Fatalf("run %q lost the authored gate name: %q", runID, gate.Definition.Name)
		}
		if gate.State != domain.GatePendingEvidence {
			t.Fatalf("run %q gate state = %q", runID, gate.State)
		}
		if !gate.Definition.Protects(own["implement"]) {
			t.Fatalf("run %q gate does not protect its own task %q: %#v", runID, own["implement"], gate.Definition)
		}
		if gate.Definition.Protects(foreign["implement"]) {
			t.Fatalf("run %q gate protects a foreign task %q: %#v", runID, foreign["implement"], gate.Definition)
		}
	}
	check(firstRun, firstTasks, secondTasks)
	check(secondRun, secondTasks, firstTasks)
}
