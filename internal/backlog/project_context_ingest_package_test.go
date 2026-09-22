package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestManifestIngestReopenResolvesStaticContextIntoOffer(t *testing.T) {
	now := time.Date(2026, 9, 22, 18, 0, 0, 0, time.UTC)
	for name, reference := range map[string]string{
		"git": fmt.Sprintf(`
          - id: source
            kind: git
            uri: ssh://git/steward
            revision: %s
            artifact: inputs/context.txt`, strings.Repeat("b", 40)),
		"jocasta": `
          - id: source
            kind: jocasta
            uri: jocasta:0123456789abcdef0123456789abcdef@7
            revision: "7"
            artifact: inputs/context.txt`,
	} {
		t.Run(name, func(t *testing.T) {
			bundle := validBundle(t)
			writeBundleFile(t, bundle, "inputs/context.txt", name+" retained context")
			rewriteBundleManifest(t, bundle, fmt.Sprintf(`
version: 2
name: context
class: required
environment: {project: steward, ref: main}
inputs: [inputs/context.txt]
routes:
  - {host: normandy, instance: codex, model: gpt-5.6-sol, quota_pool: openai}
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    outputs: [out.txt]
    context:
      version: 1
      revision: context-1
      objective: use exact retained context
      budget: one attempt
      outputs: [out.txt]
      required_references: [source]
      references:%s
      setup: ["true"]
      checks: ["true"]
      freshness:
        observed_at: 2026-09-22T17:59:00Z
        fresh_through: 2026-09-22T19:00:00Z
`, reference))
			statePath := filepath.Join(t.TempDir(), "state.db")
			store, err := sqlite.OpenMigrated(statePath)
			if err != nil {
				t.Fatal(err)
			}
			ingester := BundleIngester{
				StorageRoot: filepath.Join(t.TempDir(), "artifacts"), Store: store,
				Now: func() time.Time { return now },
			}
			t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
			if _, err := ingester.Ingest(context.Background(), bundle); err != nil {
				t.Fatalf("manifest ingest: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlite.OpenMigrated(statePath)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := store.LoadCoordinatorRecords(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			attempt := &loaded.Attempts[0]
			assignment := domain.Assignment{
				ID: "assignment", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1",
				Route: domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol", QuotaPoolID: "openai"},
				State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", ThreadID: "thread",
				LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
			}
			attempt.AssignmentID = assignment.ID
			loaded.Assignments = []domain.Assignment{assignment}
			builder := packageBuilder(t, loaded)
			builder.WorkerCapabilities = map[string][]string{"normandy": {workerproto.PackageCapabilityProjectContext}}
			offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
			if err != nil {
				t.Fatalf("reopened context failed at offer/package: %v", err)
			}
			resolved := offer.Package.Package.Context
			if resolved == nil || resolved.Status != domain.ProjectContextPinned ||
				len(resolved.References) != 1 || resolved.References[0].Binding == nil ||
				resolved.References[0].Binding.Path != "inputs/inputs/context.txt" ||
				resolved.References[0].Status != domain.ProjectContextPinned {
				t.Fatalf("reopened context was not resolved: %+v", resolved)
			}
			if strings.Contains(fmt.Sprintf("%+v", resolved), "unresolved") {
				t.Fatalf("unresolved placeholder reached package: %+v", resolved)
			}
		})
	}
}
