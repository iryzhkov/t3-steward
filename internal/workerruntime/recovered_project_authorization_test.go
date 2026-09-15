package workerruntime

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveredPreparationRechecksCurrentProjectAuthorization(t *testing.T) {
	for _, revoked := range []bool{true, false} {
		t.Run(fmt.Sprintf("revoked=%t", revoked), func(t *testing.T) {
			repository, commit := makeGitRepository(t)
			pkg := testPackage()
			pkg.Environment.Repository = "https://example.com/steward.git"
			pkg.Environment.Ref = commit
			pkg.Environment.RequiredCredentials = []string{"github-token"}
			pkg.StaticInputs = []workerproto.ArtifactObject{testArtifact("input-1", "inputs/context.md", "context")}
			manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
			if err != nil {
				t.Fatal(err)
			}

			catalog, err := backlog.NewProjectCatalog(
				[]backlog.ProjectDefinition{{
					Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit,
					T3ProjectTemplate: "development", SetupProfile: "go", RequiredCredentials: []string{"github-token"},
				}},
				[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
			)
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			artifactRoot := filepath.Join(root, "artifacts")
			runsRoot := filepath.Join(root, "runs")
			t.Cleanup(func() {
				if err := removeReadOnlyTree(runsRoot); err != nil {
					t.Error(err)
				}
			})
			publisher := &recordingPublisher{}
			control := &recordingT3{message: "finished\nBACKLOG STATUS: done", archive: []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)}
			source := mapArtifactSource{
				pkg.Prompt.ID:          []byte("prompt"),
				pkg.StaticInputs[0].ID: []byte("context"),
			}
			driver, err := NewLocalDriver(LocalDriver{
				Config: LocalDriverConfig{
					Authorization:   &domain.WorkerInventory{ID: pkg.WorkerID, Providers: []domain.WorkerProviderInventory{{InstanceID: pkg.Route.ProviderInstanceID, Models: []string{pkg.Route.Model}, QuotaPoolID: pkg.Route.QuotaPoolID}}},
					CatalogRevision: "catalog-1", ArtifactRoot: artifactRoot,
					RunsRoot: runsRoot,
				},
				Catalog: catalog,
				Workspace: backlog.WorkspacePreparer{
					Cache:     staticRepositoryCache{path: repository},
					Processes: successfulProcessRunner{},
				},
				Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification-1" }},
				Source:    source, Publisher: publisher, T3: control, Now: func() time.Time { return runtimeTestNow },
				Credentials: EnvironmentCredentialChecker{Lookup: func(name string) (string, bool) {
					return "resolved-secret", name == "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN"
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = driver.Prepare(context.Background(), pkg)
			if err != nil {
				t.Fatal(err)
			}

			// Leave the durable journal at preparing, as after a crash between
			// filesystem publication and the prepared journal write.
			r := newClaimedRuntime(t, t.TempDir(), &fakeDriver{})
			r.driver = driver
			command := testCommand(t, r, domain.WorkerCommandDispatch, "dispatch")
			if err := r.journal.update(func(state *journalState) error {
				record := state.Attempts["assignment-1"]
				record.Package = manifest
				record.Phase = PhasePreparing
				record.CommandRequests = map[string]domain.WorkerCommand{command.ID: command}
				state.Attempts["assignment-1"] = record
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// Worker binding catalog excludes projects removed from its eligibility.
			if revoked {
				driver.Catalog, err = backlog.NewProjectCatalog(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			// An unrelated catalog change alone must not invalidate a still-authorized
			// prepared environment.
			driver.Config.CatalogRevision = "catalog-2"
			if err := r.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if revoked && len(control.created) != 0 {
				t.Fatalf("revoked project dispatched from recovered workspace: %+v", control.created)
			}
			if !revoked && len(control.created) != 1 {
				t.Fatalf("authorized recovered workspace withheld: %+v", control.created)
			}
		})
	}
}
