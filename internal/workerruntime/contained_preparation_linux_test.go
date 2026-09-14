//go:build linux

package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

func TestContainedPreparationReusesPublishedWorkspaceAfterReadinessFailure(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() {
				return os.Chmod(path, 0700)
			}
			return err
		})
	})
	data := filepath.Join(root, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	pkg := testPackage()
	pkg.Environment.Type = backlog.EnvironmentFresh
	pkg.Environment.Repository = ""
	pkg.Environment.Ref = ""
	pkg.Environment.T3Project = ""
	pkg.Environment.SetupProfile = "steward-fresh-empty"
	pkg.Route.ProviderInstanceID = "opencode"
	f, identity, err := directoryresource.Open(directoryresource.Registration{WorkerID: pkg.WorkerID, ResourceID: "source", Revision: "1", Path: data})
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	binding, err := directoryresource.Bind(identity, "")
	if err != nil {
		t.Fatal(err)
	}
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{binding}
	catalog, err := backlog.NewProjectCatalog([]backlog.ProjectDefinition{{Name: "steward", Type: backlog.EnvironmentFresh, SetupProfile: "steward-fresh-empty", DirectoryBindings: pkg.Environment.DirectoryBindings}}, []backlog.SetupProfile{{Name: "steward-fresh-empty", Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Catalog: catalog, Source: mapArtifactSource{pkg.Prompt.ID: []byte("prompt")}, Publisher: &recordingPublisher{}, T3: &recordingT3{},
		ScopedT3: ContainedT3{Profile: &ContainedProfile{}, Supervisor: providercontainment.Supervisor{Root: filepath.Join(root, "journal")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		_, err := driver.Prepare(context.Background(), pkg)
		if err == nil || !strings.Contains(err.Error(), "runtime roots") {
			t.Fatalf("pass %d: %v", pass, err)
		}
		workspace := filepath.Join(driver.workspacePath(pkg), "workspace")
		if pass == 0 {
			if err = os.WriteFile(filepath.Join(workspace, "retained"), []byte("same"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		raw, err := os.ReadFile(filepath.Join(workspace, "retained"))
		if err != nil || string(raw) != "same" {
			t.Fatal("workspace recreated")
		}
	}
	if _, exists, err := driver.InspectWorkspace(context.Background(), pkg); err != nil || exists {
		t.Fatalf("unprepared server reported ready: %v %v", exists, err)
	}
}
