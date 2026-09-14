//go:build linux

package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

// Invoked only by the disposable systemd test unit, never a production helper.
func TestContainedCollectionProcessHelper(t *testing.T) {
	args := os.Args
	if len(args) < 4 || args[len(args)-2] != "--spec" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(args[len(args)-1])
	if err != nil {
		t.Fatal(err)
	}
	var spec providercontainment.Spec
	if err = json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	if err = providercontainment.Run(context.Background(), spec, providercontainment.Streams{Stdout: os.Stdout, Stderr: os.Stderr}); err != nil {
		t.Fatal(err)
	}
}

func TestContainedCollectionRecoversSnapshotAndVerifiesInNamespace(t *testing.T) {
	if os.Getenv("T3_STEWARD_REQUIRE_CONTAINMENT_TESTS") != "1" || os.Getenv("T3_STEWARD_REQUIRE_SUPERVISOR_TESTS") != "1" {
		t.Skip("requires containment and systemd")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() {
				return os.Chmod(path, 0700)
			}
			return err
		})
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(root, "runner")
	script := "#!/bin/sh\nexec '" + strings.ReplaceAll(executable, "'", "'\\''") + "' -test.run='^TestContainedCollectionProcessHelper$' -- \"$@\"\n"
	if err = os.WriteFile(wrapper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	pkg := testPackage()
	pkg.Environment.Type = backlog.EnvironmentFresh
	pkg.Route.ProviderInstanceID = "opencode"
	pkg.Verification = []string{"test \"$(cat /data/0/source)\" = source; test ! -e /home/igor; if echo bad > /data/0/source; then exit 9; fi; echo verified > result"}
	pkg.Outputs = []domain.ArtifactDeclaration{{Name: "result"}}
	identity := func(name string, writable bool) directoryresource.Identity {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		f, id, err := directoryresource.Open(directoryresource.Registration{WorkerID: pkg.WorkerID, ResourceID: name, Revision: "1", Path: path, Writable: writable})
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		return id
	}
	home, workspace, data := identity("home", true), identity("workspace", true), identity("data", false)
	if err = os.WriteFile(filepath.Join(data.Registration.Path, "source"), []byte("source"), 0400); err != nil {
		t.Fatal(err)
	}
	binding, err := directoryresource.Bind(data, "")
	if err != nil {
		t.Fatal(err)
	}
	pkg.Environment.DirectoryBindings = []directoryresource.Binding{binding}
	manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: filepath.Join(root, "journal"), Executable: wrapper}, Timeout: time.Second}
	if err = os.Mkdir(manager.Supervisor.Root, 0700); err != nil {
		t.Fatal(err)
	}
	launch := providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID, Spec: providercontainment.Spec{WorkerID: pkg.WorkerID, Home: home, Workspace: workspace, Directories: pkg.Environment.DirectoryBindings, Command: []string{"/bin/true"}}}
	if _, err = manager.Supervisor.Start(ctx, launch); err != nil {
		t.Fatal(err)
	}
	defer manager.Supervisor.Stop(context.Background(), launch)
	for {
		_, err = manager.Supervisor.Result(ctx, launch)
		if err == nil {
			break
		}
		if !errors.Is(err, providercontainment.ErrCommandRunning) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	path, err := manager.recordPath(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err = privateJSON(path+".preparation", containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: launch}); err != nil {
		t.Fatal(err)
	}
	capture := containedCapture{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Thread: &domain.Thread{ID: pkg.Identity.ThreadID, TurnState: "completed"}, Message: "finished",
		Archive: []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)}
	if err = privateJSON(path+".capture", capture); err != nil {
		t.Fatal(err)
	}
	publisher := &recordingPublisher{}
	// No live T3 object or provider profile: recovery uses only durable identity,
	// the terminal archive and a confirmed stopped supervisor.
	driver := &LocalDriver{ScopedT3: manager, Publisher: publisher, Now: time.Now, Finalizer: backlog.AttemptFinalizer{StorageRoot: filepath.Join(root, "artifacts")}}
	if err = driver.Collect(ctx, pkg, workspace.Registration.Path); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 || !publisher.results[0].Finalized.Completion.ExplicitSuccess || publisher.results[0].Finalized.Completion.Failure != "" {
		t.Fatalf("published=%+v", publisher.results)
	}
	if err = manager.stopVerifications(ctx, pkg); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(data.Registration.Path, "source"))
	if err != nil || string(original) != "source" {
		t.Fatalf("dataset changed: %q %v", original, err)
	}
}
