package workerruntime

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestLocalDriverManagedProjectStableAcrossAttempts(t *testing.T) {
	pkg := testPackage()
	pkg.Environment.T3Project = ""
	if err := workerproto.ValidateExecutionPackage(pkg); err != nil {
		t.Fatalf("managed project package rejected: %v", err)
	}
	root := t.TempDir()
	control := &recordingT3{}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: root, RunsRoot: filepath.Join(root, "runs")}, T3: control}
	cachePath := filepath.Join(root, "objects", pkg.Prompt.SHA256)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, []byte("prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []string{"attempt-a", "attempt-b"} {
		pkg.Identity.AttemptID = attempt
		if err := driver.CreateThread(context.Background(), pkg, filepath.Join(root, attempt)); err != nil {
			t.Fatal(err)
		}
	}
	if control.resolveProject != "" || len(control.managedProjects) != 2 ||
		control.managedProjects[0] != control.managedProjects[1] {
		t.Fatalf("unstable project provisioning: %+v", control.managedProjects)
	}
	for _, created := range control.created {
		if created.ProjectID != "managed-project-id" {
			t.Fatalf("thread did not use managed project: %+v", created)
		}
	}
	if filepath.Dir(control.managedProjects[0].WorkspaceRoot) != filepath.Join(driver.Config.RunsRoot, ".projects") {
		t.Fatalf("project root is not owned metadata: %+v", control.managedProjects[0])
	}
	control.resolveErr = io.EOF
	if err := driver.CreateThread(context.Background(), pkg, root); err == nil {
		t.Fatal("creation continued after ambiguous project result")
	}
	if len(control.created) != 2 {
		t.Fatal("started a thread without confirmed project")
	}
}
