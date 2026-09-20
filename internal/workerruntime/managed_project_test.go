package workerruntime

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Every activation of one run files its overseer session in one project.
//
// Keying the project by the activation's thread put one project in the T3
// picker per activation -- five for a single run on the fleet -- and a person
// choosing a project for their own session had to read past every one of them.
// The project is also addressed by an owned metadata directory rather than by
// the activation's prepared workspace, because a project is identified by its
// workspace root and every activation of a run prepares a different one.
func TestSupervisionProjectIsOnePerRun(t *testing.T) {
	root := t.TempDir()
	control := &recordingT3{}
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: root, RunsRoot: filepath.Join(root, "runs")}, T3: control}
	for _, activation := range []struct{ id, thread, attempt string }{
		{"activation-1", "thread-a", "attempt-a"},
		{"activation-2", "thread-b", "attempt-b"},
	} {
		pkg := testActivationPackage()
		pkg.Supervision.ActivationID = activation.id
		pkg.Identity.ThreadID = activation.thread
		pkg.Identity.AttemptID = activation.attempt
		if err := driver.createActivationThread(context.Background(), pkg, filepath.Join(root, activation.attempt)); err != nil {
			t.Fatalf("%s: %v", activation.id, err)
		}
	}
	if len(control.managedProjects) != 2 || control.managedProjects[0] != control.managedProjects[1] {
		t.Fatalf("one run took more than one supervision project: %+v", control.managedProjects)
	}
	project := control.managedProjects[0]
	if filepath.Dir(project.WorkspaceRoot) != filepath.Join(driver.Config.RunsRoot, ".projects") {
		t.Fatalf("supervision project root is not owned metadata: %+v", project)
	}
	if len(control.created) != 2 {
		t.Fatalf("created = %+v", control.created)
	}
	for _, created := range control.created {
		if created.ProjectID != "managed-project-id" {
			t.Fatalf("activation thread did not use the supervision project: %+v", created)
		}
		if created.WorktreePath == project.WorkspaceRoot {
			t.Fatalf("the activation opened in the project's metadata directory rather than its own workspace: %+v", created)
		}
	}
}

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
