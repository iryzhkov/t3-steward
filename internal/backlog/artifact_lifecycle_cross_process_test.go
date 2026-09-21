package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const artifactLifecycleHelperEnv = "T3_ARTIFACT_LIFECYCLE_HELPER"

func TestArtifactLifecycleLockCrossProcessHelper(t *testing.T) {
	if os.Getenv(artifactLifecycleHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	var publication domain.ArtifactPublication
	raw, err := os.ReadFile(os.Getenv("T3_ARTIFACT_PUBLICATION"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &publication); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(os.Getenv("T3_ARTIFACT_CONTENT"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenMigrated(os.Getenv("T3_ARTIFACT_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := os.WriteFile(os.Getenv("T3_ARTIFACT_READY"), []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	service := CoordinatorArtifactStore{Root: os.Getenv("T3_ARTIFACT_ROOT"), Catalog: store}
	if _, err := service.Publish(context.Background(), publication, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactLifecycleLockSerializesCrossProcessCanonicalRootAlias(t *testing.T) {
	ctx := context.Background()
	store, root, publication := coordinatorArtifactFixture(t)
	content := []byte("cross-process shared blob\n")
	setPublicationContent(&publication, content)
	publication.Artifact.ID = "artifact-cross-process"
	publication.Artifact.Name = "cross-process-context"
	raw, err := json.Marshal(publication)
	if err != nil {
		t.Fatal(err)
	}
	control := t.TempDir()
	publicationPath := filepath.Join(control, "publication.json")
	contentPath := filepath.Join(control, "content")
	readyPath := filepath.Join(control, "ready")
	if err := os.WriteFile(publicationPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(contentPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	storagePath := filepath.ToSlash(filepath.Join("objects", publication.Artifact.SHA256[:2], publication.Artifact.SHA256))
	lock, err := acquireFileLock(ctx, root, "artifact-object:"+storagePath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestArtifactLifecycleLockCrossProcessHelper$", "-test.v")
	cmd.Env = append(os.Environ(),
		artifactLifecycleHelperEnv+"=1",
		"T3_ARTIFACT_DATABASE="+filepath.Join(filepath.Dir(root), "state.db"),
		"T3_ARTIFACT_ROOT="+filepath.Join(root, "objects", ".."),
		"T3_ARTIFACT_PUBLICATION="+publicationPath,
		"T3_ARTIFACT_CONTENT="+contentPath,
		"T3_ARTIFACT_READY="+readyPath,
	)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		_ = lock.Close()
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = lock.Close()
			_ = cmd.Process.Kill()
			t.Fatalf("helper did not reach publish barrier: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case err := <-exited:
		_ = lock.Close()
		t.Fatalf("helper crossed process lock early: %v: %s", err, output.String())
	case <-time.After(100 * time.Millisecond):
	}
	if err := lock.Close(); err != nil {
		_ = cmd.Process.Kill()
		t.Fatal(err)
	}
	if err := <-exited; err != nil {
		t.Fatalf("helper publish failed after release: %v: %s", err, output.String())
	}

	artifacts, err := store.LoadArtifacts(ctx, []string{publication.Artifact.ID})
	if err != nil || len(artifacts) != 1 {
		t.Fatalf("published metadata=%#v err=%v", artifacts, err)
	}
	service := CoordinatorArtifactStore{Root: root, Catalog: store}
	_, file, err := service.Open(ctx, publication.Artifact.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(file.Name())
	_ = file.Close()
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("retained bytes=%q err=%v", got, err)
	}
}
