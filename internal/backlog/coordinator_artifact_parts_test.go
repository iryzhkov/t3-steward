package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCoordinatorArtifactMigratesLegacyPartsAndRefusesMissing(t *testing.T) {
	ctx := context.Background()
	db, root, publication := coordinatorArtifactFixture(t)
	content := bytes.Repeat([]byte("l"), int(workerproto.StreamArtifactLimit)+1)
	setPublicationContent(&publication, content)
	service := CoordinatorArtifactStore{Root: root, Catalog: db}
	artifact, err := service.Publish(ctx, publication, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(root, filepath.FromSlash(artifact.StoragePath))
	t.Cleanup(func() { _ = removeArtifactParts(objectPath + ".parts") })
	// Model an oversized object produced before this release.
	if err := removeArtifactParts(objectPath + ".parts"); err != nil {
		t.Fatal(err)
	}
	got, file, err := service.Open(ctx, artifact.ID)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	if got.SHA256 != artifact.SHA256 || got.Size != artifact.Size {
		t.Fatal("legacy metadata changed")
	}
	if _, err := os.Stat(filepath.Join(objectPath+".parts", "manifest.json")); err != nil {
		t.Fatal(err)
	}
	// Existing incomplete parts are refused, never silently rebuilt.
	if err := os.Chmod(objectPath+".parts", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(objectPath+".parts", "00000001")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Open(ctx, artifact.ID); err == nil {
		t.Fatal("missing retained part accepted")
	}
}

func TestRetainedArtifactPartsShareCASAcrossMetadata(t *testing.T) {
	content := bytes.Repeat([]byte("a"), int(workerproto.StreamArtifactLimit)+1)
	digest := sha256.Sum256(content)
	hash := hex.EncodeToString(digest[:])
	objectPath := filepath.Join(t.TempDir(), "object")
	t.Cleanup(func() { _ = removeArtifactParts(objectPath + ".parts") })
	if err := os.WriteFile(objectPath, content, 0o400); err != nil {
		t.Fatal(err)
	}

	first := domain.Artifact{
		ID: "artifact-one", Name: "one.txt", Kind: domain.ArtifactOutput,
		MediaType: "text/plain", Size: int64(len(content)), SHA256: hash,
	}
	if err := retainArtifactParts(objectPath, objectPath, first); err != nil {
		t.Fatalf("retain first metadata variant: %v", err)
	}

	second := first
	second.ID = "artifact-two"
	second.Name = "two.json"
	second.Kind = domain.ArtifactInput
	second.MediaType = "application/json"
	if err := retainArtifactParts(objectPath, objectPath, second); err != nil {
		t.Fatalf("share parts for same CAS digest: %v", err)
	}

	expected := workerproto.ArtifactObject{
		ID: "retained", Path: "content", Kind: "blob",
		MediaType: "application/octet-stream", Size: int64(len(content)), SHA256: hash,
	}
	if err := verifyRetainedArtifactParts(objectPath, expected); err != nil {
		t.Fatalf("verify shared parts: %v", err)
	}
	partPath := filepath.Join(objectPath+".parts", "00000001")
	if err := os.Chmod(partPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyRetainedArtifactParts(objectPath, expected); err == nil {
		t.Fatal("corrupt retained part was accepted")
	}
}
