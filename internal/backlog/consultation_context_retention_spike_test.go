package backlog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type consultationContextReferenceCatalog struct {
	ArtifactCatalog
	references map[string]bool
	commitErr  error
}

func (c *consultationContextReferenceCatalog) CommitArtifactPublication(ctx context.Context, publication domain.ArtifactPublication) (domain.Artifact, error) {
	if c.commitErr != nil {
		return domain.Artifact{}, c.commitErr
	}
	return c.ArtifactCatalog.CommitArtifactPublication(ctx, publication)
}

func (c *consultationContextReferenceCatalog) ArtifactStoragePathReferenced(ctx context.Context, storagePath string) (bool, error) {
	artifactReference, err := c.ArtifactCatalog.ArtifactStoragePathReferenced(ctx, storagePath)
	if err != nil {
		return false, err
	}
	return artifactReference || c.references[storagePath], nil
}

func TestConsultationContextReferenceRetainsBlobAfterProducerMetadataPrune(t *testing.T) {
	ctx := context.Background()
	store, root, publication := coordinatorArtifactFixture(t)
	catalog := &consultationContextReferenceCatalog{ArtifactCatalog: store, references: make(map[string]bool)}
	service := CoordinatorArtifactStore{Root: root, Catalog: catalog}
	content := []byte("immutable consultation context\n")
	setPublicationContent(&publication, content)
	artifact, err := service.Publish(ctx, publication, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	catalog.references[artifact.StoragePath] = true

	expired, skipped, err := service.Prune(ctx, publication.Artifact.CreatedAt.Add(24*time.Hour), nil)
	if err != nil || len(skipped) != 0 || len(expired) != 1 {
		t.Fatalf("prune producer metadata: expired=%#v skipped=%#v err=%v", expired, skipped, err)
	}
	objectPath := filepath.Join(root, filepath.FromSlash(artifact.StoragePath))
	got, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatalf("independently referenced context blob was removed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("retained content=%q, want %q", got, content)
	}
	if artifacts, err := store.LoadArtifacts(ctx, []string{artifact.ID}); err == nil || len(artifacts) != 0 {
		t.Fatalf("producer metadata remains after prune: %#v err=%v", artifacts, err)
	}
}

func TestConsultationContextReferenceProtectsPublicationFailureCleanup(t *testing.T) {
	ctx := context.Background()
	store, root, publication := coordinatorArtifactFixture(t)
	content := []byte("shared context bytes\n")
	setPublicationContent(&publication, content)
	storagePath := filepath.ToSlash(filepath.Join("objects", publication.Artifact.SHA256[:2], publication.Artifact.SHA256))
	catalog := &consultationContextReferenceCatalog{
		ArtifactCatalog: store,
		references:      map[string]bool{storagePath: true},
		commitErr:       errors.New("injected metadata failure"),
	}
	service := CoordinatorArtifactStore{Root: root, Catalog: catalog}
	if _, err := service.Publish(ctx, publication, bytes.NewReader(content)); err == nil {
		t.Fatal("publish unexpectedly succeeded")
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(storagePath))); err != nil {
		t.Fatalf("failure cleanup removed independently referenced blob: %v", err)
	}
}
