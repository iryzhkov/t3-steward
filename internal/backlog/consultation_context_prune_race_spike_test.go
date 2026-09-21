package backlog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type consultationPruneRaceCatalog struct {
	ArtifactCatalog
	observedUnreferenced chan struct{}
	adopted              chan struct{}
}

func (c *consultationPruneRaceCatalog) ArtifactStoragePathReferenced(ctx context.Context, storagePath string) (bool, error) {
	referenced, err := c.ArtifactCatalog.ArtifactStoragePathReferenced(ctx, storagePath)
	if err != nil || referenced {
		return referenced, err
	}
	close(c.observedUnreferenced)
	<-c.adopted
	// Return the result observed before adoption, exactly as the production
	// read-then-unlink seam does.
	return false, nil
}

func TestConsultationPruneSerializesConcurrentBlobAdoption(t *testing.T) {
	ctx := context.Background()
	store, root, producer := coordinatorArtifactFixture(t)
	content := []byte("shared immutable bytes\n")
	setPublicationContent(&producer, content)
	base := CoordinatorArtifactStore{Root: root, Catalog: store}
	original, err := base.Publish(ctx, producer, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	racingCatalog := &consultationPruneRaceCatalog{
		ArtifactCatalog:      store,
		observedUnreferenced: make(chan struct{}),
		adopted:              make(chan struct{}),
	}
	racing := CoordinatorArtifactStore{Root: root, Catalog: racingCatalog}
	pruneDone := make(chan error, 1)
	go func() {
		_, _, err := racing.Prune(ctx, producer.Artifact.CreatedAt.Add(24*time.Hour), nil)
		pruneDone <- err
	}()

	<-racingCatalog.observedUnreferenced
	adopter := producer
	adopter.Artifact.ID = "artifact-adopter"
	adopter.Artifact.Name = "adopted-context"
	adopter.Artifact.CreatedAt = producer.Artifact.CreatedAt.Add(25 * time.Hour)
	publishDone := make(chan error, 1)
	go func() {
		_, err := base.Publish(ctx, adopter, bytes.NewReader(content))
		publishDone <- err
	}()
	select {
	case err := <-publishDone:
		t.Fatalf("publish crossed prune's content lifecycle: %v", err)
	case <-time.After(50 * time.Millisecond):
		// Publish is waiting on the shared filesystem lock held across the
		// unreferenced observation and unlink.
	}
	close(racingCatalog.adopted)
	if err := <-pruneDone; err != nil {
		t.Fatal(err)
	}
	if err := <-publishDone; err != nil {
		t.Fatalf("adopt same hash after prune: %v", err)
	}

	if artifacts, err := store.LoadArtifacts(ctx, []string{adopter.Artifact.ID}); err != nil || len(artifacts) != 1 {
		t.Fatalf("adopter metadata missing: %#v err=%v", artifacts, err)
	}
	objectPath := filepath.Join(root, filepath.FromSlash(original.StoragePath))
	if _, err := os.Stat(objectPath); err != nil {
		t.Fatalf("concurrently adopted blob is missing: %v", err)
	}
	if _, file, err := base.Open(ctx, adopter.Artifact.ID); err != nil {
		t.Fatalf("adopted metadata cannot open its blob: %v", err)
	} else {
		_ = file.Close()
	}
}
