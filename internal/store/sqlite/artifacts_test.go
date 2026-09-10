package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestCoordinatorArtifactSnapshotMetadataIsImmutable(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fixture := coordinatorFixture()
	if err := store.SaveCoordinatorRecords(context.Background(), fixture); err != nil {
		t.Fatal(err)
	}
	fixture.Artifacts[0].Name = "changed-after-publication"
	err = store.SaveCoordinatorRecords(context.Background(), fixture)
	if err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("mutable artifact save error = %v", err)
	}
}
