package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCoordinatorOfferBuilderBindsDependencyPartsBeforeValidation(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	content := bytes.Repeat([]byte("dependency\n"), 300)
	sum := sha256.Sum256(content)
	artifact := &records.Artifacts[2]
	artifact.Size = int64(len(content))
	artifact.SHA256 = hex.EncodeToString(sum[:])
	root := t.TempDir()
	path := filepath.Join(root, artifact.StoragePath)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0400); err != nil {
		t.Fatal(err)
	}
	builder := packageBuilder(t, records)
	builder.MaxArtifactBytes = 1024
	builder.MaxTotalBytes = 8192
	builder.Artifacts = &CoordinatorArtifactStore{Root: root, Catalog: builder.Store.(packageRecordStore).decisions}
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	object := offer.Package.Package.Dependencies[0].Artifacts[0]
	if object.ID != artifact.ID || object.SHA256 != artifact.SHA256 || object.Size != artifact.Size || len(object.Parts) != 4 {
		t.Fatalf("logical dependency changed or parts absent: %+v", object)
	}
	if err := workerproto.VerifyArtifact(bytes.NewReader(content), object, builder.MaxArtifactBytes); err != nil {
		t.Fatal(err)
	}
	builder.Artifacts = nil
	if _, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute)); err == nil {
		t.Fatal("large dependency built without content storage")
	}
}
