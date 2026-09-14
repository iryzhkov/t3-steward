package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// refusingValidator stands in for the coordinator's readiness check.
type refusingValidator struct {
	calls     int
	manifests []string
	err       error
}

func (v *refusingValidator) ValidatePermanent(_ context.Context, manifest Manifest) error {
	v.calls++
	v.manifests = append(v.manifests, manifest.Name)
	return v.err
}

// TestIngestRefusesAPermanentlyImpossibleBundle is the zero-workflow guarantee
// at the coordinator. A client that skipped its own readiness check, or a fleet
// that changed after the client checked, must still not be able to create a
// structurally impossible run.
func TestIngestRefusesAPermanentlyImpossibleBundle(t *testing.T) {
	bundle := validBundle(t)
	store := &ingestionStore{}
	storage := filepath.Join(t.TempDir(), "artifacts")
	validator := &refusingValidator{err: errors.New("this campaign can never run as written: unknown-project")}
	_, err := (BundleIngester{
		StorageRoot: storage, Store: store, Permanent: validator,
	}).Ingest(context.Background(), bundle)
	if err == nil || !strings.Contains(err.Error(), "can never run as written") {
		t.Fatalf("error = %v", err)
	}
	if validator.calls != 1 {
		t.Fatalf("permanent validation ran %d times, want 1", validator.calls)
	}
	if store.calls != 0 {
		t.Fatal("a refused bundle still persisted records")
	}
	if len(store.records.Workflows) != 0 || len(store.records.WorkflowRuns) != 0 {
		t.Fatalf("a refused bundle created records: %#v", store.records)
	}
	// Nothing was staged either: the refusal happens before any file is copied,
	// so there is no partial tree to clean up afterwards.
	if entries, statErr := os.ReadDir(filepath.Join(storage, "workflows")); statErr == nil && len(entries) != 0 {
		t.Fatalf("a refused bundle left %d staged entries", len(entries))
	}
}

// TestIngestAcceptsWhatTheValidatorAllows states that the gate only refuses; it
// never changes what an acceptable bundle becomes.
func TestIngestAcceptsWhatTheValidatorAllows(t *testing.T) {
	bundle := validBundle(t)
	store := &ingestionStore{}
	validator := &refusingValidator{}
	ingester := BundleIngester{
		StorageRoot: filepath.Join(t.TempDir(), "artifacts"), Store: store, Permanent: validator,
	}
	t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
	ingested, err := ingester.Ingest(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if validator.calls != 1 || len(validator.manifests) != 1 || validator.manifests[0] == "" {
		t.Fatalf("the validator did not see the manifest: %+v", validator.manifests)
	}
	if ingested.WorkflowID == "" || store.calls != 1 {
		t.Fatalf("an accepted bundle was not ingested: %+v", ingested)
	}
}
