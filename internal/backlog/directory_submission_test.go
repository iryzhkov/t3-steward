package backlog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestDirectorySubmissionResolvesOperatorEvidenceAndReplays(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	root := filepath.Join(t.TempDir(), "bundles")
	t.Cleanup(func() { _ = removeIngestedTree(root) })
	allowed := directoryTestBinding(directoryresource.ReadWrite)
	reg := allowed.Identity.Registration
	manifest := "version: 2\nname: directories\nenvironment: {project: scratch, type: fresh}\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n    directories:\n      - {worker: " + reg.WorkerID + ", resource: " + reg.ResourceID + ", revision: '" + reg.Revision + "'}\n"
	archive := submissionTar(t, map[string]string{"workflow.yaml": manifest, "prompts/inspect.md": "inspect"})
	service := &SubmissionService{StorageRoot: root, Store: store, MaxBytes: 1 << 20, MaxFiles: 16, DirectoryCatalogs: map[string][]directoryresource.Binding{"scratch": {allowed}}}
	request := func() ArchiveSubmission {
		return ArchiveSubmission{IdempotencyKey: "directories", Archive: bytes.NewReader(archive)}
	}
	result, err := service.SubmitArchive(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil || len(records.Tasks) != 1 {
		t.Fatalf("records: %+v %v", records, err)
	}
	binding := records.Tasks[0].DirectoryBindings[0]
	if binding.Access != directoryresource.ReadOnly || binding.Identity.Registration != reg {
		t.Fatalf("resolved binding: %+v", binding)
	}
	// Idempotent replay uses durable accepted evidence, even after approval is removed.
	service.DirectoryCatalogs = nil
	replay, err := service.SubmitArchive(ctx, request())
	if err != nil || !replay.Replay || replay.Record.RunID != result.Record.RunID {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.OpenMigrated(db)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	records, err = reopened.LoadCoordinatorRecords(ctx)
	if err != nil || records.Tasks[0].DirectoryBindings[0].Identity.Object != allowed.Identity.Object {
		t.Fatalf("lost identity after reopen: %v", err)
	}
}

func TestDirectorySubmissionRejectsUnapprovedBeforeStorage(t *testing.T) {
	allowed := directoryTestBinding(directoryresource.ReadOnly)
	reg := allowed.Identity.Registration
	for _, tc := range []struct {
		name, worker, resource, revision, access string
		catalog                                  bool
	}{
		{"missing catalog", reg.WorkerID, reg.ResourceID, reg.Revision, "", false},
		{"wrong revision", reg.WorkerID, reg.ResourceID, "changed", "", true},
		{"wrong worker", "other", reg.ResourceID, reg.Revision, "", true},
		{"write escalation", reg.WorkerID, reg.ResourceID, reg.Revision, "read-write", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := validBundle(t)
			rewriteBundleManifest(t, bundle, "version: 2\nname: directories\nenvironment: {project: scratch, type: fresh}\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n    directories:\n      - {worker: "+tc.worker+", resource: "+tc.resource+", revision: '"+tc.revision+"', access: '"+tc.access+"'}\n")
			root := filepath.Join(t.TempDir(), "uncreated")
			store := &ingestionStore{}
			ingester := BundleIngester{StorageRoot: root, Store: store}
			if tc.catalog {
				ingester.DirectoryCatalogs = map[string][]directoryresource.Binding{"scratch": {allowed}}
			}
			if _, err := ingester.Ingest(context.Background(), bundle); err == nil {
				t.Fatal("unapproved resource accepted")
			}
			if store.calls != 0 {
				t.Fatal("metadata persisted for rejected request")
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("storage touched: %v", err)
			}
		})
	}
}

func TestDirectoryManifestRejectsPathsAndIncompatiblePlacement(t *testing.T) {
	base := "version: 2\nname: directories\nenvironment: {project: scratch, type: fresh}\ntasks:\n  inspect:\n    prompt_file: prompts/inspect.md\n    directories: [{worker: worker-a, resource: data, revision: '1'}]\n"
	for _, raw := range []string{
		strings.Replace(base, "resource: data", "path: /srv/data", 1),
		strings.Replace(base, "worker: worker-a", "worker: worker-a, identity: {}", 1),
		strings.Replace(base, "    directories:", "    placement: {hosts: [worker-b]}\n    directories:", 1),
		strings.Replace(base, "type: fresh", "type: git", 1),
		strings.Replace(base, "revision: '1'", "revision: ''", 1),
	} {
		if _, err := ParseManifest([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid manifest: %s", raw)
		}
	}
}
