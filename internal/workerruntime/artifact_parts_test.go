package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestSplitOutputCollectedImportedRetrievedAndMaterialized(t *testing.T) {
	const size = 26929544
	ctx := context.Background()
	f := newCollectionFixtureWith(t, 16<<20, 64<<20, size, 100, func(pkg *workerproto.ExecutionPackage, _ *LocalDriver, _ string) {
		pkg.Limits.MaxArtifactBytes = 16 << 20
		pkg.Limits.MaxTotalBytes = 64 << 20
	})
	if err := f.runtime.collect(ctx, "assignment-1"); err != nil {
		t.Fatal(err)
	}
	pending, err := f.custody.PendingUploads()
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending: %v %d", err, len(pending))
	}
	var output workerproto.ArtifactObject
	for _, object := range pending[0].Manifest.Objects {
		if object.Path == "results/answer.txt" {
			output = object
		}
	}
	if output.Size != size || len(output.Parts) != 2 {
		t.Fatalf("split descriptor: %+v", output)
	}
	for _, part := range output.Parts {
		if part.Size > workerproto.StreamArtifactLimit {
			t.Fatal("oversized physical part")
		}
	}
	reader, err := f.custody.OpenArtifact(ctx, output)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.VerifyArtifact(reader, output, 16<<20); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// The authenticated worker stream returns the full signed manifest while
	// transferring only the requested bounded physical part.
	service := testWorkerService(t)
	service.Exchange.Custody = f.custody
	ids := make([]string, 0, len(pending[0].Manifest.Objects))
	for _, object := range pending[0].Manifest.Objects {
		ids = append(ids, object.ID)
	}
	for index := range output.Parts {
		request := signedWorkerRequest(t, workerproto.MessageArtifactUpload, int64(index+1), workerproto.ArtifactDownloadRequest{ManifestID: pending[0].Manifest.ID, ObjectIDs: ids, Selection: &workerproto.ArtifactSelection{ObjectID: output.ID, PartIndex: index}})
		request.RequestID = fmt.Sprintf("split-fetch-%d", index)
		if err := workerproto.SignEnvelope(&request, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
			t.Fatal(err)
		}
		var encoded bytes.Buffer
		if err := service.ServeArtifactSend(ctx, bytes.NewReader(mustEncodeEnvelope(t, service.Codec, request)), &encoded); err != nil {
			t.Fatal(err)
		}
		_, raw, err := ReadStreamEnvelope(&encoded, service.Codec)
		if err != nil {
			t.Fatal(err)
		}
		part := workerproto.ArtifactPartObject(output, index)
		if err := workerproto.VerifyArtifact(raw, part, 16<<20); err != nil {
			t.Fatal(err)
		}
	}

	db, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for epoch := int64(1); epoch < 9; epoch++ {
		if _, err := db.AdvanceCoordinatorEpoch(ctx, epoch); err != nil {
			t.Fatal(err)
		}
	}
	task, attempt := packageRecords(f.pkg, runtimeTestNow)
	attempt.Progress, attempt.Control, attempt.Revision, attempt.AssignmentID = domain.ProgressVerifying, domain.ControlStopped, 3, "assignment-1"
	assignment := f.record(t).Assignment
	assignment.State = domain.AssignmentCompleted
	assignment.WorkerEpoch = "worker-1"
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", Revision: 1, Progress: domain.ProgressActive, CreatedAt: runtimeTestNow, UpdatedAt: runtimeTestNow}, []domain.Task{task})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "split-output", Class: domain.TaskClassRequired, CreatedAt: runtimeTestNow}},
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	store := backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "imported"), Catalog: db}
	t.Cleanup(func() {
		_ = filepath.WalkDir(store.Root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	importer := backlog.CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 9, Store: db, Artifacts: store, MaxArtifactBytes: 16 << 20, MaxTotalBytes: 64 << 20, Now: func() time.Time { return runtimeTestNow }}
	report, err := importer.Import(ctx, workerproto.ArtifactUploadResponse{Manifest: pending[0].Manifest, Custody: pending[0].Custody}, collectionUploadOpener{f.custody})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
		t.Fatalf("import failed: %+v", report)
	}
	retained, file, err := store.Open(ctx, output.ID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	n, err := io.Copy(digest, file)
	file.Close()
	if err != nil || n != size || hex.EncodeToString(digest.Sum(nil)) != output.SHA256 {
		t.Fatal("retained whole digest mismatch")
	}
	partsDir := filepath.Join(store.Root, filepath.FromSlash(retained.StoragePath)) + ".parts"
	if _, err := os.Stat(filepath.Join(partsDir, "manifest.json")); err != nil {
		t.Fatal(err)
	}

	// A later worker has only transferred parts, and exposes one verified cached artifact.
	consumer := testCustodyStore(t, filepath.Join(t.TempDir(), "consumer-custody"), func() time.Time { return runtimeTestNow })
	consumer.config.MaxArtifactBytes, consumer.config.MaxTotalBytes = 16<<20, 64<<20
	for index := range output.Parts {
		part := workerproto.ArtifactPartObject(output, index)
		source, err := f.custody.OpenArtifact(ctx, part)
		if err != nil {
			t.Fatal(err)
		}
		err = consumer.storeObject(source, part)
		source.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	dependency := output
	dependency.Path = "dependencies/producer/answer.txt"
	dependency.Kind = "dependency"
	driver := &LocalDriver{Config: LocalDriverConfig{ArtifactRoot: filepath.Join(t.TempDir(), "cache")}, Source: consumer}
	if _, err := driver.cacheArtifact(ctx, dependency, 16<<20); err != nil {
		t.Fatal(err)
	}
	producer := domain.Task{ID: task.ID, Name: "producer"}
	dependent := domain.Task{ID: "dependent", Name: "dependent", Needs: []string{"producer"}, DependencyInputs: map[string][]string{"producer": {"answer.txt"}}}
	artifact := retained
	artifact.StoragePath = filepath.ToSlash(filepath.Join("objects", output.SHA256))
	workspace := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(workspace, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	paths, err := backlog.MaterializeDependencies(workspace, driver.Config.ArtifactRoot, "run-1", dependent, []domain.Task{producer, dependent}, []domain.Artifact{artifact})
	if err != nil || len(paths) != 1 {
		t.Fatalf("materialization: %v %v", paths, err)
	}
	got, err := os.ReadFile(filepath.Join(workspace, ".t3", "dependencies", "producer", "answer.txt"))
	if err != nil || !bytes.Equal(got, f.raw) {
		t.Fatalf("dependent bytes: %v", err)
	}

	// Neither truncated nor corrupt parts can be used after a restart.
	part := workerproto.ArtifactPartObject(output, 1)
	partPath := consumer.objectPath(part.SHA256)
	if err := os.Chmod(partPath, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.OpenArtifact(ctx, output); err == nil {
		t.Fatal("corrupt part accepted")
	}
}

func TestSplitOutputUnderLimitUnchanged(t *testing.T) {
	f := newCollectionFixtureWith(t, 16<<20, 64<<20, 100, 100, func(pkg *workerproto.ExecutionPackage, _ *LocalDriver, _ string) {
		pkg.Limits.MaxArtifactBytes = 16 << 20
		pkg.Limits.MaxTotalBytes = 64 << 20
	})
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	pending, err := f.custody.PendingUploads()
	if err != nil || len(pending) != 1 {
		t.Fatal(err)
	}
	for _, object := range pending[0].Manifest.Objects {
		if len(object.Parts) != 0 {
			t.Fatalf("small object changed: %+v", object)
		}
		if _, err := os.Stat(f.custody.objectPath(object.SHA256)); err != nil {
			t.Fatal(err)
		}
	}
}
