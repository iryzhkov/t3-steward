package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Start with the coordinator's durable records, not an invented wire object.
// Both ordinary empty files must survive offer serialization, verified custody
// (including restart), and real worker workspace preparation.
func TestZeroByteArtifactPackageDeliveryAndPreparation(t *testing.T) {
	ctx := context.Background()
	repository, commit := makeGitRepository(t)
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: "ssh://git/steward", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: 5 * time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	records, assignment := zeroByteBuilderFixture(runtimeTestNow)
	records.Artifacts[0].Name = "tasks/consumer.md"
	data := map[string][]byte{"prompt-1": []byte("prompt"), "input-1": {}, "output-1": {}}
	for index := range records.Artifacts {
		a := &records.Artifacts[index]
		object := testArtifact(a.ID, "inputs/unused", string(data[a.ID]))
		a.Size, a.SHA256 = object.Size, object.SHA256
	}
	db, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "records.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	builder := backlog.CoordinatorOfferBuilder{
		Store: db, Catalog: catalog, CatalogRevision: "catalog-1",
		CoordinatorID: "coordinator", CoordinatorEpoch: 1,
		VerificationTimeout: 2 * time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20,
	}
	offer, err := builder.BuildAssignmentOffer(ctx, assignment, runtimeTestNow.Add(time.Minute))
	if err != nil {
		t.Fatalf("construct offer: %v", err)
	}
	encoded, err := json.Marshal(offer)
	if err != nil {
		t.Fatal(err)
	}
	var decoded workerproto.AssignmentOffer
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := workerproto.ValidateExecutionPackageManifest(decoded.Package, 1<<20); err != nil {
		t.Fatal(err)
	}
	pkg := decoded.Package.Package
	objects := append([]workerproto.ArtifactObject{pkg.Prompt}, pkg.StaticInputs...)
	objects = append(objects, pkg.Dependencies[0].Artifacts...)
	manifest := zeroByteDownloadManifest(objects)
	manifest.AssignmentEpoch = assignment.Epoch
	manifest.CoordinatorEpoch = pkg.CoordinatorEpoch
	readers := map[string]io.Reader{}
	for _, object := range objects {
		readers[object.ID] = bytes.NewReader(data[object.ID])
	}
	custodyRoot := t.TempDir()
	custody := testCustodyStore(t, custodyRoot, func() time.Time { return runtimeTestNow })
	custody.config.CoordinatorEpoch = pkg.CoordinatorEpoch
	receipts, err := custody.ReceiveDownload(ctx, manifest, readers)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != len(objects) {
		t.Fatalf("receipts = %d", len(receipts))
	}
	for index, receipt := range receipts {
		if receipt.ObjectID != objects[index].ID || receipt.Size != objects[index].Size || receipt.SHA256 != objects[index].SHA256 || receipt.RecordSHA256 == "" {
			t.Fatalf("custody changed object metadata: %+v", receipt)
		}
	}
	restarted := testCustodyStore(t, custodyRoot, func() time.Time { return runtimeTestNow })
	restarted.config.CoordinatorEpoch = pkg.CoordinatorEpoch
	replay, err := restarted.ReceiveDownload(ctx, manifest, nil)
	if err != nil || len(replay) != len(receipts) {
		t.Fatalf("restart replay = %+v, %v", replay, err)
	}
	for index := range replay {
		if replay[index].RecordSHA256 != receipts[index].RecordSHA256 {
			t.Fatal("restart changed custody")
		}
	}
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: t.TempDir(), RunsRoot: t.TempDir()},
		Catalog: catalog, Workspace: backlog.WorkspacePreparer{Cache: staticRepositoryCache{path: repository}, Processes: successfulProcessRunner{}},
		Source: restarted, Publisher: &recordingPublisher{}, T3: &recordingT3{}, Now: func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(ctx, pkg)
	if err != nil {
		t.Fatalf("prepare delivered empty files: %v", err)
	}
	t.Cleanup(func() {
		if err := driver.Cleanup(ctx, pkg, workspace); err != nil {
			t.Errorf("cleanup prepared fixture: %v", err)
		}
	})
	for _, object := range []workerproto.ArtifactObject{pkg.StaticInputs[0], pkg.Dependencies[0].Artifacts[0]} {
		path := filepath.Join(workspace, ".t3", filepath.FromSlash(object.Path))
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Size() != 0 {
			t.Fatalf("materialized %s: %+v", object.Path, info)
		}
		got, err := os.ReadFile(path)
		if err != nil || len(got) != 0 {
			t.Fatalf("materialized %s = %q, %v", object.Path, got, err)
		}
	}
}

func zeroByteDownloadManifest(objects []workerproto.ArtifactObject) workerproto.ArtifactTransferManifest {
	var total int64
	for _, object := range objects {
		total += object.Size
	}
	return workerproto.ArtifactTransferManifest{
		Version: workerproto.ArtifactManifestVersion, ID: "download-1", Direction: "download",
		CoordinatorEpoch: 9, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: "assignment-1", AssignmentEpoch: 2,
		Objects: objects, TotalBytes: total, CreatedAt: runtimeTestNow.Add(-time.Minute), ExpiresAt: runtimeTestNow.Add(time.Hour),
	}
}

func TestZeroByteArtifactDeliveryRefusesInvalidObjects(t *testing.T) {
	cases := map[string]func(*workerproto.ArtifactObject){
		"missing-id":         func(o *workerproto.ArtifactObject) { o.ID = "" },
		"missing-path":       func(o *workerproto.ArtifactObject) { o.Path = "" },
		"missing-kind":       func(o *workerproto.ArtifactObject) { o.Kind = "" },
		"missing-media-type": func(o *workerproto.ArtifactObject) { o.MediaType = "" },
		"missing-sha":        func(o *workerproto.ArtifactObject) { o.SHA256 = "" },
		"malformed-sha":      func(o *workerproto.ArtifactObject) { o.SHA256 = "bad" },
		"wrong-checksum":     func(o *workerproto.ArtifactObject) { o.SHA256 = strings.Repeat("a", 64) },
		"negative-size":      func(o *workerproto.ArtifactObject) { o.Size = -1 },
		"excessive-size":     func(o *workerproto.ArtifactObject) { o.Size = 3 << 20 },
		"traversal-path":     func(o *workerproto.ArtifactObject) { o.Path = "../escape" },
		"absolute-path":      func(o *workerproto.ArtifactObject) { o.Path = "/escape" },
		"unclean-path":       func(o *workerproto.ArtifactObject) { o.Path = "inputs/../escape" },
		"backslash-path":     func(o *workerproto.ArtifactObject) { o.Path = "inputs\\escape" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			object := testArtifact("empty-1", "inputs/empty.diff", "")
			mutate(&object)
			store := testCustodyStore(t, t.TempDir(), func() time.Time { return runtimeTestNow })
			receipts, err := store.ReceiveDownload(context.Background(), zeroByteDownloadManifest([]workerproto.ArtifactObject{object}), map[string]io.Reader{object.ID: bytes.NewReader(nil)})
			if err == nil || len(receipts) != 0 {
				t.Fatalf("invalid delivery receipted: %+v, %v", receipts, err)
			}
			if _, err := os.Stat(filepath.Join(store.config.Root, "receipts", "download-1.json")); !os.IsNotExist(err) {
				t.Fatalf("invalid delivery published receipt: %v", err)
			}
		})
	}
}

func zeroByteBuilderFixture(now time.Time) (sqlite.CoordinatorRecords, domain.Assignment) {
	workflow := domain.Workflow{
		ID: "workflow-1", Version: 2, Name: "workflow", Project: "steward",
		Environment: domain.ExecutionEnvironment{Type: backlog.EnvironmentGit, Scope: backlog.EnvironmentScopeTask},
		Class:       domain.TaskClassRequired, TaskIDs: []string{"task-producer", "task-consumer"}, CreatedAt: now,
	}
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: workflow.ID, Progress: domain.ProgressActive,
		Revision: 2, CreatedAt: now, UpdatedAt: now,
	}
	producer := domain.Task{ID: "task-producer", WorkflowID: workflow.ID, Name: "producer", Class: domain.TaskClassRequired}
	consumer := domain.Task{
		ID: "task-consumer", WorkflowID: workflow.ID, Name: "consumer", Class: domain.TaskClassRequired,
		Needs: []string{"producer"}, PromptArtifactID: "prompt-1", InputArtifactIDs: []string{"input-1"},
		DependencyInputs: map[string][]string{"producer": {"reports/result.txt"}},
		Outputs:          []domain.ArtifactDeclaration{{Name: "out.txt", MediaType: "text/plain"}},
		Verification:     []string{"go test ./..."}, MaxTurns: 4,
	}
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: run.ID, TaskID: consumer.ID, Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
		Revision: 2, AssignmentID: "assignment-1", UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1",
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol",
			QuotaPoolID: "openai", Options: map[string]string{"effort": "high"},
		},
		State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-1",
		LeaseExpiresAt: now.Add(time.Hour), DispatchToken: "dispatch-1", ThreadID: "thread-1",
		CreatedAt: now, UpdatedAt: now,
	}
	artifact := func(id, taskID string, kind domain.ArtifactKind, name string) domain.Artifact {
		return domain.Artifact{
			ID: id, WorkflowRunID: run.ID, TaskID: taskID, Kind: kind, Name: name,
			MediaType: "text/plain", Size: 10, SHA256: strings.Repeat("a", 64),
			StoragePath: "objects/" + id, Producer: "coordinator", CreatedAt: now,
		}
	}
	return sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run},
		Tasks: []domain.Task{producer, consumer}, Attempts: []domain.Attempt{attempt},
		Assignments: []domain.Assignment{assignment},
		Artifacts: []domain.Artifact{
			artifact("prompt-1", consumer.ID, domain.ArtifactInput, "tasks/consumer.md"),
			artifact("input-1", "", domain.ArtifactInput, "context.txt"),
			artifact("output-1", producer.ID, domain.ArtifactOutput, "reports/result.txt"),
		},
	}, assignment
}
