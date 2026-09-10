package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var coordinatorArtifactTime = time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)

func TestCoordinatorArtifactPublicationReplayFencingAndPartialUpload(t *testing.T) {
	ctx := context.Background()
	store, root, publication := coordinatorArtifactFixture(t)
	service := CoordinatorArtifactStore{Root: root, Catalog: store}
	content := []byte("immutable result\n")
	setPublicationContent(&publication, content)

	artifact, err := service.Publish(ctx, publication, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	wantStoragePath := filepath.ToSlash(filepath.Join("objects", artifact.SHA256[:2], artifact.SHA256))
	if artifact.StoragePath != wantStoragePath {
		t.Fatalf("storage path = %q, want %q", artifact.StoragePath, wantStoragePath)
	}
	if got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(artifact.StoragePath))); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("retained content = %q, %v", got, err)
	}

	replayed, err := service.Publish(ctx, publication, bytes.NewReader(content))
	if err != nil || replayed != artifact {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}
	conflict := publication
	conflict.Artifact.Name = "other.txt"
	if _, err := service.Publish(ctx, conflict, bytes.NewReader(content)); !errors.Is(err, sqlite.ErrArtifactConflict) {
		t.Fatalf("conflicting replay error = %v", err)
	}

	partial := publication
	partial.Artifact.ID = "artifact-partial"
	partial.Artifact.Name = "partial.txt"
	setPublicationContent(&partial, []byte("complete"))
	if _, err := service.Publish(ctx, partial, io.MultiReader(strings.NewReader("part"), failingReader{})); err == nil || !strings.Contains(err.Error(), "receive content") {
		t.Fatalf("partial upload error = %v", err)
	}
	if _, err := store.LoadArtifacts(ctx, []string{partial.Artifact.ID}); err == nil {
		t.Fatal("partial upload published metadata")
	}

	stale := publication
	stale.Artifact.ID = "artifact-stale"
	stale.Artifact.Name = "stale.txt"
	stale.WorkerEpoch = "worker-restarted"
	setPublicationContent(&stale, []byte("stale"))
	if _, err := service.Publish(ctx, stale, strings.NewReader("stale")); !errors.Is(err, sqlite.ErrStaleArtifactPublication) {
		t.Fatalf("stale publication error = %v", err)
	}
	if _, err := store.LoadArtifacts(ctx, []string{stale.Artifact.ID}); err == nil {
		t.Fatal("stale publication committed metadata")
	}
	staleObject := filepath.Join(root, "objects", stale.Artifact.SHA256[:2], stale.Artifact.SHA256)
	if _, err := os.Stat(staleObject); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced stale object remains: %v", err)
	}
	uploads, err := os.ReadDir(filepath.Join(root, ".uploads"))
	if err != nil || len(uploads) != 0 {
		t.Fatalf("staged uploads = %#v, %v", uploads, err)
	}
}

func TestCoordinatorArtifactsTransferAcrossWorkerRestartAndVerifyChecksum(t *testing.T) {
	ctx := context.Background()
	store, root, publication := coordinatorArtifactFixture(t)
	service := CoordinatorArtifactStore{Root: root, Catalog: store}
	content := []byte("cross-worker\n")
	setPublicationContent(&publication, content)
	artifact, err := service.Publish(ctx, publication, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(filepath.Dir(root), "state.db")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	service.Catalog = store

	producer := domain.Task{ID: "task-producer", Name: "producer"}
	consumer := domain.Task{
		ID: "task-consumer", Name: "consumer", Needs: []string{"producer"},
		DependencyInputs: map[string][]string{"producer": []string{"result.txt"}},
	}
	workspace := t.TempDir()
	result, paths, err := service.FetchDependencies(ctx, domain.ArtifactFetchRequest{
		WorkflowRunID: "run-1", ArtifactIDs: []string{artifact.ID},
	}, workspace, consumer, []domain.Task{producer, consumer})
	if err != nil {
		t.Fatalf("fetch after producer is offline: %v", err)
	}
	if len(result.Artifacts) != 1 || len(paths) != 1 || paths[0] != ".t3/dependencies/producer/result.txt" {
		t.Fatalf("fetch result = %#v, paths = %#v", result, paths)
	}
	if got, err := os.ReadFile(filepath.Join(workspace, filepath.FromSlash(paths[0]))); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("dependency content = %q, %v", got, err)
	}

	if err := removeIngestedTree(filepath.Join(workspace, ".t3")); err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(root, filepath.FromSlash(artifact.StoragePath))
	if err := os.Chmod(objectPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objectPath, []byte("corrupt"), 0o444); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.FetchDependencies(ctx, domain.ArtifactFetchRequest{
		WorkflowRunID: "run-1", ArtifactIDs: []string{artifact.ID},
	}, workspace, consumer, []domain.Task{producer, consumer}); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("checksum error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".t3", "dependencies")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial dependency publish remains: %v", err)
	}
}

func TestCoordinatorArtifactRetentionAndPathSafety(t *testing.T) {
	ctx := context.Background()
	store, root, publication := coordinatorArtifactFixture(t)
	service := CoordinatorArtifactStore{Root: root, Catalog: store}

	oldContent := []byte("old")
	setPublicationContent(&publication, oldContent)
	publication.Artifact.CreatedAt = coordinatorArtifactTime.Add(-48 * time.Hour)
	oldArtifact, err := service.Publish(ctx, publication, bytes.NewReader(oldContent))
	if err != nil {
		t.Fatal(err)
	}
	fresh := publication
	fresh.Artifact.ID = "artifact-fresh"
	fresh.Artifact.Name = "fresh.txt"
	fresh.Artifact.CreatedAt = coordinatorArtifactTime
	setPublicationContent(&fresh, []byte("fresh"))
	if _, err := service.Publish(ctx, fresh, strings.NewReader("fresh")); err != nil {
		t.Fatal(err)
	}

	if expired, err := service.Prune(ctx, coordinatorArtifactTime.Add(-24*time.Hour), []string{"run-1"}); err != nil || len(expired) != 0 {
		t.Fatalf("protected prune = %#v, %v", expired, err)
	}
	expired, err := service.Prune(ctx, coordinatorArtifactTime.Add(-24*time.Hour), nil)
	if err != nil || len(expired) != 1 || expired[0].ID != oldArtifact.ID {
		t.Fatalf("prune = %#v, %v", expired, err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(oldArtifact.StoragePath))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired blob remains: %v", err)
	}
	if _, err := store.LoadArtifacts(ctx, []string{fresh.Artifact.ID}); err != nil {
		t.Fatalf("fresh metadata missing: %v", err)
	}

	unsafe := publication
	unsafe.Artifact.ID = "artifact-unsafe"
	unsafe.Artifact.Name = "../escape"
	setPublicationContent(&unsafe, []byte("unsafe"))
	if _, err := service.Publish(ctx, unsafe, strings.NewReader("unsafe")); err == nil {
		t.Fatal("unsafe artifact name accepted")
	}

	rootLink := filepath.Join(filepath.Dir(root), "artifact-root-link")
	if err := os.Symlink(root, rootLink); err != nil {
		t.Fatal(err)
	}
	service.Root = rootLink
	if _, err := service.Publish(ctx, fresh, strings.NewReader("fresh")); err == nil || !strings.Contains(err.Error(), "not a real directory") {
		t.Fatalf("symlinked storage root error = %v", err)
	}
}

func TestCoordinatorArtifactOpenVerifiesContentBeforeReturning(t *testing.T) {
	catalog, root, publication := coordinatorArtifactFixture(t)
	content := []byte("verified output\n")
	setPublicationContent(&publication, content)
	store := CoordinatorArtifactStore{Root: root, Catalog: catalog}
	if _, err := store.Publish(ctxForTest(), publication, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	artifact, reader, err := store.Open(ctxForTest(), publication.Artifact.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(got, content) || artifact.ID != publication.Artifact.ID {
		t.Fatalf("open = %q, %#v, %v", got, artifact, err)
	}
	objectPath := filepath.Join(root, filepath.FromSlash(artifact.StoragePath))
	if err := os.Chmod(objectPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objectPath, []byte("tampered output"), 0o444); err != nil {
		t.Fatal(err)
	}
	if _, reader, err := store.Open(ctxForTest(), artifact.ID); err == nil || reader != nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("tampered open = reader %#v, error %v", reader, err)
	}
}
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("connection lost") }

func coordinatorArtifactFixture(t *testing.T) (*sqlite.Store, string, domain.ArtifactPublication) {
	t.Helper()
	base := t.TempDir()
	dbPath := filepath.Join(base, "state.db")
	store, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-producer", Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 2,
		AssignmentID: "assignment-1", UpdatedAt: coordinatorArtifactTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "process-1",
		Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"},
		State: domain.AssignmentClaimed, Epoch: 3, LeaseToken: "lease",
		LeaseExpiresAt: coordinatorArtifactTime.Add(time.Hour), DispatchToken: "dispatch",
		CreatedAt: coordinatorArtifactTime, UpdatedAt: coordinatorArtifactTime,
	}
	if err := store.SaveCoordinatorRecords(ctxForTest(), sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	publication := domain.ArtifactPublication{
		CoordinatorEpoch: 1, WorkerID: "worker-a", WorkerEpoch: "process-1",
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, AttemptRevision: attempt.Revision,
		Artifact: domain.Artifact{
			ID: "artifact-output", WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
			AttemptID: attempt.ID, Kind: domain.ArtifactOutput, Name: "result.txt",
			MediaType: "text/plain", Producer: "producer", CreatedAt: coordinatorArtifactTime,
		},
	}
	return store, filepath.Join(base, "artifacts"), publication
}

func setPublicationContent(publication *domain.ArtifactPublication, content []byte) {
	sum := sha256.Sum256(content)
	publication.Artifact.Size = int64(len(content))
	publication.Artifact.SHA256 = hex.EncodeToString(sum[:])
	publication.Artifact.StoragePath = ""
}

func ctxForTest() context.Context { return context.Background() }
