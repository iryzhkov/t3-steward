package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ArtifactCatalog is the coordinator persistence seam for immutable artifact
// metadata. Implementations fence publication before making metadata visible.
type ArtifactCatalog interface {
	CommitArtifactPublication(context.Context, domain.ArtifactPublication) (domain.Artifact, error)
	LoadArtifacts(context.Context, []string) ([]domain.Artifact, error)
	PruneArtifacts(context.Context, time.Time, []string) ([]domain.Artifact, error)
	ArtifactStoragePathReferenced(context.Context, string) (bool, error)
}

// CoordinatorArtifactStore retains worker uploads independently from worker
// availability and serves them to later assignments.
type CoordinatorArtifactStore struct {
	Root    string
	Catalog ArtifactCatalog
}

// Publish streams one complete worker artifact into coordinator-owned,
// content-addressed storage, verifies it, and then commits fenced metadata.
func (s CoordinatorArtifactStore) Publish(ctx context.Context, publication domain.ArtifactPublication, content io.Reader) (domain.Artifact, error) {
	if s.Catalog == nil {
		return domain.Artifact{}, errors.New("publish artifact: catalog is required")
	}
	if content == nil {
		return domain.Artifact{}, errors.New("publish artifact: content is required")
	}
	artifact := publication.Artifact
	if err := validatePublicationArtifact(artifact); err != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: %w", err)
	}
	root, err := ensureArtifactRoot(s.Root)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: %w", err)
	}
	uploads := filepath.Join(root, ".uploads")
	if err := ensureRealDirectory(uploads, 0o700); err != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: prepare uploads: %w", err)
	}
	stage, err := os.CreateTemp(uploads, ".artifact-")
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: create staging file: %w", err)
	}
	stagePath := stage.Name()
	keepStage := false
	defer func() {
		if !keepStage {
			_ = os.Remove(stagePath)
		}
	}()

	digest := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(stage, digest), content)
	syncErr := stage.Sync()
	closeErr := stage.Close()
	if copyErr != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: receive content: %w", copyErr)
	}
	if syncErr != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: sync content: %w", syncErr)
	}
	if closeErr != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: close content: %w", closeErr)
	}
	gotHash := hex.EncodeToString(digest.Sum(nil))
	if size != artifact.Size || !strings.EqualFold(gotHash, artifact.SHA256) {
		return domain.Artifact{}, fmt.Errorf("publish artifact: content mismatch: got size %d sha256 %s, want size %d sha256 %s", size, gotHash, artifact.Size, artifact.SHA256)
	}

	hash := strings.ToLower(artifact.SHA256)
	objectDir := filepath.Join(root, "objects", hash[:2])
	if err := ensureRealDirectory(filepath.Join(root, "objects"), 0o755); err != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: prepare objects: %w", err)
	}
	if err := ensureRealDirectory(objectDir, 0o755); err != nil {
		return domain.Artifact{}, fmt.Errorf("publish artifact: prepare object prefix: %w", err)
	}
	objectPath := filepath.Join(objectDir, hash)
	createdObject := false
	if info, statErr := os.Lstat(objectPath); statErr == nil {
		if !info.Mode().IsRegular() {
			return domain.Artifact{}, errors.New("publish artifact: retained object is not a regular file")
		}
		if err := verifyArtifactFile(objectPath, artifact.Size, hash); err != nil {
			return domain.Artifact{}, fmt.Errorf("publish artifact: retained object: %w", err)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return domain.Artifact{}, fmt.Errorf("publish artifact: inspect retained object: %w", statErr)
	} else {
		if err := os.Chmod(stagePath, 0o444); err != nil {
			return domain.Artifact{}, fmt.Errorf("publish artifact: protect content: %w", err)
		}
		if err := os.Rename(stagePath, objectPath); err != nil {
			return domain.Artifact{}, fmt.Errorf("publish artifact: retain content: %w", err)
		}
		keepStage = true
		createdObject = true
	}
	artifact.SHA256 = hash
	artifact.StoragePath = filepath.ToSlash(filepath.Join("objects", hash[:2], hash))
	publication.Artifact = artifact
	committed, err := s.Catalog.CommitArtifactPublication(ctx, publication)
	if err != nil {
		if createdObject {
			referenced, referenceErr := s.Catalog.ArtifactStoragePathReferenced(ctx, artifact.StoragePath)
			if referenceErr == nil && !referenced {
				_ = os.Remove(objectPath)
			}
		}
		return domain.Artifact{}, fmt.Errorf("publish artifact metadata: %w", err)
	}
	return committed, nil
}

// FetchDependencies resolves coordinator metadata and atomically materializes
// declared predecessor outputs into a worker workspace.
func (s CoordinatorArtifactStore) FetchDependencies(
	ctx context.Context,
	request domain.ArtifactFetchRequest,
	workspaceDir string,
	task domain.Task,
	tasks []domain.Task,
) (domain.ArtifactFetchResult, []string, error) {
	if s.Catalog == nil {
		return domain.ArtifactFetchResult{}, nil, errors.New("fetch artifacts: catalog is required")
	}
	artifacts, err := s.Catalog.LoadArtifacts(ctx, request.ArtifactIDs)
	if err != nil {
		return domain.ArtifactFetchResult{}, nil, fmt.Errorf("fetch artifacts: %w", err)
	}
	for _, artifact := range artifacts {
		if artifact.WorkflowRunID != request.WorkflowRunID {
			return domain.ArtifactFetchResult{}, nil, fmt.Errorf("fetch artifacts: artifact %q belongs to run %q", artifact.ID, artifact.WorkflowRunID)
		}
	}
	paths, err := MaterializeDependencies(workspaceDir, s.Root, request.WorkflowRunID, task, tasks, artifacts)
	if err != nil {
		return domain.ArtifactFetchResult{}, nil, err
	}
	return domain.ArtifactFetchResult{Artifacts: artifacts}, paths, nil
}

// Prune applies retention to metadata first, then removes blobs no retained
// artifact references. A crash can leave an unreferenced blob, never dangling metadata.
func (s CoordinatorArtifactStore) Prune(ctx context.Context, before time.Time, protectedRunIDs []string) ([]domain.Artifact, error) {
	if s.Catalog == nil {
		return nil, errors.New("prune artifacts: catalog is required")
	}
	expired, err := s.Catalog.PruneArtifacts(ctx, before, protectedRunIDs)
	if err != nil {
		return nil, fmt.Errorf("prune artifacts: %w", err)
	}
	for _, artifact := range expired {
		referenced, err := s.Catalog.ArtifactStoragePathReferenced(ctx, artifact.StoragePath)
		if err != nil {
			return expired, fmt.Errorf("prune artifacts: check blob %q: %w", artifact.StoragePath, err)
		}
		if referenced {
			continue
		}
		path, err := safeBundleFile(s.Root, filepath.FromSlash(artifact.StoragePath))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return expired, fmt.Errorf("prune artifacts: resolve blob %q: %w", artifact.StoragePath, err)
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return expired, fmt.Errorf("prune artifacts: remove blob %q: %w", artifact.StoragePath, err)
		}
	}
	return expired, nil
}

func validatePublicationArtifact(artifact domain.Artifact) error {
	for label, value := range map[string]string{
		"artifact ID": artifact.ID, "workflow run ID": artifact.WorkflowRunID,
		"task ID": artifact.TaskID, "attempt ID": artifact.AttemptID,
		"name": artifact.Name, "media type": artifact.MediaType, "producer": artifact.Producer,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", label)
		}
	}
	if artifact.StoragePath != "" {
		return errors.New("worker must not choose coordinator storage path")
	}
	if err := validateRelativePath(artifact.Name, false); err != nil {
		return fmt.Errorf("artifact name %q: %w", artifact.Name, err)
	}
	if artifact.Size < 0 || len(artifact.SHA256) != 64 {
		return errors.New("artifact size and sha256 are invalid")
	}
	if _, err := hex.DecodeString(artifact.SHA256); err != nil {
		return errors.New("artifact sha256 is not hexadecimal")
	}
	switch artifact.Kind {
	case domain.ArtifactInput, domain.ArtifactOutput, domain.ArtifactCheckpoint,
		domain.ArtifactLog, domain.ArtifactSummary, domain.ArtifactGitState,
		domain.ArtifactVerification:
	default:
		return fmt.Errorf("artifact kind %q is invalid", artifact.Kind)
	}
	if artifact.CreatedAt.IsZero() {
		return errors.New("artifact creation time is required")
	}
	return nil
}

func ensureArtifactRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", errors.New("storage root is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if err := ensureRealDirectory(absolute, 0o700); err != nil {
		return "", err
	}
	return absolute, nil
}

func ensureRealDirectory(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(path, mode); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%q is not a real directory", path)
	}
	return nil
}

func verifyArtifactFile(path string, expectedSize int64, expectedHash string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return err
	}
	if size != expectedSize || hex.EncodeToString(digest.Sum(nil)) != expectedHash {
		return errors.New("checksum mismatch")
	}
	return nil
}
