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

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type activationEvidenceCatalog interface {
	EnsureActivationEvidence(context.Context, sqlite.ActivationEvidencePublication) (domain.Artifact, bool, error)
	LoadActivationEvidence(context.Context, string, string) (domain.Artifact, bool, error)
}

// OpenActivationEvidenceSnapshot retrieves and verifies the first frozen
// snapshot for an activation.
func (s CoordinatorArtifactStore) OpenActivationEvidenceSnapshot(ctx context.Context, runID, activationID string) (domain.Artifact, *os.File, bool, error) {
	catalog, ok := s.Catalog.(activationEvidenceCatalog)
	if !ok {
		return domain.Artifact{}, nil, false, errors.New("activation evidence: catalog does not support coordinator-owned snapshots")
	}
	artifact, found, err := catalog.LoadActivationEvidence(ctx, runID, activationID)
	if err != nil || !found {
		return domain.Artifact{}, nil, found, err
	}
	verified, file, err := s.Open(ctx, artifact.ID)
	return verified, file, true, err
}

// EnsureActivationEvidenceSnapshot retains canonical snapshot bytes through the
// existing content-addressed artifact store, then freezes their metadata.
func (s CoordinatorArtifactStore) EnsureActivationEvidenceSnapshot(
	ctx context.Context,
	request sqlite.ActivationEvidencePublication,
	content io.Reader,
) (domain.Artifact, error) {
	catalog, ok := s.Catalog.(activationEvidenceCatalog)
	if !ok {
		return domain.Artifact{}, errors.New("activation evidence: catalog does not support coordinator-owned snapshots")
	}
	if content == nil {
		return domain.Artifact{}, errors.New("activation evidence: content is required")
	}
	artifact := request.Artifact
	if err := validatePublicationArtifact(artifact); err != nil {
		return domain.Artifact{}, fmt.Errorf("activation evidence: %w", err)
	}
	root, err := ensureArtifactRoot(s.Root)
	if err != nil {
		return domain.Artifact{}, err
	}
	uploads := filepath.Join(root, ".uploads")
	if err := ensureRealDirectory(uploads, 0o700); err != nil {
		return domain.Artifact{}, err
	}
	stage, err := os.CreateTemp(uploads, ".activation-evidence-")
	if err != nil {
		return domain.Artifact{}, err
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
	if copyErr != nil || syncErr != nil || closeErr != nil {
		return domain.Artifact{}, fmt.Errorf("activation evidence: retain content: %v %v %v", copyErr, syncErr, closeErr)
	}
	gotHash := hex.EncodeToString(digest.Sum(nil))
	if size != artifact.Size || !strings.EqualFold(gotHash, artifact.SHA256) {
		return domain.Artifact{}, fmt.Errorf("activation evidence: content mismatch")
	}
	hash := strings.ToLower(artifact.SHA256)
	if len(hash) < 2 {
		return domain.Artifact{}, errors.New("activation evidence: invalid digest")
	}
	objects := filepath.Join(root, "objects")
	objectDir := filepath.Join(objects, hash[:2])
	if err := ensureRealDirectory(objects, 0o700); err != nil {
		return domain.Artifact{}, err
	}
	if err := ensureRealDirectory(objectDir, 0o700); err != nil {
		return domain.Artifact{}, err
	}
	objectPath := filepath.Join(objectDir, hash)
	artifact.SHA256 = hash
	artifact.StoragePath = filepath.ToSlash(filepath.Join("objects", hash[:2], hash))
	request.Artifact = artifact
	lock, err := acquireFileLock(ctx, root, "artifact-object:"+artifact.StoragePath)
	if err != nil {
		return domain.Artifact{}, err
	}
	defer lock.Close()
	if info, statErr := os.Lstat(objectPath); statErr == nil {
		if !info.Mode().IsRegular() {
			return domain.Artifact{}, errors.New("activation evidence: retained object is not a regular file")
		}
		if err := verifyArtifactFile(objectPath, artifact.Size, hash); err != nil {
			return domain.Artifact{}, err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return domain.Artifact{}, statErr
	} else {
		if err := os.Chmod(stagePath, 0o400); err != nil {
			return domain.Artifact{}, err
		}
		if err := os.Rename(stagePath, objectPath); err != nil {
			return domain.Artifact{}, err
		}
		keepStage = true
	}
	committed, _, err := catalog.EnsureActivationEvidence(ctx, request)
	if err != nil {
		return domain.Artifact{}, err
	}
	return committed, nil
}
