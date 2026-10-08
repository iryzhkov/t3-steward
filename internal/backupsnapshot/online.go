package backupsnapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	storesqlite "github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// CreateOnline snapshots committed SQLite state while its coordinator runs.
// Only immutable objects referenced by that exact database image are copied;
// incomplete uploads, lock files and later publications do not enter the backup.
// Concurrent pruning may force a retry, but never produces a partial snapshot.
func (m Manager) CreateOnline(ctx context.Context, databasePath, artifactRoot, destination string) (Manifest, error) {
	if err := validateInputs(databasePath, artifactRoot, destination); err != nil {
		return Manifest{}, err
	}
	if err := validateCreateCanonicalPaths(databasePath, artifactRoot, destination); err != nil {
		return Manifest{}, err
	}
	if m.SubmissionRoot != "" {
		if err := validateInputs(databasePath, m.SubmissionRoot, destination); err != nil {
			return Manifest{}, err
		}
		if err := validateCreateCanonicalPaths(databasePath, m.SubmissionRoot, destination); err != nil {
			return Manifest{}, err
		}
	}
	if err := m.validateLimits(); err != nil {
		return Manifest{}, err
	}
	if _, err := os.Lstat(destination); err == nil {
		return Manifest{}, fmt.Errorf("snapshot destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return Manifest{}, err
	}
	stage, err := os.MkdirTemp(filepath.Dir(destination), ".coordinator-online-backup-")
	if err != nil {
		return Manifest{}, err
	}
	defer os.RemoveAll(stage)
	database := filepath.Join(stage, "state.db")
	if err := storesqlite.BackupOnline(ctx, databasePath, database); err != nil {
		return Manifest{}, err
	}
	schema, err := inspectDatabase(ctx, database)
	if err != nil {
		return Manifest{}, err
	}
	store, err := storesqlite.OpenReadOnly(database)
	if err != nil {
		return Manifest{}, err
	}
	artifacts, readErr := store.SnapshotArtifacts(ctx)
	closeErr := store.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return Manifest{}, err
	}
	state, err := hashRegular(database, "state.db")
	if err != nil {
		return Manifest{}, err
	}
	files := []File{state}
	if err := m.checkBounds(files); err != nil {
		return Manifest{}, err
	}
	seen := make(map[string]File)
	for _, artifact := range artifacts {
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
		if err := validateOnlineArtifact(artifact); err != nil {
			return Manifest{}, err
		}
		manifestPath := "artifacts/" + artifact.StoragePath
		expected := File{Path: manifestPath, Size: artifact.Size, SHA256: strings.ToLower(artifact.SHA256)}
		if existing, ok := seen[manifestPath]; ok {
			if existing != expected {
				return Manifest{}, fmt.Errorf("%w: conflicting artifact metadata for %s", ErrInvalidSnapshot, artifact.StoragePath)
			}
			continue
		}
		// Bound before copying so catalog sizes cannot fill the staging filesystem
		// beyond the caller's configured backup budget.
		if err := m.checkBounds(append(files, expected)); err != nil {
			return Manifest{}, err
		}
		root := artifactRoot
		if artifact.Producer == "submission" && m.SubmissionRoot != "" {
			root = m.SubmissionRoot
		}
		entry, err := copyOnlineArtifact(ctx, root, artifact, filepath.Join(stage, filepath.FromSlash(manifestPath)), manifestPath)
		if err != nil {
			return Manifest{}, fmt.Errorf("copy snapshot artifact %s: %w", artifact.ID, err)
		}
		files = append(files, entry)
		seen[manifestPath] = entry
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	manifest := Manifest{Version: FormatVersion, SchemaVersion: schema, CreatedAt: m.now(), Files: files}
	if err := writeManifest(filepath.Join(stage, "manifest.json"), manifest); err != nil {
		return Manifest{}, err
	}
	if _, err := m.Verify(ctx, stage); err != nil {
		return Manifest{}, fmt.Errorf("verify staged online backup: %w", err)
	}
	if err := protectTree(stage); err != nil {
		return Manifest{}, err
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if err := os.Rename(stage, destination); err != nil {
		return Manifest{}, fmt.Errorf("publish online backup: %w", err)
	}
	return manifest, nil
}

func validateOnlineArtifact(artifact domain.Artifact) error {
	digest, err := hex.DecodeString(artifact.SHA256)
	if err != nil || len(digest) != sha256.Size || artifact.Size < 0 {
		return fmt.Errorf("%w: artifact %s has invalid size or digest", ErrInvalidSnapshot, artifact.ID)
	}
	hash := strings.ToLower(artifact.SHA256)
	if artifact.StoragePath != filepath.ToSlash(filepath.Join("objects", hash[:2], hash)) {
		return fmt.Errorf("%w: artifact %s has invalid content address", ErrInvalidSnapshot, artifact.ID)
	}
	return nil
}

func copyOnlineArtifact(ctx context.Context, rootPath string, artifact domain.Artifact, destination, manifestPath string) (File, error) {
	// Root-relative opening prevents a concurrently replaced directory from
	// redirecting reads outside custody. Reject symlinks in existing components.
	rootInfo, err := os.Lstat(rootPath)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return File{}, fmt.Errorf("%w: artifact root must be a real directory", ErrInvalidSnapshot)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return File{}, err
	}
	defer root.Close()
	relative := ""
	for _, component := range strings.Split(artifact.StoragePath, "/") {
		relative = filepath.Join(relative, component)
		info, err := root.Lstat(relative)
		if err != nil {
			return File{}, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return File{}, fmt.Errorf("%w: symlink in artifact content address", ErrInvalidSnapshot)
		}
	}
	input, err := root.Open(filepath.FromSlash(artifact.StoragePath))
	if err != nil {
		return File{}, err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != artifact.Size {
		return File{}, fmt.Errorf("%w: retained artifact size or type mismatch", ErrInvalidSnapshot)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return File{}, err
	}
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return File{}, err
	}
	hash := sha256.New()
	// Read at most one extra byte to detect growth without exceeding the budget
	// by an unbounded amount if immutable custody has been violated.
	size, copyErr := io.Copy(io.MultiWriter(output, hash), io.LimitReader(&backupContextReader{ctx: ctx, reader: input}, artifact.Size+1))
	err = errors.Join(copyErr, output.Sync(), output.Close())
	if err != nil {
		return File{}, err
	}
	entry := File{Path: manifestPath, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}
	if entry.Size != artifact.Size || !strings.EqualFold(entry.SHA256, artifact.SHA256) {
		return File{}, fmt.Errorf("%w: retained artifact checksum or size mismatch", ErrInvalidSnapshot)
	}
	return entry, nil
}

type backupContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *backupContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// DrillReport is evidence read from a scratch restore using the backup's own
// coordinator identity. It contains no production configuration or runtime state.
type DrillReport struct {
	CoordinatorID string           `json:"coordinatorId"`
	SchemaVersion int              `json:"schemaVersion"`
	Counts        map[string]int64 `json:"counts"`
}

// RestoreDrill verifies and restores into an owner-only temporary directory,
// opens only that restored image read-only, and cleans it up before returning.
// It never acquires production coordinator ownership or runs schema migrations.
func (m Manager) RestoreDrill(ctx context.Context, snapshot string) (DrillReport, error) {
	scratch, err := os.MkdirTemp("", "t3-steward-restore-drill-")
	if err != nil {
		return DrillReport{}, err
	}
	defer os.RemoveAll(scratch)
	database := filepath.Join(scratch, "state.db")
	manifest, err := m.Restore(ctx, snapshot, database, filepath.Join(scratch, "artifacts"))
	if err != nil {
		return DrillReport{}, err
	}
	// Bind the copied bytes to the verified manifest too, so source changes
	// between verification and restoration cannot silently enter the drill.
	for _, expected := range manifest.Files {
		actual, err := hashRegular(filepath.Join(scratch, filepath.FromSlash(expected.Path)), expected.Path)
		if err != nil {
			return DrillReport{}, err
		}
		if actual != expected {
			return DrillReport{}, fmt.Errorf("%w: restored size or checksum mismatch for %s", ErrInvalidSnapshot, expected.Path)
		}
	}
	schema, err := inspectDatabase(ctx, database)
	if err != nil {
		return DrillReport{}, err
	}
	store, err := storesqlite.OpenReadOnly(database)
	if err != nil {
		return DrillReport{}, err
	}
	defer store.Close()
	identity, ok, err := store.GetKV(ctx, "backlog_v2_coordinator_identity")
	if err != nil {
		return DrillReport{}, err
	}
	if !ok || strings.TrimSpace(identity) == "" {
		return DrillReport{}, fmt.Errorf("%w: backup coordinator identity is missing", ErrInvalidSnapshot)
	}
	counts, err := store.SnapshotCounts(ctx)
	if err != nil {
		return DrillReport{}, err
	}
	return DrillReport{CoordinatorID: identity, SchemaVersion: schema, Counts: counts}, nil
}
