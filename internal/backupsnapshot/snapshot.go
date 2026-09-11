// Package backupsnapshot creates and verifies stopped, coherent backlog-v2
// snapshots. A snapshot binds one SQLite database to one complete artifact tree.
package backupsnapshot

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	storesqlite "github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const FormatVersion = 1

var (
	ErrActiveCoordinator = errors.New("coordinator state is active")
	ErrInvalidSnapshot   = errors.New("invalid backlog-v2 snapshot")
)

type Limits struct {
	MaxFiles int
	MaxBytes int64
}

type File struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Version       int       `json:"version"`
	SchemaVersion int       `json:"schemaVersion"`
	CreatedAt     time.Time `json:"createdAt"`
	Files         []File    `json:"files"`
}

type Manager struct {
	Limits Limits
	Now    func() time.Time
}

// Create copies a stopped database and its complete artifact tree into a new,
// owner-only directory, verifies the copy, then publishes it atomically.
func (m Manager) Create(ctx context.Context, databasePath, artifactRoot, destination string) (Manifest, error) {
	if err := validateInputs(databasePath, artifactRoot, destination); err != nil {
		return Manifest{}, err
	}
	if err := validateCreateCanonicalPaths(databasePath, artifactRoot, destination); err != nil {
		return Manifest{}, err
	}
	if err := m.validateLimits(); err != nil {
		return Manifest{}, err
	}
	lock, err := acquireCoordinatorLock(databasePath)
	if err != nil {
		return Manifest{}, err
	}
	defer releaseLock(lock)
	if err := rejectWAL(databasePath); err != nil {
		return Manifest{}, err
	}
	if _, err := os.Lstat(destination); err == nil {
		return Manifest{}, fmt.Errorf("snapshot destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, fmt.Errorf("inspect snapshot destination: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return Manifest{}, fmt.Errorf("create snapshot parent: %w", err)
	}
	stage, err := os.MkdirTemp(filepath.Dir(destination), ".backlog-v2-snapshot-")
	if err != nil {
		return Manifest{}, fmt.Errorf("create snapshot staging directory: %w", err)
	}
	if err := os.Chmod(stage, 0o700); err != nil {
		_ = os.RemoveAll(stage)
		return Manifest{}, fmt.Errorf("protect snapshot staging directory: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(stage)
		}
	}()

	files := make([]File, 0)
	state, err := copyRegular(databasePath, filepath.Join(stage, "state.db"), "state.db")
	if err != nil {
		return Manifest{}, err
	}
	files = append(files, state)
	if err := walkRegular(artifactRoot, func(source, relative string) error {
		entry, err := copyRegular(source, filepath.Join(stage, "artifacts", relative), filepath.ToSlash(filepath.Join("artifacts", relative)))
		if err != nil {
			return err
		}
		files = append(files, entry)
		return m.checkBounds(files)
	}); err != nil {
		return Manifest{}, fmt.Errorf("copy artifact tree: %w", err)
	}
	if err := m.checkBounds(files); err != nil {
		return Manifest{}, err
	}
	schema, err := inspectDatabase(ctx, filepath.Join(stage, "state.db"))
	if err != nil {
		return Manifest{}, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	manifest := Manifest{Version: FormatVersion, SchemaVersion: schema, CreatedAt: m.now(), Files: files}
	if err := writeManifest(filepath.Join(stage, "manifest.json"), manifest); err != nil {
		return Manifest{}, err
	}
	if _, err := m.Verify(ctx, stage); err != nil {
		return Manifest{}, fmt.Errorf("verify staged snapshot: %w", err)
	}
	if err := protectTree(stage); err != nil {
		return Manifest{}, err
	}
	if err := os.Rename(stage, destination); err != nil {
		return Manifest{}, fmt.Errorf("publish snapshot: %w", err)
	}
	keep = true
	return manifest, nil
}

// Verify validates the manifest, exact file set, checksums, bounds, SQLite
// integrity, and schema compatibility without migration.
func (m Manager) Verify(ctx context.Context, snapshot string) (Manifest, error) {
	if err := m.validateLimits(); err != nil {
		return Manifest{}, err
	}
	info, err := os.Lstat(snapshot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return Manifest{}, fmt.Errorf("%w: snapshot root must be a real directory", ErrInvalidSnapshot)
	}
	manifest, err := readManifest(filepath.Join(snapshot, "manifest.json"))
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version != FormatVersion {
		return Manifest{}, fmt.Errorf("%w: snapshot format version %d is unsupported", ErrInvalidSnapshot, manifest.Version)
	}
	if manifest.SchemaVersion != storesqlite.CurrentSchemaVersion() {
		return Manifest{}, fmt.Errorf("%w: snapshot schema version %d does not match supported version %d", ErrInvalidSnapshot, manifest.SchemaVersion, storesqlite.CurrentSchemaVersion())
	}
	expected := map[string]File{"manifest.json": {Path: "manifest.json"}}
	for _, entry := range manifest.Files {
		if err := validateSnapshotPath(entry.Path); err != nil || entry.Path == "manifest.json" || entry.Size < 0 || len(entry.SHA256) != 64 {
			return Manifest{}, fmt.Errorf("%w: invalid manifest file %q", ErrInvalidSnapshot, entry.Path)
		}
		if _, err := hex.DecodeString(entry.SHA256); err != nil {
			return Manifest{}, fmt.Errorf("%w: invalid checksum for %q", ErrInvalidSnapshot, entry.Path)
		}
		if _, exists := expected[entry.Path]; exists {
			return Manifest{}, fmt.Errorf("%w: duplicate manifest file %q", ErrInvalidSnapshot, entry.Path)
		}
		expected[entry.Path] = entry
	}
	if _, ok := expected["state.db"]; !ok {
		return Manifest{}, fmt.Errorf("%w: state.db is missing", ErrInvalidSnapshot)
	}
	seen := make(map[string]struct{}, len(expected))
	if err := walkRegular(snapshot, func(path, relative string) error {
		relative = filepath.ToSlash(relative)
		entry, ok := expected[relative]
		if !ok {
			return fmt.Errorf("%w: unexpected file %q", ErrInvalidSnapshot, relative)
		}
		seen[relative] = struct{}{}
		if relative == "manifest.json" {
			return nil
		}
		actual, err := hashRegular(path, relative)
		if err != nil {
			return err
		}
		if actual.Size != entry.Size || actual.SHA256 != entry.SHA256 {
			return fmt.Errorf("%w: size or checksum mismatch for %q", ErrInvalidSnapshot, relative)
		}
		return nil
	}); err != nil {
		return Manifest{}, err
	}
	if len(seen) != len(expected) {
		return Manifest{}, fmt.Errorf("%w: snapshot is incomplete", ErrInvalidSnapshot)
	}
	if err := m.checkBounds(manifest.Files); err != nil {
		return Manifest{}, err
	}
	schema, err := inspectDatabase(ctx, filepath.Join(snapshot, "state.db"))
	if err != nil {
		return Manifest{}, err
	}
	if schema != manifest.SchemaVersion {
		return Manifest{}, fmt.Errorf("%w: database schema %d does not match manifest schema %d", ErrInvalidSnapshot, schema, manifest.SchemaVersion)
	}
	return manifest, nil
}

// Restore verifies a snapshot and publishes it only into absent targets. The
// target coordinator lock must be free; no migration is performed.
func (m Manager) Restore(ctx context.Context, snapshot, databasePath, artifactRoot string) (Manifest, error) {
	manifest, err := m.Verify(ctx, snapshot)
	if err != nil {
		return Manifest{}, err
	}
	if err := validateRestoreTargets(snapshot, databasePath, artifactRoot); err != nil {
		return Manifest{}, err
	}
	lock, err := acquireCoordinatorLock(databasePath)
	if err != nil {
		return Manifest{}, err
	}
	defer releaseLock(lock)
	dbStage, err := os.CreateTemp(filepath.Dir(databasePath), ".backlog-v2-state-")
	if err != nil {
		return Manifest{}, fmt.Errorf("create restore database staging file: %w", err)
	}
	dbStagePath := dbStage.Name()
	_ = dbStage.Close()
	_ = os.Remove(dbStagePath)
	artifactStage, err := os.MkdirTemp(filepath.Dir(artifactRoot), ".backlog-v2-artifacts-")
	if err != nil {
		return Manifest{}, fmt.Errorf("create restore artifact staging directory: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(dbStagePath)
			_ = os.RemoveAll(artifactStage)
		}
	}()
	if _, err := copyRegular(filepath.Join(snapshot, "state.db"), dbStagePath, "state.db"); err != nil {
		return Manifest{}, err
	}
	for _, entry := range manifest.Files {
		if !strings.HasPrefix(entry.Path, "artifacts/") {
			continue
		}
		relative := strings.TrimPrefix(entry.Path, "artifacts/")
		if _, err := copyRegular(filepath.Join(snapshot, filepath.FromSlash(entry.Path)), filepath.Join(artifactStage, filepath.FromSlash(relative)), entry.Path); err != nil {
			return Manifest{}, err
		}
	}
	if schema, err := inspectDatabase(ctx, dbStagePath); err != nil || schema != manifest.SchemaVersion {
		if err == nil {
			err = fmt.Errorf("restored schema %d does not match manifest", schema)
		}
		return Manifest{}, err
	}
	if err := protectTree(artifactStage); err != nil {
		return Manifest{}, err
	}
	if err := os.Chmod(dbStagePath, 0o600); err != nil {
		return Manifest{}, fmt.Errorf("protect restored database: %w", err)
	}
	if err := os.Rename(artifactStage, artifactRoot); err != nil {
		return Manifest{}, fmt.Errorf("publish restored artifacts: %w", err)
	}
	if err := os.Rename(dbStagePath, databasePath); err != nil {
		_ = os.RemoveAll(artifactRoot)
		return Manifest{}, fmt.Errorf("publish restored database: %w", err)
	}
	keep = true
	return manifest, nil
}

func validateInputs(databasePath, artifactRoot, destination string) error {
	for label, value := range map[string]string{"database": databasePath, "artifact root": artifactRoot, "destination": destination} {
		if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value {
			return fmt.Errorf("%s path must be absolute and clean", label)
		}
	}
	if pathsOverlap(databasePath, artifactRoot) || pathsOverlap(destination, databasePath) || pathsOverlap(destination, artifactRoot) {
		return errors.New("database, artifact root, and snapshot destination must not overlap")
	}
	return nil
}

func validateRestoreTargets(snapshot, databasePath, artifactRoot string) error {
	if err := validateInputs(databasePath, artifactRoot, snapshot); err != nil {
		return err
	}
	for _, path := range []string{databasePath, artifactRoot} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("restore target already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect restore target: %w", err)
		}
	}
	snapshotReal, err := filepath.EvalSymlinks(snapshot)
	if err != nil {
		return fmt.Errorf("resolve snapshot root: %w", err)
	}
	databaseParent, err := filepath.EvalSymlinks(filepath.Dir(databasePath))
	if err != nil {
		return fmt.Errorf("resolve database target parent: %w", err)
	}
	artifactParent, err := filepath.EvalSymlinks(filepath.Dir(artifactRoot))
	if err != nil {
		return fmt.Errorf("resolve artifact target parent: %w", err)
	}
	databaseReal := filepath.Join(databaseParent, filepath.Base(databasePath))
	artifactReal := filepath.Join(artifactParent, filepath.Base(artifactRoot))
	if pathsOverlap(snapshotReal, databaseReal) || pathsOverlap(snapshotReal, artifactReal) || pathsOverlap(databaseReal, artifactReal) {
		return errors.New("resolved snapshot and restore targets must not overlap")
	}
	return nil
}

func validateCreateCanonicalPaths(databasePath, artifactRoot, destination string) error {
	databaseReal, err := filepath.EvalSymlinks(databasePath)
	if err != nil {
		return fmt.Errorf("resolve database path: %w", err)
	}
	artifactReal, err := filepath.EvalSymlinks(artifactRoot)
	if err != nil {
		return fmt.Errorf("resolve artifact root: %w", err)
	}
	destinationParent, err := resolveAllowMissing(filepath.Dir(destination))
	if err != nil {
		return fmt.Errorf("resolve snapshot parent: %w", err)
	}
	destinationReal := filepath.Join(destinationParent, filepath.Base(destination))
	if pathsOverlap(databaseReal, artifactReal) || pathsOverlap(destinationReal, databaseReal) || pathsOverlap(destinationReal, artifactReal) {
		return errors.New("resolved database, artifact root, and snapshot destination must not overlap")
	}
	return nil
}

// resolveAllowMissing resolves every existing path component, including
// symlinks, while preserving a suffix that has not been created yet.
func resolveAllowMissing(path string) (string, error) {
	missing := make([]string, 0)
	current := path
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func (m Manager) validateLimits() error {
	if m.Limits.MaxFiles < 1 || m.Limits.MaxBytes < 1 {
		return errors.New("snapshot file and byte limits must be positive")
	}
	return nil
}

func (m Manager) checkBounds(files []File) error {
	if len(files) > m.Limits.MaxFiles {
		return fmt.Errorf("%w: snapshot exceeds file limit", ErrInvalidSnapshot)
	}
	var total int64
	for _, file := range files {
		if file.Size > m.Limits.MaxBytes-total {
			return fmt.Errorf("%w: snapshot exceeds byte limit", ErrInvalidSnapshot)
		}
		total += file.Size
	}
	return nil
}

func (m Manager) now() time.Time {
	if m.Now != nil {
		return m.Now().UTC()
	}
	return time.Now().UTC()
}

func acquireCoordinatorLock(databasePath string) (*os.File, error) {
	lock, err := os.OpenFile(databasePath+".coordinator.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open coordinator ownership lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrActiveCoordinator
		}
		return nil, fmt.Errorf("acquire coordinator ownership lock: %w", err)
	}
	return lock, nil
}

func releaseLock(lock *os.File) {
	if lock != nil {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}
}

func rejectWAL(databasePath string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		info, err := os.Lstat(databasePath + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect SQLite sidecar: %w", err)
		}
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return fmt.Errorf("%w: SQLite sidecar %s is present; stop and checkpoint the coordinator first", ErrActiveCoordinator, databasePath+suffix)
		}
	}
	return nil
}

func walkRegular(root string, visit func(path, relative string) error) error {
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s must be a real directory", ErrInvalidSnapshot, root)
	}
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return fmt.Errorf("%w: unsafe file %s", ErrInvalidSnapshot, path)
		}
		if info.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		return visit(path, relative)
	})
}

func copyRegular(source, destination, manifestPath string) (File, error) {
	lstat, err := os.Lstat(source)
	if err != nil || !lstat.Mode().IsRegular() {
		return File{}, fmt.Errorf("%w: %s is not a stable regular file", ErrInvalidSnapshot, manifestPath)
	}
	input, err := os.Open(source)
	if err != nil {
		return File{}, fmt.Errorf("open %s: %w", manifestPath, err)
	}
	defer input.Close()
	before, err := input.Stat()
	if err != nil || !before.Mode().IsRegular() || !os.SameFile(lstat, before) {
		return File{}, fmt.Errorf("%w: %s is not a regular file", ErrInvalidSnapshot, manifestPath)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return File{}, fmt.Errorf("create parent for %s: %w", manifestPath, err)
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return File{}, fmt.Errorf("create %s: %w", manifestPath, err)
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(output, hash), input)
	syncErr := output.Sync()
	closeErr := output.Close()
	after, statErr := input.Stat()
	if copyErr != nil || syncErr != nil || closeErr != nil || statErr != nil || !os.SameFile(before, after) || before.Size() != size {
		_ = os.Remove(destination)
		return File{}, fmt.Errorf("copy stable %s: %w", manifestPath, errors.Join(copyErr, syncErr, closeErr, statErr))
	}
	return File{Path: manifestPath, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func hashRegular(path, manifestPath string) (File, error) {
	input, err := os.Open(path)
	if err != nil {
		return File{}, err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return File{}, fmt.Errorf("%w: %s is not regular", ErrInvalidSnapshot, manifestPath)
	}
	hash := sha256.New()
	size, err := io.Copy(hash, input)
	if err != nil {
		return File{}, err
	}
	return File{Path: manifestPath, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func writeManifest(path string, manifest Manifest) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create snapshot manifest: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(manifest)
	err = errors.Join(err, file.Sync(), file.Close())
	if err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write snapshot manifest: %w", err)
	}
	return nil
}

func readManifest(path string) (Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: open manifest: %v", ErrInvalidSnapshot, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(file, 1<<20)))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode manifest: %v", ErrInvalidSnapshot, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Manifest{}, fmt.Errorf("%w: manifest has trailing data", ErrInvalidSnapshot)
	}
	return manifest, nil
}

func inspectDatabase(ctx context.Context, path string) (int, error) {
	store, err := storesqlite.OpenReadOnly(path)
	if err != nil {
		return 0, fmt.Errorf("%w: open snapshot database: %v", ErrInvalidSnapshot, err)
	}
	defer store.Close()
	if err := store.IntegrityCheck(ctx); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	version, err := store.SchemaVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	if version > storesqlite.CurrentSchemaVersion() {
		return 0, fmt.Errorf("%w: database schema version %d is newer than supported version %d", ErrInvalidSnapshot, version, storesqlite.CurrentSchemaVersion())
	}
	return version, nil
}

func validateSnapshotPath(path string) error {
	if path == "" || path != filepath.ToSlash(filepath.Clean(filepath.FromSlash(path))) || filepath.IsAbs(path) || path == "." || path == ".." || strings.HasPrefix(path, "../") {
		return errors.New("unsafe snapshot path")
	}
	return nil
}

func protectTree(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		mode := os.FileMode(0o400)
		if info.IsDir() {
			mode = 0o700
		}
		if err := os.Chmod(path, mode); err != nil {
			return fmt.Errorf("protect snapshot path %s: %w", path, err)
		}
		return nil
	})
}

func pathsOverlap(first, second string) bool {
	first, second = filepath.Clean(first), filepath.Clean(second)
	if first == second {
		return true
	}
	return strings.HasPrefix(first, second+string(filepath.Separator)) || strings.HasPrefix(second, first+string(filepath.Separator))
}
