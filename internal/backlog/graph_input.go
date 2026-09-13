package backlog

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PrepareGraphInput retains bounded immutable prompt bytes before the graph
// transaction exposes their metadata. A failed transaction may leave an
// unreferenced content-addressed blob; it never leaves a dangling graph input.
func PrepareGraphInput(root, id, runID, taskID, prompt string, now time.Time) (domain.Artifact, error) {
	var a domain.Artifact
	if len(prompt) == 0 || len(prompt) > 256<<10 {
		return a, errors.New("graph prompt must be 1..256 KiB")
	}
	root, err := ensureArtifactRoot(root)
	if err != nil {
		return a, err
	}
	sum := sha256.Sum256([]byte(prompt))
	hash := hex.EncodeToString(sum[:])
	for _, dir := range []string{filepath.Join(root, "objects"), filepath.Join(root, "objects", hash[:2])} {
		if err = ensureRealDirectory(dir, 0700); err != nil {
			return a, err
		}
	}
	relative := filepath.Join("objects", hash[:2], hash)
	path := filepath.Join(root, relative)
	file, err := os.CreateTemp(filepath.Dir(path), ".graph-")
	if err != nil {
		return a, err
	}
	defer os.Remove(file.Name())
	if _, err = file.WriteString(prompt); err != nil {
		file.Close()
		return a, err
	}
	if err = file.Chmod(0400); err != nil {
		file.Close()
		return a, err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return a, err
	}
	if err = file.Close(); err != nil {
		return a, err
	}
	if err = os.Link(file.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return a, err
	}
	if err = verifyArtifactFile(path, int64(len(prompt)), hash); err != nil {
		return a, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return a, err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return a, err
	}
	return domain.Artifact{ID: id, WorkflowRunID: runID, TaskID: taskID, Kind: domain.ArtifactInput, Name: "prompt.md", MediaType: "text/markdown", Size: int64(len(prompt)), SHA256: hash, StoragePath: filepath.ToSlash(relative), Producer: "graph-amendment", CreatedAt: now.UTC()}, nil
}
