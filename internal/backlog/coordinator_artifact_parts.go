package backlog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const coordinatorArtifactPartManifestVersion = 1

type coordinatorArtifactPartManifest struct {
	Version int                        `json:"version"`
	Object  workerproto.ArtifactObject `json:"object"`
}

// retainArtifactParts writes a digest-keyed sidecar for artifacts which exceed
// the worker stream limit. The ordinary whole-object file remains authoritative
// for existing readers and APIs.
func retainArtifactParts(sourcePath, objectPath string, artifact domain.Artifact) error {
	if artifact.Size <= workerproto.StreamArtifactLimit {
		return nil
	}
	object := workerproto.ArtifactObject{
		ID: "retained", Path: "content", Kind: "blob",
		MediaType: "application/octet-stream", Size: artifact.Size, SHA256: strings.ToLower(artifact.SHA256),
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open staged artifact for splitting: %w", err)
	}
	object, err = workerproto.SplitArtifact(source, object, workerproto.StreamArtifactLimit)
	closeErr := source.Close()
	if err != nil {
		return fmt.Errorf("split retained artifact: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close staged artifact after splitting: %w", closeErr)
	}

	partsDir := objectPath + ".parts"
	if _, err := os.Lstat(partsDir); err == nil {
		return verifyRetainedArtifactParts(objectPath, object)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect retained artifact parts: %w", err)
	}

	parent := filepath.Dir(objectPath)
	stageDir, err := os.MkdirTemp(parent, "."+filepath.Base(objectPath)+".parts-")
	if err != nil {
		return fmt.Errorf("create artifact parts staging directory: %w", err)
	}
	defer removeArtifactParts(stageDir)

	source, err = os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("reopen staged artifact for parts: %w", err)
	}
	for index, part := range object.Parts {
		name := fmt.Sprintf("%08d", index)
		filePath := filepath.Join(stageDir, name)
		file, createErr := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
		if createErr != nil {
			source.Close()
			return fmt.Errorf("create artifact part %d: %w", index, createErr)
		}
		written, copyErr := io.CopyN(file, source, part.Size)
		syncErr := file.Sync()
		closeErr := file.Close()
		if copyErr != nil || written != part.Size {
			source.Close()
			if copyErr == nil {
				copyErr = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("write artifact part %d: %w", index, copyErr)
		}
		if syncErr != nil {
			source.Close()
			return fmt.Errorf("sync artifact part %d: %w", index, syncErr)
		}
		if closeErr != nil {
			source.Close()
			return fmt.Errorf("close artifact part %d: %w", index, closeErr)
		}
	}
	if err := source.Close(); err != nil {
		return fmt.Errorf("close staged artifact after writing parts: %w", err)
	}

	manifest := coordinatorArtifactPartManifest{Version: coordinatorArtifactPartManifestVersion, Object: object}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encode artifact parts manifest: %w", err)
	}
	manifestPath := filepath.Join(stageDir, "manifest.json")
	file, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
	if err != nil {
		return fmt.Errorf("create artifact parts manifest: %w", err)
	}
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr = file.Close()
	if writeErr != nil {
		return fmt.Errorf("write artifact parts manifest: %w", writeErr)
	}
	if syncErr != nil {
		return fmt.Errorf("sync artifact parts manifest: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close artifact parts manifest: %w", closeErr)
	}
	if err := os.Chmod(stageDir, 0o500); err != nil {
		return fmt.Errorf("protect artifact parts directory: %w", err)
	}
	if err := os.Rename(stageDir, partsDir); err != nil {
		if errors.Is(err, os.ErrExist) {
			return verifyRetainedArtifactParts(objectPath, object)
		}
		return fmt.Errorf("retain artifact parts: %w", err)
	}
	return nil
}

// verifyRetainedArtifactParts checks the manifest and every immutable part
// against metadata recomputed from the whole object.
func verifyRetainedArtifactParts(objectPath string, expected workerproto.ArtifactObject) error {
	partsDir := objectPath + ".parts"
	info, err := os.Lstat(partsDir)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("retained artifact parts are not a real directory")
	}
	manifestPath := filepath.Join(partsDir, "manifest.json")
	manifestInfo, err := os.Lstat(manifestPath)
	if err != nil {
		return fmt.Errorf("inspect retained artifact parts manifest: %w", err)
	}
	if manifestInfo.Mode()&os.ModeSymlink != 0 || !manifestInfo.Mode().IsRegular() {
		return errors.New("retained artifact parts manifest is not a regular file")
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read retained artifact parts manifest: %w", err)
	}
	if manifestInfo.Size() < 0 || manifestInfo.Size() > 1<<20 {
		return errors.New("retained artifact parts manifest exceeds size limit")
	}
	var manifest coordinatorArtifactPartManifest
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("decode retained artifact parts manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("retained artifact parts manifest has trailing data")
	}
	if manifest.Version != coordinatorArtifactPartManifestVersion || manifest.Object.Size != expected.Size || manifest.Object.SHA256 != expected.SHA256 || manifest.Object.ID != expected.ID || manifest.Object.Path != expected.Path || manifest.Object.Kind != expected.Kind || manifest.Object.MediaType != expected.MediaType {
		return errors.New("retained artifact parts manifest version mismatch")
	}
	if len(expected.Parts) == 0 {
		whole, err := os.Open(objectPath)
		if err != nil {
			return fmt.Errorf("open whole artifact for part verification: %w", err)
		}
		expected, err = workerproto.SplitArtifact(whole, expected, workerproto.StreamArtifactLimit)
		closeErr := whole.Close()
		if err != nil {
			return fmt.Errorf("recompute artifact parts: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf("close whole artifact after part verification: %w", closeErr)
		}
	}
	if !reflect.DeepEqual(manifest.Object, expected) {
		return errors.New("retained artifact parts manifest mismatch")
	}
	for index, part := range manifest.Object.Parts {
		path := filepath.Join(partsDir, fmt.Sprintf("%08d", index))
		fileInfo, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("inspect retained artifact part %d: %w", index, err)
		}
		if fileInfo.Mode()&os.ModeSymlink != 0 || !fileInfo.Mode().IsRegular() {
			return fmt.Errorf("retained artifact part %d is not a regular file", index)
		}
		if err := verifyArtifactFile(path, part.Size, part.SHA256); err != nil {
			return fmt.Errorf("retained artifact part %d: %w", index, err)
		}
	}
	return nil
}

// removeArtifactParts removes a private parts directory, including after it
// has been made read-only for retention.
func removeArtifactParts(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 && info.IsDir() {
		if err := os.Chmod(path, 0o700); err != nil {
			return err
		}
	}
	return os.RemoveAll(path)
}
