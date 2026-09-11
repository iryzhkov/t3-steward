package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const custodyReceiptVersion = 1

// CustodyConfig binds a worker-local content store to one coordinator and worker epoch.
type CustodyConfig struct {
	Root             string
	CoordinatorID    string
	CoordinatorEpoch int64
	WorkerID         string
	WorkerEpoch      string
	MaxArtifactBytes int64
	MaxTotalBytes    int64
	Now              func() time.Time
}

// PendingUpload is an immutable worker-to-coordinator transfer ready for transport.
type PendingUpload struct {
	Version  int                                  `json:"version"`
	Manifest workerproto.ArtifactTransferManifest `json:"manifest"`
	Custody  []workerproto.ArtifactCustodyRecord  `json:"custody"`
}

// CustodyStore is a content-addressed, restart-safe artifact source and publisher.
type CustodyStore struct {
	config CustodyConfig
}

// OpenCustodyStore opens or creates worker-local custody without following a root symlink.
func OpenCustodyStore(config CustodyConfig) (*CustodyStore, error) {
	root, err := safeLocalRoot(config.Root)
	if err != nil {
		return nil, fmt.Errorf("open custody: %w", err)
	}
	if config.CoordinatorID == "" || config.CoordinatorEpoch < 1 || config.WorkerID == "" ||
		config.WorkerEpoch == "" || config.MaxArtifactBytes < 1 || config.MaxTotalBytes < config.MaxArtifactBytes {
		return nil, errors.New("open custody: invalid identity or limits")
	}
	config.Root = root
	if config.Now == nil {
		config.Now = time.Now
	}
	for _, name := range []string{"objects", "receipts", "outbox", "acknowledged"} {
		if err := ensureRealDirectory(filepath.Join(root, name)); err != nil {
			return nil, fmt.Errorf("open custody: %s: %w", name, err)
		}
	}
	return &CustodyStore{config: config}, nil
}

// ReceiveDownload verifies a complete coordinator manifest before publishing any local receipt.
func (s *CustodyStore) ReceiveDownload(ctx context.Context, manifest workerproto.ArtifactTransferManifest, readers map[string]io.Reader) ([]workerproto.ArtifactCustodyRecord, error) {
	if err := s.validateManifest(manifest, "download"); err != nil {
		return nil, err
	}
	receiptPath := filepath.Join(s.config.Root, "receipts", manifest.ID+".json")
	var priorRecords []workerproto.ArtifactCustodyRecord
	if prior, err := s.loadPending(receiptPath); err == nil {
		if !reflect.DeepEqual(prior.Manifest, manifest) {
			return nil, errors.New("receive artifact: manifest id replay changed immutable content")
		}
		priorRecords = append([]workerproto.ArtifactCustodyRecord(nil), prior.Custody...)
		if len(readers) == 0 {
			return priorRecords, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(readers) != len(manifest.Objects) {
		return nil, errors.New("receive artifact: object set does not match manifest")
	}
	records := make([]workerproto.ArtifactCustodyRecord, 0, len(manifest.Objects))
	var previous string
	for index, object := range manifest.Objects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reader, ok := readers[object.ID]
		if !ok || reader == nil {
			return nil, fmt.Errorf("receive artifact: missing object %q", object.ID)
		}
		if err := s.storeObject(reader, object); err != nil {
			return nil, fmt.Errorf("receive artifact %q: %w", object.ID, err)
		}
		record, err := workerproto.BuildCustodyRecord(workerproto.ArtifactCustodyRecord{
			ManifestID:     manifest.ID,
			ObjectID:       object.ID,
			From:           "coordinator:" + s.config.CoordinatorID,
			To:             "worker:" + s.config.WorkerID,
			Sequence:       int64(index + 1),
			Size:           object.Size,
			SHA256:         object.SHA256,
			VerifiedAt:     s.now(),
			PreviousSHA256: previous,
		})
		if err != nil {
			return nil, err
		}
		previous = record.RecordSHA256
		records = append(records, record)
	}
	if priorRecords != nil {
		return priorRecords, nil
	}
	pending := PendingUpload{Version: custodyReceiptVersion, Manifest: manifest, Custody: records}
	if err := writeJSONExclusive(receiptPath, pending); err != nil {
		if errors.Is(err, os.ErrExist) {
			prior, loadErr := s.loadPending(receiptPath)
			if loadErr == nil && reflect.DeepEqual(prior.Manifest, manifest) {
				return append([]workerproto.ArtifactCustodyRecord(nil), prior.Custody...), nil
			}
		}
		return nil, fmt.Errorf("receive artifact: commit receipt: %w", err)
	}
	return records, nil
}

// OpenArtifact opens a verified content-addressed object for workspace materialization.
func (s *CustodyStore) OpenArtifact(_ context.Context, object workerproto.ArtifactObject) (io.ReadCloser, error) {
	if err := workerproto.ValidateArtifactObject(object, s.config.MaxArtifactBytes); err != nil {
		return nil, err
	}
	file, err := openRegular(s.objectPath(object.SHA256))
	if err != nil {
		return nil, fmt.Errorf("open custody object: %w", err)
	}
	if err := workerproto.VerifyArtifact(file, object, s.config.MaxArtifactBytes); err != nil {
		file.Close()
		return nil, fmt.Errorf("open custody object: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

// PublishResult retains finalizer output, final message, and thread archive as one upload.
func (s *CustodyStore) PublishResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult) error {
	objects := make([]workerproto.ArtifactObject, 0, len(result.Finalized.Artifacts)+2)
	for _, artifact := range result.Finalized.Artifacts {
		source, err := finalizedArtifactPath(result.Finalized, artifact)
		if err != nil {
			return err
		}
		file, err := openRegular(source)
		if err != nil {
			return fmt.Errorf("publish result: open %q: %w", artifact.Name, err)
		}
		object := workerproto.ArtifactObject{
			ID:        artifact.ID,
			Path:      "results/" + filepath.ToSlash(artifact.Name),
			Kind:      string(artifact.Kind),
			MediaType: artifact.MediaType,
			Size:      artifact.Size,
			SHA256:    artifact.SHA256,
		}
		storeErr := s.storeObject(file, object)
		closeErr := file.Close()
		if storeErr != nil {
			return fmt.Errorf("publish result %q: %w", artifact.Name, storeErr)
		}
		if closeErr != nil {
			return closeErr
		}
		objects = append(objects, object)
	}
	for _, extra := range []struct {
		id, path, kind, media string
		data                  []byte
	}{
		{"final-message-" + pkg.Identity.AttemptID, "results/final-message.md", "summary", "text/markdown", []byte(result.FinalMessage)},
		{"thread-archive-" + pkg.Identity.AttemptID, "results/thread.json", "log", "application/json", result.ThreadArchive},
	} {
		object := objectForBytes(extra.id, extra.path, extra.kind, extra.media, extra.data)
		if err := s.storeObject(bytes.NewReader(extra.data), object); err != nil {
			return fmt.Errorf("publish result %q: %w", extra.path, err)
		}
		objects = append(objects, object)
	}
	return s.publishManifest(ctx, pkg, "result", objects)
}

// PublishCheckpoint retains checkpoint bytes and advertises an immutable upload.
func (s *CustodyStore) PublishCheckpoint(ctx context.Context, pkg workerproto.ExecutionPackage, path string, data []byte) (*domain.CheckpointMetadata, error) {
	id := "checkpoint-" + pkg.Identity.AttemptID + "-" + shortDigest(data)
	object := objectForBytes(id, "checkpoints/"+id+".md", "checkpoint", "text/markdown", data)
	if err := s.storeObject(bytes.NewReader(data), object); err != nil {
		return nil, fmt.Errorf("publish checkpoint: %w", err)
	}
	if err := s.publishManifest(ctx, pkg, "checkpoint-"+shortDigest(data), []workerproto.ArtifactObject{object}); err != nil {
		return nil, err
	}
	return &domain.CheckpointMetadata{
		ArtifactID: object.ID,
		Path:       path,
		SHA256:     object.SHA256,
		Size:       object.Size,
		CapturedAt: s.now(),
	}, nil
}

// BuildUpload opens one complete immutable outbox manifest for authenticated transfer.

// PendingUploadFor authorizes a complete ordered transfer from the immutable outbox.
func (s *CustodyStore) PendingUploadFor(request workerproto.ArtifactDownloadRequest) (PendingUpload, error) {
	pending, err := s.PendingUploads()
	if err != nil {
		return PendingUpload{}, err
	}
	for _, upload := range pending {
		if upload.Manifest.ID != request.ManifestID {
			continue
		}
		want := make([]string, 0, len(upload.Manifest.Objects))
		for _, object := range upload.Manifest.Objects {
			want = append(want, object.ID)
		}
		if !slices.Equal(want, request.ObjectIDs) {
			return PendingUpload{}, errors.New("pending upload: request must name every object in manifest order")
		}
		return upload, nil
	}
	return PendingUpload{}, errors.New("pending upload: manifest not found")
}

// PendingUploads reloads verified immutable outbox entries after restart.
func (s *CustodyStore) PendingUploads() ([]PendingUpload, error) {
	entries, err := os.ReadDir(filepath.Join(s.config.Root, "outbox"))
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	result := make([]PendingUpload, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil, errors.New("pending uploads: unexpected outbox entry")
		}
		pending, err := s.loadPending(filepath.Join(s.config.Root, "outbox", entry.Name()))
		if err != nil {
			return nil, err
		}
		if pending.Manifest.Direction != "upload" {
			return nil, errors.New("pending uploads: non-upload manifest in outbox")
		}
		result = append(result, pending)
	}
	return result, nil
}

// PendingUploadByPurpose returns at most the lexicographically first immutable
// outbox entry for a fixed purpose. Acknowledged entries are not rediscovered.
func (s *CustodyStore) PendingUploadByPurpose(purpose string) (*PendingUpload, error) {
	if purpose != "result" && purpose != "checkpoint" {
		return nil, errors.New("pending upload: unsupported purpose")
	}
	entries, err := os.ReadDir(filepath.Join(s.config.Root, "outbox"))
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil, errors.New("pending uploads: unexpected outbox entry")
		}
		pending, err := s.loadPending(filepath.Join(s.config.Root, "outbox", entry.Name()))
		if err != nil {
			return nil, err
		}
		matches := purpose == "result" && strings.HasSuffix(pending.Manifest.ID, "-result") ||
			purpose == "checkpoint" && strings.Contains(pending.Manifest.ID, "-checkpoint-")
		if matches {
			return &pending, nil
		}
	}
	return nil, nil
}

// AcknowledgeUpload durably removes one imported manifest from discovery while
// retaining its immutable custody record for restart reconciliation and audit.
func (s *CustodyStore) AcknowledgeUpload(manifestID string) error {
	if strings.TrimSpace(manifestID) != manifestID || manifestID == "" {
		return errors.New("acknowledge upload: manifest ID is required")
	}
	outbox := filepath.Join(s.config.Root, "outbox")
	acknowledged := filepath.Join(s.config.Root, "acknowledged")
	source, _, err := s.findPending(outbox, manifestID)
	if err != nil {
		return err
	}
	if source == "" {
		_, retained, retainedErr := s.findPending(acknowledged, manifestID)
		if retainedErr != nil {
			return retainedErr
		}
		if retained == nil {
			return errors.New("acknowledge upload: manifest not found")
		}
		return nil
	}
	target := filepath.Join(acknowledged, filepath.Base(source))
	if err := os.Rename(source, target); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, retained, retainedErr := s.findPending(acknowledged, manifestID)
			if retainedErr == nil && retained != nil {
				return nil
			}
		}
		return fmt.Errorf("acknowledge upload: retain manifest: %w", err)
	}
	if err := syncDirectory(outbox); err != nil {
		return err
	}
	return syncDirectory(acknowledged)
}

func (s *CustodyStore) findPending(directory, manifestID string) (string, *PendingUpload, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return "", nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return "", nil, errors.New("pending uploads: unexpected custody entry")
		}
		path := filepath.Join(directory, entry.Name())
		pending, err := s.loadPending(path)
		if err != nil {
			return "", nil, err
		}
		if pending.Manifest.ID == manifestID {
			return path, &pending, nil
		}
	}
	return "", nil, nil
}

func (s *CustodyStore) publishManifest(ctx context.Context, pkg workerproto.ExecutionPackage, purpose string, objects []workerproto.ArtifactObject) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if pkg.CoordinatorID != s.config.CoordinatorID || pkg.CoordinatorEpoch != s.config.CoordinatorEpoch ||
		pkg.WorkerID != s.config.WorkerID || pkg.WorkerEpoch != s.config.WorkerEpoch {
		return errors.New("publish artifact: execution package epoch binding mismatch")
	}
	id := "upload-" + pkg.Identity.AssignmentID + "-" + purpose
	path := filepath.Join(s.config.Root, "outbox", id+".json")
	var total int64
	for _, object := range objects {
		if err := workerproto.ValidateArtifactObject(object, s.config.MaxArtifactBytes); err != nil {
			return err
		}
		if object.Size > s.config.MaxTotalBytes-total {
			return errors.New("publish artifact: total size exceeds limit")
		}
		total += object.Size
	}
	if prior, err := s.loadPending(path); err == nil {
		if prior.Manifest.AssignmentID != pkg.Identity.AssignmentID ||
			prior.Manifest.AssignmentEpoch != pkg.Identity.AssignmentEpoch ||
			!reflect.DeepEqual(prior.Manifest.Objects, objects) {
			return errors.New("publish artifact: manifest id replay changed immutable content")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	now := s.now()
	manifest := workerproto.ArtifactTransferManifest{
		Version:          workerproto.ArtifactManifestVersion,
		ID:               id,
		Direction:        "upload",
		CoordinatorEpoch: s.config.CoordinatorEpoch,
		WorkerID:         s.config.WorkerID,
		WorkerEpoch:      s.config.WorkerEpoch,
		AssignmentID:     pkg.Identity.AssignmentID,
		AssignmentEpoch:  pkg.Identity.AssignmentEpoch,
		Objects:          append([]workerproto.ArtifactObject(nil), objects...),
		TotalBytes:       total,
		CreatedAt:        now,
		ExpiresAt:        now.Add(24 * time.Hour),
	}
	if err := s.validateManifest(manifest, "upload"); err != nil {
		return err
	}
	records := make([]workerproto.ArtifactCustodyRecord, 0, len(objects))
	var previous string
	for index, object := range objects {
		record, err := workerproto.BuildCustodyRecord(workerproto.ArtifactCustodyRecord{
			ManifestID:     id,
			ObjectID:       object.ID,
			From:           "worker:" + s.config.WorkerID,
			To:             "outbox:" + s.config.CoordinatorID,
			Sequence:       int64(index + 1),
			Size:           object.Size,
			SHA256:         object.SHA256,
			VerifiedAt:     now,
			PreviousSHA256: previous,
		})
		if err != nil {
			return err
		}
		previous = record.RecordSHA256
		records = append(records, record)
	}
	return writeJSONExclusive(path, PendingUpload{Version: custodyReceiptVersion, Manifest: manifest, Custody: records})
}

func (s *CustodyStore) validateManifest(manifest workerproto.ArtifactTransferManifest, direction string) error {
	if err := workerproto.ValidateArtifactTransferManifest(manifest, s.config.MaxArtifactBytes, s.config.MaxTotalBytes, s.now()); err != nil {
		return err
	}
	if manifest.Direction != direction || manifest.CoordinatorEpoch != s.config.CoordinatorEpoch ||
		manifest.WorkerID != s.config.WorkerID || manifest.WorkerEpoch != s.config.WorkerEpoch {
		return errors.New("artifact manifest: custody epoch binding mismatch")
	}
	return nil
}

func (s *CustodyStore) storeObject(reader io.Reader, object workerproto.ArtifactObject) error {
	if err := workerproto.ValidateArtifactObject(object, s.config.MaxArtifactBytes); err != nil {
		return err
	}
	dir := filepath.Join(s.config.Root, "objects", strings.ToLower(object.SHA256[:2]))
	if err := ensureRealDirectory(dir); err != nil {
		return err
	}
	target := s.objectPath(object.SHA256)
	if existing, err := openRegular(target); err == nil {
		defer existing.Close()
		if err := workerproto.VerifyArtifact(existing, object, s.config.MaxArtifactBytes); err != nil {
			return err
		}
		return workerproto.VerifyArtifact(reader, object, s.config.MaxArtifactBytes)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temp, err := os.CreateTemp(dir, ".receive-")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(temp, hash), io.LimitReader(reader, s.config.MaxArtifactBytes+1))
	if copyErr == nil && n != object.Size {
		copyErr = errors.New("artifact size mismatch")
	}
	if copyErr == nil && hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(object.SHA256) {
		copyErr = errors.New("artifact checksum mismatch")
	}
	if copyErr == nil {
		copyErr = temp.Sync()
	}
	closeErr := temp.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Chmod(tempName, 0o400); err != nil {
		return err
	}
	if err := os.Link(tempName, target); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, openErr := openRegular(target)
		if openErr != nil {
			return openErr
		}
		defer existing.Close()
		return workerproto.VerifyArtifact(existing, object, s.config.MaxArtifactBytes)
	}
	return syncDirectory(dir)
}

func (s *CustodyStore) objectPath(digest string) string {
	return filepath.Join(s.config.Root, "objects", strings.ToLower(digest[:2]), strings.ToLower(digest))
}

func (s *CustodyStore) loadPending(path string) (PendingUpload, error) {
	file, err := openRegular(path)
	if err != nil {
		return PendingUpload{}, err
	}
	defer file.Close()
	var pending PendingUpload
	decoder := json.NewDecoder(io.LimitReader(file, s.config.MaxTotalBytes+1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&pending); err != nil {
		return PendingUpload{}, fmt.Errorf("load custody record: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return PendingUpload{}, errors.New("load custody record: trailing content")
	}
	if pending.Version != custodyReceiptVersion {
		return PendingUpload{}, errors.New("load custody record: unsupported version")
	}
	if err := s.validateManifest(pending.Manifest, pending.Manifest.Direction); err != nil {
		return PendingUpload{}, err
	}
	if len(pending.Custody) != len(pending.Manifest.Objects) {
		return PendingUpload{}, errors.New("load custody record: custody length mismatch")
	}
	var previous string
	for index, record := range pending.Custody {
		object := pending.Manifest.Objects[index]
		if record.ManifestID != pending.Manifest.ID || record.ObjectID != object.ID ||
			record.Size != object.Size || !strings.EqualFold(record.SHA256, object.SHA256) ||
			record.Sequence != int64(index+1) || record.PreviousSHA256 != previous {
			return PendingUpload{}, errors.New("load custody record: custody chain mismatch")
		}
		if err := workerproto.ValidateCustodyRecord(record); err != nil {
			return PendingUpload{}, err
		}
		previous = record.RecordSHA256
	}
	return pending, nil
}

func finalizedArtifactPath(finalized backlog.FinalizedAttempt, artifact domain.Artifact) (string, error) {
	if finalized.StorageDir == "" {
		return "", errors.New("publish result: missing finalizer storage")
	}
	prefix := filepath.ToSlash(filepath.Join("runs", artifact.WorkflowRunID, artifact.TaskID, artifact.AttemptID)) + "/"
	if !strings.HasPrefix(artifact.StoragePath, prefix) {
		return "", errors.New("publish result: artifact escaped finalizer storage")
	}
	relative := strings.TrimPrefix(artifact.StoragePath, prefix)
	if relative == "" || filepath.IsAbs(relative) || strings.Contains(relative, "..") {
		return "", errors.New("publish result: unsafe finalizer artifact path")
	}
	return filepath.Join(finalized.StorageDir, filepath.FromSlash(relative)), nil
}

func objectForBytes(id, path, kind, media string, data []byte) workerproto.ArtifactObject {
	sum := sha256.Sum256(data)
	return workerproto.ArtifactObject{
		ID:        id,
		Path:      path,
		Kind:      kind,
		MediaType: media,
		Size:      int64(len(data)),
		SHA256:    hex.EncodeToString(sum[:]),
	}
}

func shortDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

func openRegular(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("path is not a regular file")
	}
	return os.Open(path)
}

func writeJSONExclusive(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".commit-")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tempName, 0o400); err != nil {
		return err
	}
	if err := os.Link(tempName, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *CustodyStore) now() time.Time {
	return s.config.Now().UTC()
}
