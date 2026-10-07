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
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const custodyReceiptVersion = 1

// UploadRetention is the advertised lifetime of an outbox upload. Durable
// results are never discarded for age on the import path; the lifetime only
// bounds how long a coordinator may keep discovering an unimported result.
const UploadRetention = 30 * 24 * time.Hour

// CustodyConfig binds a worker-local content store to one coordinator and worker epoch.
type CustodyConfig struct {
	SecretScan       SecretScanConfig
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

// ResultDurable checks the exact result receipt, including acknowledged results.
// Corrupt or differently bound receipts are ambiguous, never "not published".
func (s *CustodyStore) ResultDurable(pkg workerproto.ExecutionPackage) (bool, error) {
	if pkg.CoordinatorID != s.config.CoordinatorID || pkg.CoordinatorEpoch < 1 || pkg.CoordinatorEpoch > s.config.CoordinatorEpoch ||
		pkg.WorkerID != s.config.WorkerID || pkg.WorkerEpoch != s.config.WorkerEpoch {
		return false, errors.New("result custody: execution package epoch binding mismatch")
	}
	id := "upload-" + pkg.Identity.AssignmentID + "-result"
	// Outbox first: an acknowledgement can move it to acknowledged between
	// these reads, but cannot make a durable receipt disappear from both.
	for _, directory := range []string{"outbox", "acknowledged"} {
		prior, err := s.loadPending(filepath.Join(s.config.Root, directory, id+".json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			// Existing custody is ambiguous. Even a validation size error
			// here is not a new pre-publication size rejection.
			return false, fmt.Errorf("result custody receipt is unreadable: %v", err)
		}
		if prior.Manifest.ID != id || prior.Manifest.AssignmentID != pkg.Identity.AssignmentID ||
			prior.Manifest.AssignmentEpoch != pkg.Identity.AssignmentEpoch {
			return false, errors.New("result custody: immutable assignment binding mismatch")
		}
		if prior.Manifest.Direction != "upload" || prior.Manifest.CoordinatorEpoch < pkg.CoordinatorEpoch {
			return false, errors.New("result custody: result upload authority mismatch")
		}
		// loadPending already proves the complete ordered object chain and
		// record checksums. Result authority additionally fixes its endpoints,
		// just as the coordinator importer does; a rehashed foreign chain is
		// ambiguous custody, not permission to recapture or complete.
		for _, record := range prior.Custody {
			if record.From != "worker:"+pkg.WorkerID || record.To != "outbox:"+pkg.CoordinatorID {
				return false, errors.New("result custody: result upload custody authority mismatch")
			}
		}
		return true, nil
	}
	return false, nil
}

// resultObject is one object of a result upload: a finalized artifact, read
// from the finalizer's storage when it is stored, or bytes the result holds.
type resultObject struct {
	object   workerproto.ArtifactObject
	artifact *domain.Artifact
	data     []byte
	// failure prefixes an error storing the object.
	failure string
}

// resultObjects lists, in upload order, the objects of the one upload that
// carries result. It reads no file, so that AdmitResult checks exactly the
// upload PublishResult makes before anything exists to store.
func resultObjects(pkg workerproto.ExecutionPackage, result PublishedResult) ([]resultObject, error) {
	objects := make([]resultObject, 0, len(result.Finalized.Artifacts)+5)
	for index := range result.Finalized.Artifacts {
		artifact := &result.Finalized.Artifacts[index]
		transferName := artifact.Name
		if artifact.Kind == domain.ArtifactGate && artifact.Name == "gate" {
			transferName = "gate/report.json"
		}
		objects = append(objects, resultObject{
			object: workerproto.ArtifactObject{
				ID:        artifact.ID,
				Path:      "results/" + filepath.ToSlash(transferName),
				Kind:      string(artifact.Kind),
				MediaType: artifact.MediaType,
				Size:      artifact.Size,
				SHA256:    artifact.SHA256,
			},
			artifact: artifact,
			failure:  fmt.Sprintf("publish result %q", artifact.Name),
		})
	}
	if result.RecoveryProposal != nil {
		proposal := *result.RecoveryProposal
		if proposal.InstructionArtifact.ArtifactID == "" || len(result.RecoveryInstructions) == 0 {
			return nil, errors.New("publish result: recovery proposal is missing retained instructions")
		}
		instruction := objectForBytes(proposal.InstructionArtifact.ArtifactID,
			"results/recovery/instructions.md", string(domain.ArtifactInput), "text/markdown", result.RecoveryInstructions)
		if instruction.SHA256 != proposal.InstructionArtifact.Digest {
			return nil, errors.New("publish result: recovery instruction digest changed")
		}
		objects = append(objects, resultObject{object: instruction, data: result.RecoveryInstructions, failure: "publish recovery instructions"})
		if len(proposal.CheckpointArtifacts) > 1 {
			return nil, errors.New("publish result: recovery proposal has too many checkpoints")
		}
		if len(proposal.CheckpointArtifacts) == 1 {
			checkpoint := objectForBytes(proposal.CheckpointArtifacts[0].ArtifactID,
				"results/recovery/checkpoint.tar", string(domain.ArtifactCheckpoint), "application/x-tar", result.RecoveryCheckpointTar)
			if checkpoint.SHA256 != proposal.CheckpointArtifacts[0].Digest {
				return nil, errors.New("publish result: recovery checkpoint digest changed")
			}
			objects = append(objects, resultObject{object: checkpoint, data: result.RecoveryCheckpointTar, failure: "publish recovery checkpoint"})
		} else if result.RecoveryCheckpointTar != nil {
			return nil, errors.New("publish result: undeclared recovery checkpoint")
		}
		raw, err := json.Marshal(proposal)
		if err != nil {
			return nil, err
		}
		proposalObject := objectForBytes("recovery-proposal-"+pkg.Identity.AttemptID,
			"results/recovery/proposal.json", string(domain.ArtifactInput), "application/json", raw)
		objects = append(objects, resultObject{object: proposalObject, data: raw, failure: "publish recovery proposal"})
	} else if result.RecoveryInstructions != nil || result.RecoveryCheckpointTar != nil {
		return nil, errors.New("publish result: recovery bytes need a typed proposal")
	}
	if len(result.WorkInProgressBundle) != 0 {
		bundle := objectForBytes(backlog.WorkInProgressBundleID(pkg.Identity.AttemptID), "results/"+backlog.WorkInProgressBundleName,
			string(domain.ArtifactGitState), backlog.CommitBundleMediaType, result.WorkInProgressBundle)
		objects = append(objects, resultObject{object: bundle, data: result.WorkInProgressBundle, failure: "publish work-in-progress bundle"})
	}
	if continuation := result.Continuation; continuation != nil {
		if continuation.Checkpoint.AttemptID != pkg.Identity.AttemptID {
			return nil, errors.New("publish result: continuation checkpoint belongs to another attempt")
		}
		snapshot := objectForBytes(domain.ContinuationArtifactID(pkg.Identity.AttemptID),
			"results/"+domain.ContinuationArtifactName, string(domain.ArtifactCheckpoint), "text/markdown", continuation.Data)
		if snapshot.SHA256 != continuation.Checkpoint.SHA256 || snapshot.Size != continuation.Checkpoint.Size {
			return nil, errors.New("publish result: continuation snapshot does not match its checkpoint")
		}
		raw, err := json.Marshal(continuation.Checkpoint)
		if err != nil {
			return nil, err
		}
		metadata := objectForBytes(domain.ContinuationMetadataArtifactID(pkg.Identity.AttemptID),
			"results/"+domain.ContinuationMetadataArtifactName, string(domain.ArtifactCheckpoint), "application/json", raw)
		objects = append(objects,
			resultObject{object: snapshot, data: continuation.Data, failure: "publish continuation checkpoint"},
			resultObject{object: metadata, data: raw, failure: "publish continuation checkpoint metadata"})
	}
	for _, extra := range []struct {
		id, path, kind, media string
		data                  []byte
	}{
		{"final-message-" + pkg.Identity.AttemptID, "results/final-message.md", "summary", "text/markdown", []byte(result.FinalMessage)},
		{"thread-archive-" + pkg.Identity.AttemptID, "results/thread.json", "log", "application/json", result.ThreadArchive},
	} {
		object := objectForBytes(extra.id, extra.path, extra.kind, extra.media, extra.data)
		objects = append(objects, resultObject{object: object, data: extra.data, failure: fmt.Sprintf("publish result %q", extra.path)})
	}
	return objects, nil
}

// AdmitResult reports whether PublishResult would accept the upload of result,
// by the same object list and the same limit and manifest checks, without
// storing anything. The finalizer asks it before it settles optional result
// metadata, so that metadata never makes a result fail that would otherwise
// publish.
func (s *CustodyStore) AdmitResult(pkg workerproto.ExecutionPackage, result PublishedResult) error {
	result = boundThreadArchive(pkg, result, s.resultLimits(pkg))
	planned, err := resultObjects(pkg, result)
	if err != nil {
		return err
	}
	return s.admitPlannedResult(context.Background(), pkg, result, planned)
}

func (s *CustodyStore) admitPlannedResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult, planned []resultObject) error {
	objects := make([]workerproto.ArtifactObject, 0, len(planned))
	for _, entry := range planned {
		objects = append(objects, entry.object)
	}
	total, err := s.checkUpload(pkg, objects)
	if err != nil {
		return err
	}
	if err := s.validateManifest(s.uploadManifest(pkg, "result", objects, total, s.now()), "upload"); err != nil {
		return err
	}
	// The finalizer's optional-bundle size probe has no file bytes yet.
	if result.Finalized.StorageDir == "" && len(result.Finalized.Artifacts) > 0 {
		return nil
	}
	return s.scanResult(ctx, pkg, result, planned)
}

// PublishResult retains finalizer output, final message, and thread archive as one upload.
// A thread archive the upload has no room for is published compacted, and the
// full archive is kept in this store, never uploaded.
func (s *CustodyStore) PublishResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult) error {
	if durable, err := s.ResultDurable(pkg); err != nil || durable {
		return err
	}
	full := result.ThreadArchive
	result = boundThreadArchive(pkg, result, s.resultLimits(pkg))
	if !bytes.Equal(full, result.ThreadArchive) {
		retained, err := s.retainThreadArchive(pkg.Identity.AttemptID, full)
		if err != nil {
			return fmt.Errorf("publish result: retain full thread archive: %w", err)
		}
		slog.Warn("thread archive over the result upload limits; publishing a compacted archive",
			"attempt", pkg.Identity.AttemptID, "size", len(full), "compacted", len(result.ThreadArchive), "retained", retained)
	}
	planned, err := resultObjects(pkg, result)
	if err != nil {
		return err
	}
	objects := make([]workerproto.ArtifactObject, 0, len(planned))
	if err := s.admitPlannedResult(ctx, pkg, result, planned); err != nil {
		return err
	}
	for _, entry := range planned {
		if err := s.storeResultObject(result.Finalized, entry); err != nil {
			return err
		}
		objects = append(objects, entry.object)
	}
	return s.publishManifest(ctx, pkg, "result", objects)
}

// uploadLimits are the per-object and aggregate byte limits of one upload.
type uploadLimits struct {
	object, total int64
}

// resultLimits are the limits a result upload for pkg must meet: this store's,
// and the package's where it sets tighter ones, because the coordinator
// imports the result under the package's.
func (s *CustodyStore) resultLimits(pkg workerproto.ExecutionPackage) uploadLimits {
	limits := uploadLimits{object: s.config.MaxArtifactBytes, total: s.config.MaxTotalBytes}
	if pkg.Limits.MaxArtifactBytes > 0 && pkg.Limits.MaxArtifactBytes < limits.object {
		limits.object = pkg.Limits.MaxArtifactBytes
	}
	if pkg.Limits.MaxTotalBytes > 0 && pkg.Limits.MaxTotalBytes < limits.total {
		limits.total = pkg.Limits.MaxTotalBytes
	}
	return limits
}

// retainedThreadArchivePath is where a worker keeps the full thread archive of
// an attempt whose upload carries a compacted one, relative to the custody
// root. It is named by the archive's digest, so a retry finds its own copy.
func retainedThreadArchivePath(attemptID string, archive []byte) string {
	sum := sha256.Sum256(archive)
	return path.Join("thread-archives", attemptID, hex.EncodeToString(sum[:])+".json")
}

// boundThreadArchive returns result with its thread archive compacted to the
// room the upload leaves it under limits, when the archive is what would make
// the upload too large. Anything else is returned unchanged, so that a size
// error on another object, an archive within its room and an archive that
// cannot be compacted all meet the upload's own validation as before.
func boundThreadArchive(pkg workerproto.ExecutionPackage, result PublishedResult, limits uploadLimits) PublishedResult {
	planned, err := resultObjects(pkg, result)
	if err != nil || len(planned) == 0 {
		return result
	}
	// The thread archive is the last object of the upload; every other
	// object must be accepted on its own first.
	others := make([]workerproto.ArtifactObject, 0, len(planned)-1)
	for _, entry := range planned[:len(planned)-1] {
		others = append(others, entry.object)
	}
	total, err := workerproto.ValidateUploadObjects(others, limits.object, limits.total)
	if err != nil {
		return result
	}
	room := min(limits.object, limits.total-total)
	if int64(len(result.ThreadArchive)) <= room {
		return result
	}
	attemptID := pkg.Identity.AttemptID
	if attemptID == "" || filepath.Base(attemptID) != attemptID || attemptID == "." || attemptID == ".." {
		return result
	}
	compacted, err := backlog.CompactThreadArchive(result.ThreadArchive, room,
		retainedThreadArchivePath(attemptID, result.ThreadArchive))
	if err != nil {
		return result
	}
	result.ThreadArchive = compacted
	return result
}

// retainThreadArchive keeps the full thread archive of an attempt in this
// store, read-only, and returns its path. A copy already there is verified.
func (s *CustodyStore) retainThreadArchive(attemptID string, archive []byte) (string, error) {
	relative := retainedThreadArchivePath(attemptID, archive)
	target := filepath.Join(s.config.Root, filepath.FromSlash(relative))
	dir := filepath.Dir(target)
	if err := ensureRealDirectory(filepath.Dir(dir)); err != nil {
		return "", err
	}
	if err := ensureRealDirectory(dir); err != nil {
		return "", err
	}
	if existing, err := openRegular(target); err == nil {
		defer existing.Close()
		kept, err := io.ReadAll(existing)
		if err != nil {
			return "", err
		}
		if !bytes.Equal(kept, archive) {
			return "", errors.New("retained thread archive differs from its digest")
		}
		return target, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	temp, err := os.CreateTemp(dir, ".retain-")
	if err != nil {
		return "", err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	_, writeErr := temp.Write(archive)
	if writeErr == nil {
		writeErr = temp.Sync()
	}
	if closeErr := temp.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return "", writeErr
	}
	if err := os.Chmod(tempName, 0o400); err != nil {
		return "", err
	}
	if err := os.Link(tempName, target); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	return target, syncDirectory(dir)
}

func (s *CustodyStore) storeResultObject(finalized backlog.FinalizedAttempt, entry resultObject) error {
	if entry.artifact == nil {
		if err := s.storeObject(bytes.NewReader(entry.data), entry.object); err != nil {
			return fmt.Errorf("%s: %w", entry.failure, err)
		}
		return nil
	}
	source, err := finalizedArtifactPath(finalized, *entry.artifact)
	if err != nil {
		return err
	}
	file, err := openRegular(source)
	if err != nil {
		return fmt.Errorf("publish result: open %q: %w", entry.artifact.Name, err)
	}
	storeErr := s.storeObject(file, entry.object)
	closeErr := file.Close()
	if storeErr != nil {
		return fmt.Errorf("%s: %w", entry.failure, storeErr)
	}
	return closeErr
}

// PublishCheckpoint retains checkpoint bytes and advertises an immutable upload.
func (s *CustodyStore) PublishCheckpoint(ctx context.Context, pkg workerproto.ExecutionPackage, path string, data []byte) (*domain.CheckpointMetadata, error) {
	// The task wrote the checkpoint, and it reaches the coordinator like a
	// result, so it passes the same scan; patterns only warn, as for outputs.
	scanner, _, err := s.executionScanner(ctx, pkg)
	if err != nil {
		return nil, err
	}
	if err := scanner.scan(path, "checkpoint", bytes.NewReader(data)); err != nil {
		return nil, err
	}
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

// PublishContinuation retains a continuation.md snapshot a running attempt
// took and advertises it, with its metadata, as an immutable upload on the
// checkpoint channel. The manifest and both objects are named by the
// attempt and the snapshot's sequence, so a replay is the same upload and a
// newer snapshot is a new one.
func (s *CustodyStore) PublishContinuation(ctx context.Context, pkg workerproto.ExecutionPackage, snapshot ContinuationSnapshot) error {
	checkpoint := snapshot.Checkpoint
	epoch := pkg.Identity.AssignmentEpoch
	if checkpoint.AttemptID != pkg.Identity.AttemptID || checkpoint.Sequence < 1 || epoch < 1 {
		return errors.New("publish continuation checkpoint: the snapshot belongs to another attempt")
	}
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	snapshotID := domain.ContinuationLiveArtifactID(checkpoint.AttemptID, epoch, checkpoint.Sequence)
	metadataID := domain.ContinuationLiveMetadataArtifactID(checkpoint.AttemptID, epoch, checkpoint.Sequence)
	objects := []workerproto.ArtifactObject{
		objectForBytes(snapshotID, "checkpoints/"+snapshotID+".md", string(domain.ArtifactCheckpoint), "text/markdown", snapshot.Data),
		objectForBytes(metadataID, "checkpoints/"+metadataID+".json", string(domain.ArtifactCheckpoint), "application/json", raw),
	}
	if objects[0].SHA256 != checkpoint.SHA256 || objects[0].Size != checkpoint.Size {
		return errors.New("publish continuation checkpoint: the snapshot does not match its checkpoint")
	}
	// The task wrote continuation.md, and the snapshot reaches the
	// coordinator while the attempt runs, so it passes the same scan as every
	// other checkpoint; a refused snapshot stays on the worker.
	scanner, _, err := s.executionScanner(ctx, pkg)
	if err != nil {
		return err
	}
	if err := scanner.scan(objects[0].Path, "checkpoint", bytes.NewReader(snapshot.Data)); err != nil {
		return err
	}
	for index, data := range [][]byte{snapshot.Data, raw} {
		if err := s.storeObject(bytes.NewReader(data), objects[index]); err != nil {
			return fmt.Errorf("publish continuation checkpoint: %w", err)
		}
	}
	// The purpose contains "checkpoint-", so the coordinator polls it with the
	// other checkpoints; the zero-padded epoch and sequence keep snapshots in
	// order and apart from another dispatch's.
	return s.publishManifest(ctx, pkg, fmt.Sprintf("checkpoint-continuation-%020d-%020d", epoch, checkpoint.Sequence), objects)
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
			continue
		}
		pending, err := s.loadPending(filepath.Join(s.config.Root, "outbox", entry.Name()))
		if err != nil {
			slog.Warn("custody outbox entry skipped", "entry", entry.Name(), "error", err)
			continue
		}
		if pending.Manifest.Direction != "upload" {
			slog.Warn("custody outbox entry is not an upload; skipped", "entry", entry.Name())
			continue
		}
		result = append(result, pending)
	}
	return result, nil
}

// PendingUploadByPurpose returns at most the lexicographically first immutable
// outbox entry for a fixed purpose. Acknowledged entries are not rediscovered.
func (s *CustodyStore) PendingUploadByPurpose(purpose string, exclude ...string) (*PendingUpload, error) {
	if purpose != "result" && purpose != "checkpoint" {
		return nil, errors.New("pending upload: unsupported purpose")
	}
	excluded := make(map[string]struct{}, len(exclude))
	for _, id := range exclude {
		excluded[id] = struct{}{}
	}
	entries, err := os.ReadDir(filepath.Join(s.config.Root, "outbox"))
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		pending, err := s.loadPending(filepath.Join(s.config.Root, "outbox", entry.Name()))
		if err != nil {
			slog.Warn("custody outbox entry skipped", "entry", entry.Name(), "error", err)
			continue
		}
		if _, skip := excluded[pending.Manifest.ID]; skip {
			continue
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
			continue
		}
		path := filepath.Join(directory, entry.Name())
		pending, err := s.loadPending(path)
		if err != nil {
			slog.Warn("custody entry skipped", "entry", path, "error", err)
			continue
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
	total, err := s.checkUpload(pkg, objects)
	if err != nil {
		return err
	}
	id := "upload-" + pkg.Identity.AssignmentID + "-" + purpose
	path := filepath.Join(s.config.Root, "outbox", id+".json")
	if prior, err := s.loadPending(path); err == nil {
		if prior.Manifest.AssignmentID != pkg.Identity.AssignmentID ||
			prior.Manifest.AssignmentEpoch != pkg.Identity.AssignmentEpoch {
			return errors.New("publish artifact: manifest id replay changed immutable content")
		}
		if reflect.DeepEqual(prior.Manifest.Objects, objects) {
			return nil
		}
		// First durable result wins, including a failed envelope. A replay
		// must never replace success with a later failure or recapture.
		if purpose == "result" {
			return nil
		}
		return errors.New("publish artifact: manifest id replay changed immutable content")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	now := s.now()
	manifest := s.uploadManifest(pkg, purpose, objects, total, now)
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

// checkUpload checks that this store may publish an upload of objects for pkg
// and that the objects fit its per-object and aggregate limits, and returns
// their total size.
func (s *CustodyStore) checkUpload(pkg workerproto.ExecutionPackage, objects []workerproto.ArtifactObject) (int64, error) {
	// A package created under an earlier coordinator authority is still this
	// worker's own execution; its result must survive a coordinator restart.
	if pkg.CoordinatorID != s.config.CoordinatorID || pkg.CoordinatorEpoch < 1 || pkg.CoordinatorEpoch > s.config.CoordinatorEpoch ||
		pkg.WorkerID != s.config.WorkerID || pkg.WorkerEpoch != s.config.WorkerEpoch {
		return 0, errors.New("publish artifact: execution package epoch binding mismatch")
	}
	return workerproto.ValidateUploadObjects(objects, s.config.MaxArtifactBytes, s.config.MaxTotalBytes)
}

// uploadManifest is the manifest of one upload of objects for pkg.
func (s *CustodyStore) uploadManifest(pkg workerproto.ExecutionPackage, purpose string, objects []workerproto.ArtifactObject, total int64, now time.Time) workerproto.ArtifactTransferManifest {
	return workerproto.ArtifactTransferManifest{
		Version:          workerproto.ArtifactManifestVersion,
		ID:               "upload-" + pkg.Identity.AssignmentID + "-" + purpose,
		Direction:        "upload",
		CoordinatorEpoch: s.config.CoordinatorEpoch,
		WorkerID:         s.config.WorkerID,
		WorkerEpoch:      s.config.WorkerEpoch,
		AssignmentID:     pkg.Identity.AssignmentID,
		AssignmentEpoch:  pkg.Identity.AssignmentEpoch,
		Objects:          append([]workerproto.ArtifactObject(nil), objects...),
		TotalBytes:       total,
		CreatedAt:        now,
		ExpiresAt:        now.Add(UploadRetention),
	}
}

func (s *CustodyStore) validateManifest(manifest workerproto.ArtifactTransferManifest, direction string) error {
	if err := workerproto.ValidateArtifactTransferManifest(manifest, s.config.MaxArtifactBytes, s.config.MaxTotalBytes, s.now()); err != nil {
		return err
	}
	epochMatches := manifest.CoordinatorEpoch == s.config.CoordinatorEpoch
	if direction == "upload" {
		// A completed upload is immutable assignment evidence. It must remain
		// readable after coordinator failover, while future-authority evidence
		// is never accepted by an older coordinator.
		epochMatches = manifest.CoordinatorEpoch > 0 && manifest.CoordinatorEpoch <= s.config.CoordinatorEpoch
	}
	if manifest.Direction != direction || !epochMatches || manifest.WorkerID != s.config.WorkerID || manifest.WorkerEpoch != s.config.WorkerEpoch {
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
	// The path may have been replaced since the check: the open does not wait
	// on a FIFO, and the opened file is checked again.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, err
	}
	if info, err := f.Stat(); err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("path is not a regular file")
	}
	return f, nil
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
