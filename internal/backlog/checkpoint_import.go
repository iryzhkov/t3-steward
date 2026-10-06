package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type CheckpointImportStore interface {
	ArtifactCatalog
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	LoadThrottleAttemptRecords(context.Context) ([]domain.ThrottleAttemptRecord, error)
	RecordCheckpointImportRejection(context.Context, sqlite.CheckpointImportRejection) (domain.AuditEvent, error)
}

type CoordinatorCheckpointImporter struct {
	CoordinatorID    string
	CoordinatorEpoch int64
	Store            CheckpointImportStore
	Artifacts        CoordinatorArtifactStore
	MaxArtifactBytes int64
	MaxTotalBytes    int64
	Now              func() time.Time
}

// Import verifies that an announced checkpoint is the exact checkpoint already
// accepted by the coordinator's throttle projection, then publishes its bytes
// into coordinator ownership. Exact replay returns immutable metadata.
func (i CoordinatorCheckpointImporter) Import(ctx context.Context, response workerproto.ArtifactUploadResponse, opener WorkerUploadOpener) (domain.Artifact, error) {
	if i.Store == nil || opener == nil || i.CoordinatorID == "" || i.CoordinatorEpoch < 1 ||
		i.MaxArtifactBytes < 1 || i.MaxTotalBytes < i.MaxArtifactBytes {
		return domain.Artifact{}, errors.New("checkpoint import requires authority, storage, opener, and positive limits")
	}
	if i.Artifacts.Catalog == nil {
		i.Artifacts.Catalog = i.Store
	}
	now := time.Now().UTC()
	if i.Now != nil {
		now = i.Now().UTC()
	}
	manifest := response.Manifest
	if err := workerproto.ValidateArtifactTransferManifest(manifest, i.MaxArtifactBytes, i.MaxTotalBytes, now); err != nil {
		return domain.Artifact{}, err
	}
	if isContinuationUpload(manifest) {
		return i.importContinuation(ctx, response, opener, now)
	}
	// A checkpoint announced under an earlier coordinator epoch is still this
	// worker's own execution, exactly as a result is (ResultImporter). Requiring
	// the current epoch made every checkpoint pending across a coordinator
	// restart unimportable, and since the upload is only acknowledged after an
	// import, the worker offered it again on every boundary and its whole
	// exchange failed each time (S14: homelab, from epoch 224 on).
	if manifest.Direction != "upload" || manifest.CoordinatorEpoch < 1 || manifest.CoordinatorEpoch > i.CoordinatorEpoch ||
		len(manifest.Objects) != 1 {
		return domain.Artifact{}, errors.New("checkpoint import manifest authority or object count mismatch")
	}
	if err := validateWorkerUploadCustody(response, i.CoordinatorID); err != nil {
		return domain.Artifact{}, err
	}
	object := manifest.Objects[0]
	if object.Kind != string(domain.ArtifactCheckpoint) || object.MediaType != "text/markdown" ||
		!strings.HasPrefix(object.Path, "checkpoints/") {
		return domain.Artifact{}, errors.New("checkpoint import object identity mismatch")
	}
	records, err := i.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return domain.Artifact{}, err
	}
	assignment, attempt, task, err := checkpointImportBinding(records, manifest, object.ID)
	if err != nil {
		if checkpointBindingIsFinal(assignment, attempt, manifest) {
			return domain.Artifact{}, i.reject(ctx, records, manifest, now, err)
		}
		return domain.Artifact{}, err
	}
	throttle, err := i.Store.LoadThrottleAttemptRecords(ctx)
	if err != nil {
		return domain.Artifact{}, err
	}
	checkpoint, err := checkpointImportEvidence(throttle, attempt.ID, object)
	if err != nil {
		// Evidence for a settled assignment will never arrive.
		if assignment.State == domain.AssignmentCompleted {
			return domain.Artifact{}, i.reject(ctx, records, manifest, now, err)
		}
		return domain.Artifact{}, err
	}
	data, err := i.readCheckpointObject(ctx, opener, object)
	if err != nil {
		return domain.Artifact{}, err
	}
	artifact := domain.Artifact{
		ID: object.ID, WorkflowRunID: attempt.WorkflowRunID, TaskID: task.ID, AttemptID: attempt.ID,
		Kind: domain.ArtifactCheckpoint, Name: checkpoint.Path, MediaType: object.MediaType,
		Size: object.Size, SHA256: strings.ToLower(object.SHA256), Producer: "worker:" + manifest.WorkerID,
		CreatedAt: checkpoint.CapturedAt,
	}
	return i.Artifacts.Publish(ctx, domain.ArtifactPublication{
		CoordinatorEpoch: i.CoordinatorEpoch, WorkerID: manifest.WorkerID, WorkerEpoch: manifest.WorkerEpoch,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		AttemptRevision: attempt.Revision, Artifact: artifact,
	}, bytes.NewReader(data))
}

// readCheckpointObject reads one announced object and verifies it against
// the manifest's size and digest.
func (i CoordinatorCheckpointImporter) readCheckpointObject(ctx context.Context, opener WorkerUploadOpener, object workerproto.ArtifactObject) ([]byte, error) {
	reader, err := opener.OpenWorkerUpload(ctx, object)
	if err != nil {
		return nil, fmt.Errorf("open worker checkpoint %q: %w", object.ID, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, object.Size+1))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(data)) != object.Size {
		return nil, errors.New("worker checkpoint size changed")
	}
	if err := workerproto.VerifyArtifact(bytes.NewReader(data), object, i.MaxArtifactBytes); err != nil {
		return nil, err
	}
	return data, nil
}

// isContinuationUpload reports whether a checkpoint-channel upload is a
// continuation.md snapshot a running attempt handed on: the snapshot and the
// metadata describing it, in that order.
func isContinuationUpload(manifest workerproto.ArtifactTransferManifest) bool {
	return len(manifest.Objects) == 2 &&
		strings.HasPrefix(manifest.Objects[0].ID, "continuation-") &&
		strings.HasPrefix(manifest.Objects[1].ID, "continuation-meta-")
}

// importContinuation imports a continuation.md snapshot a worker took at a
// turn end or a pause, while its attempt still holds its assignment. It is
// what lets a superseded attempt, which never publishes a result, hand its
// latest checkpoint to the task's next attempt.
//
// It binds exactly as a checkpoint does (same assignment, assignment epoch,
// worker and worker epoch; claimed with a running attempt or completed with a
// settled one), except that no throttle evidence names it: the metadata
// travelling with the snapshot is checked against it instead, and the
// snapshot must be under its attempt's own identity and sequence. An upload
// whose assignment has moved on, or whose metadata does not describe it, is
// refused for good. Only the snapshot is kept, dated by its capture; an exact
// replay returns the same immutable artifact.
func (i CoordinatorCheckpointImporter) importContinuation(ctx context.Context, response workerproto.ArtifactUploadResponse, opener WorkerUploadOpener, now time.Time) (domain.Artifact, error) {
	manifest := response.Manifest
	if manifest.Direction != "upload" || manifest.CoordinatorEpoch < 1 || manifest.CoordinatorEpoch > i.CoordinatorEpoch {
		return domain.Artifact{}, errors.New("continuation import manifest authority mismatch")
	}
	if err := validateWorkerUploadCustody(response, i.CoordinatorID); err != nil {
		return domain.Artifact{}, err
	}
	records, err := i.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return domain.Artifact{}, err
	}
	assignment, attempt, task, err := checkpointImportBinding(records, manifest, "")
	if err == nil && attempt.IsSupervisionActivation() {
		err = errors.New("continuation import names a supervision activation")
	}
	if err != nil {
		if checkpointBindingIsFinal(assignment, attempt, manifest) || attempt.IsSupervisionActivation() {
			return domain.Artifact{}, i.reject(ctx, records, manifest, now, err)
		}
		return domain.Artifact{}, err
	}
	snapshot, metadata := manifest.Objects[0], manifest.Objects[1]
	payloads := make([][]byte, len(manifest.Objects))
	for index, object := range manifest.Objects {
		if payloads[index], err = i.readCheckpointObject(ctx, opener, object); err != nil {
			return domain.Artifact{}, err
		}
	}
	var checkpoint domain.ContinuationCheckpoint
	reason := ""
	switch {
	case json.Unmarshal(payloads[1], &checkpoint) != nil:
		reason = "the metadata is not a checkpoint description"
	case checkpoint.AttemptID != attempt.ID || checkpoint.Sequence < 1 ||
		snapshot.ID != domain.ContinuationLiveArtifactID(attempt.ID, checkpoint.Sequence) ||
		metadata.ID != domain.ContinuationLiveMetadataArtifactID(attempt.ID, checkpoint.Sequence):
		reason = "the snapshot is not under its attempt's own identity and sequence"
	case snapshot.Kind != string(domain.ArtifactCheckpoint) || snapshot.MediaType != "text/markdown" || !strings.HasPrefix(snapshot.Path, "checkpoints/") ||
		metadata.Kind != string(domain.ArtifactCheckpoint) || metadata.MediaType != "application/json" || !strings.HasPrefix(metadata.Path, "checkpoints/"):
		reason = "the objects are not a snapshot and its metadata"
	case !strings.EqualFold(checkpoint.SHA256, snapshot.SHA256) || checkpoint.Size != snapshot.Size || checkpoint.Size > domain.ContinuationSnapshotLimit:
		reason = "the metadata does not describe the snapshot"
	case checkpoint.CapturedAt.IsZero() || checkpoint.CapturedAt.After(manifest.CreatedAt):
		reason = "the capture time is missing or later than the upload"
	}
	if reason != "" {
		return domain.Artifact{}, i.reject(ctx, records, manifest, now, errors.New("continuation checkpoint: "+reason))
	}
	artifact := domain.Artifact{
		ID: snapshot.ID, WorkflowRunID: attempt.WorkflowRunID, TaskID: task.ID, AttemptID: attempt.ID,
		Kind: domain.ArtifactCheckpoint, Name: domain.ContinuationArtifactName, MediaType: snapshot.MediaType,
		Size: snapshot.Size, SHA256: strings.ToLower(snapshot.SHA256), Producer: "worker:" + manifest.WorkerID,
		CreatedAt: checkpoint.CapturedAt.UTC(),
	}
	published, err := i.Artifacts.Publish(ctx, domain.ArtifactPublication{
		CoordinatorEpoch: i.CoordinatorEpoch, WorkerID: manifest.WorkerID, WorkerEpoch: manifest.WorkerEpoch,
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		AttemptRevision: attempt.Revision, Artifact: artifact,
	}, bytes.NewReader(payloads[0]))
	if errors.Is(err, sqlite.ErrArtifactConflict) {
		// The attempt already handed on different bytes under this sequence;
		// the first stands and this one can never be imported.
		return domain.Artifact{}, i.reject(ctx, records, manifest, now, err)
	}
	return published, err
}

// ErrCheckpointImportRejected marks a checkpoint upload that can never be
// imported, because the assignment it names is gone or settled without it, or
// its attempt has since recorded a different checkpoint. The caller discards
// it, loudly, instead of failing the worker's whole exchange on every boundary
// for as long as the worker keeps offering it. The bytes stay in the worker's
// acknowledged custody.
var ErrCheckpointImportRejected = errors.New("checkpoint import rejected")

// reject records a final refusal as an audit event on the attempt the upload
// was taken for, then reports it as ErrCheckpointImportRejected. The event is
// written before the caller acknowledges the upload, so a discarded checkpoint
// always shows in the run's events rather than only in the coordinator's
// journal. When the event cannot be written the refusal is returned as an
// ordinary error, which the caller defers and retries, because acknowledging
// without the record would lose the only trace of the checkpoint.
func (i CoordinatorCheckpointImporter) reject(ctx context.Context, records sqlite.CoordinatorRecords, manifest workerproto.ArtifactTransferManifest, now time.Time, cause error) error {
	var attemptID string
	for _, assignment := range records.Assignments {
		if assignment.ID == manifest.AssignmentID {
			attemptID = assignment.AttemptID
			break
		}
	}
	var attempt domain.Attempt
	for _, candidate := range records.Attempts {
		if attemptID != "" && candidate.ID == attemptID {
			attempt = candidate
			break
		}
	}
	object := manifest.Objects[0]
	if _, err := i.Store.RecordCheckpointImportRejection(ctx, sqlite.CheckpointImportRejection{
		ManifestID: manifest.ID, ArtifactID: object.ID,
		WorkerID: manifest.WorkerID, WorkerEpoch: manifest.WorkerEpoch,
		AssignmentID: manifest.AssignmentID, AssignmentEpoch: manifest.AssignmentEpoch,
		WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, AttemptID: attempt.ID,
		CoordinatorEpoch: i.CoordinatorEpoch,
		Reason: fmt.Sprintf("worker %s checkpoint for assignment %s discarded: %v",
			manifest.WorkerID, manifest.AssignmentID, cause),
		RejectedAt: now,
	}); err != nil {
		return fmt.Errorf("record checkpoint import rejection: %w (refusal: %w)", err, cause)
	}
	return fmt.Errorf("%w: %w", ErrCheckpointImportRejected, cause)
}

// checkpointBindingIsFinal reports whether a binding refusal can never turn
// into an import: the assignment is gone, at another epoch, released, or
// settled. A claimed assignment is never final; discarding its checkpoint could
// lose the resume context.
func checkpointBindingIsFinal(assignment domain.Assignment, attempt domain.Attempt, manifest workerproto.ArtifactTransferManifest) bool {
	if assignment.ID == "" || assignment.Epoch != manifest.AssignmentEpoch ||
		(assignment.State != domain.AssignmentClaimed && assignment.State != domain.AssignmentCompleted) {
		return true
	}
	// While the assignment is claimed nothing is final: the attempt's recorded
	// checkpoint is projected from throttle records and is not guaranteed to
	// move only forward. A settled assignment's attempt will not change again.
	return assignment.State == domain.AssignmentCompleted
}

// checkpointImportBinding binds an upload to its assignment, attempt and
// task. A non-empty checkpointID must be the checkpoint the attempt records.
func checkpointImportBinding(records sqlite.CoordinatorRecords, manifest workerproto.ArtifactTransferManifest, checkpointID string) (domain.Assignment, domain.Attempt, domain.Task, error) {
	var assignment domain.Assignment
	for _, candidate := range records.Assignments {
		if candidate.ID == manifest.AssignmentID {
			assignment = candidate
			break
		}
	}
	if assignment.ID == "" || (assignment.State != domain.AssignmentClaimed && assignment.State != domain.AssignmentCompleted) || assignment.Epoch != manifest.AssignmentEpoch ||
		assignment.WorkerID != manifest.WorkerID || assignment.WorkerEpoch != manifest.WorkerEpoch {
		return assignment, domain.Attempt{}, domain.Task{}, errors.New("checkpoint import assignment binding is stale")
	}
	var attempt domain.Attempt
	for _, candidate := range records.Attempts {
		if candidate.ID == assignment.AttemptID {
			attempt = candidate
			break
		}
	}
	// A claimed attempt binds while it still records this checkpoint, whatever
	// its control: a pause that is later resumed keeps the checkpoint it took,
	// and an import that missed the paused window must not become impossible
	// (and block the worker's exchange) just because the attempt is running.
	// A continuation snapshot (no checkpointID) taken at the attempt's last
	// turn end may arrive after its result was reported and before the
	// result is imported, while the attempt verifies.
	validProgress := assignment.State == domain.AssignmentClaimed && !attempt.Progress.Terminal() ||
		assignment.State == domain.AssignmentCompleted && (attempt.Progress.Terminal() ||
			checkpointID == "" && attempt.Progress == domain.ProgressVerifying)
	if attempt.ID == "" || attempt.AssignmentID != assignment.ID || !validProgress ||
		(checkpointID != "" && attempt.CheckpointArtifactID != checkpointID) {
		return assignment, attempt, domain.Task{}, errors.New("checkpoint import attempt binding is stale")
	}
	task, _ := domain.TaskForAttempt(attempt, records.WorkflowRuns, records.Tasks)
	if task.ID == "" {
		return assignment, attempt, task, errors.New("checkpoint import task is missing")
	}
	return assignment, attempt, task, nil
}

func checkpointImportEvidence(records []domain.ThrottleAttemptRecord, attemptID string, object workerproto.ArtifactObject) (*domain.CheckpointMetadata, error) {
	var found *domain.CheckpointMetadata
	for _, record := range records {
		// The checkpoint itself is the durable evidence: it is set only from a
		// validated checkpointed acknowledgement, and planning a resume rewrites
		// the record's delivery and result while keeping it. Requiring the
		// acknowledged checkpointed state refused every import after a resume.
		if record.AttemptID != attemptID || record.Checkpoint == nil ||
			record.Checkpoint.ArtifactID != object.ID {
			continue
		}
		if record.Checkpoint.Size != object.Size || !strings.EqualFold(record.Checkpoint.SHA256, object.SHA256) {
			return nil, errors.New("checkpoint import throttle evidence mismatch")
		}
		checkpoint := *record.Checkpoint
		if found != nil && *found != checkpoint {
			return nil, errors.New("checkpoint import has conflicting throttle evidence")
		}
		found = &checkpoint
	}
	if found == nil {
		return nil, errors.New("checkpoint import has no acknowledged throttle evidence")
	}
	return found, nil
}
