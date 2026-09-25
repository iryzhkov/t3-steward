package backlog

import (
	"bytes"
	"context"
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
	assignment, attempt, task, err := checkpointImportBinding(records, manifest)
	if err != nil {
		if checkpointBindingIsFinal(assignment, attempt, manifest) {
			return domain.Artifact{}, fmt.Errorf("%w: %w", ErrCheckpointImportRejected, err)
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
			return domain.Artifact{}, fmt.Errorf("%w: %w", ErrCheckpointImportRejected, err)
		}
		return domain.Artifact{}, err
	}
	reader, err := opener.OpenWorkerUpload(ctx, object)
	if err != nil {
		return domain.Artifact{}, fmt.Errorf("open worker checkpoint %q: %w", object.ID, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, object.Size+1))
	closeErr := reader.Close()
	if readErr != nil {
		return domain.Artifact{}, readErr
	}
	if closeErr != nil {
		return domain.Artifact{}, closeErr
	}
	if int64(len(data)) != object.Size {
		return domain.Artifact{}, errors.New("worker checkpoint size changed")
	}
	if err := workerproto.VerifyArtifact(bytes.NewReader(data), object, i.MaxArtifactBytes); err != nil {
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

// ErrCheckpointImportRejected marks a checkpoint upload that can never be
// imported, because the assignment it names is gone or settled without it, or
// its attempt has since recorded a different checkpoint. The caller discards
// it, loudly, instead of failing the worker's whole exchange on every boundary
// for as long as the worker keeps offering it. The bytes stay in the worker's
// acknowledged custody.
var ErrCheckpointImportRejected = errors.New("checkpoint import rejected")

// checkpointBindingIsFinal reports whether a binding refusal can never turn
// into an import. A claimed assignment whose attempt is not yet marked paused
// is not final: the pause may be recorded on a later boundary, and discarding
// the checkpoint then would lose the resume context.
func checkpointBindingIsFinal(assignment domain.Assignment, attempt domain.Attempt, manifest workerproto.ArtifactTransferManifest) bool {
	if assignment.ID == "" || assignment.Epoch != manifest.AssignmentEpoch ||
		(assignment.State != domain.AssignmentClaimed && assignment.State != domain.AssignmentCompleted) {
		return true
	}
	if attempt.ID == "" {
		return false
	}
	if attempt.CheckpointArtifactID != "" && attempt.CheckpointArtifactID != manifest.Objects[0].ID {
		return true
	}
	return assignment.State == domain.AssignmentCompleted
}

func checkpointImportBinding(records sqlite.CoordinatorRecords, manifest workerproto.ArtifactTransferManifest) (domain.Assignment, domain.Attempt, domain.Task, error) {
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
	validProgress := assignment.State == domain.AssignmentClaimed && attempt.Control == domain.ControlPaused ||
		assignment.State == domain.AssignmentCompleted && attempt.Progress.Terminal()
	if attempt.ID == "" || attempt.AssignmentID != assignment.ID || !validProgress ||
		attempt.CheckpointArtifactID != manifest.Objects[0].ID {
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
		if record.AttemptID != attemptID || record.Delivery != domain.ThrottleDeliveryAcknowledged ||
			record.Result != domain.ThrottleResultCheckpointed || record.Checkpoint == nil ||
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
