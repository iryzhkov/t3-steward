package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ResultImportStore is the authoritative metadata and outcome boundary used
// after a worker has exposed one immutable upload manifest.
type ResultImportStore interface {
	ArtifactCatalog
	TurnOutcomeStore
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	CommitRecoveryRetry(context.Context, domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error)
}

// WorkerUploadOpener opens one exact object advertised by a worker upload.
// The caller closes every returned reader.
type WorkerUploadOpener interface {
	OpenWorkerUpload(context.Context, workerproto.ArtifactObject) (io.ReadCloser, error)
}

type CoordinatorResultImporter struct {
	CoordinatorID    string
	CoordinatorEpoch int64
	Store            ResultImportStore
	Artifacts        CoordinatorArtifactStore
	MaxArtifactBytes int64
	MaxTotalBytes    int64
	Now              func() time.Time
}

type ResultImportReport struct {
	Artifacts  []domain.Artifact
	Transition []domain.TurnOutcomeTransition
	Recovery   *domain.RecoveryRetryReceipt
}

// Import verifies custody, copies every object into coordinator ownership, and
// only then commits the terminal turn outcome. Exact replay reuses immutable
// artifact metadata and the original outcome transition.
func (i CoordinatorResultImporter) Import(ctx context.Context, response workerproto.ArtifactUploadResponse, opener WorkerUploadOpener) (ResultImportReport, error) {
	if i.Store == nil || opener == nil || i.CoordinatorID == "" || i.CoordinatorEpoch < 1 ||
		i.MaxArtifactBytes < 1 || i.MaxTotalBytes < i.MaxArtifactBytes {
		return ResultImportReport{}, errors.New("result import requires authority, storage, opener, and positive limits")
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
		return ResultImportReport{}, err
	}
	if manifest.Direction != "upload" || manifest.CoordinatorEpoch < 1 || manifest.CoordinatorEpoch > i.CoordinatorEpoch {
		return ResultImportReport{}, errors.New("result import manifest authority mismatch")
	}
	if err := validateWorkerUploadCustody(response, i.CoordinatorID); err != nil {
		return ResultImportReport{}, err
	}
	records, err := i.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return ResultImportReport{}, err
	}
	assignment, attempt, task, err := resultImportBinding(records, manifest)
	if err != nil {
		if errors.Is(err, errResultImportTaskMissing) {
			return i.rejectResult(ctx, ResultImportReport{}, stableCoordinatorID("outcome", manifest.ID),
				attempt, manifest.CreatedAt, now, err)
		}
		return ResultImportReport{}, err
	}
	outcomeID := stableCoordinatorID("outcome", manifest.ID)
	if attempt.Progress.Terminal() && attempt.LastTurnOutcomeID != outcomeID {
		return ResultImportReport{}, fmt.Errorf("%w: attempt %q already has a different terminal outcome", ErrResultImportSuperseded, attempt.ID)
	}
	if err := i.refuseWhileWaiting(ctx, attempt, outcomeID, now); err != nil {
		return ResultImportReport{}, err
	}

	report := ResultImportReport{}
	outputs := make(map[string]string)
	artifacts := make([]domain.Artifact, 0, len(manifest.Objects))
	var summaries, logs, preflightLogs, verifications int
	for _, object := range manifest.Objects {
		artifact, err := resultArtifact(object, manifest, attempt, task, manifest.CreatedAt)
		if err != nil {
			// A result whose objects violate the contract will violate it on every
			// retry, so returning a plain error made the worker's whole
			// reconciliation pass fail forever and took unrelated results on that
			// worker down with it.
			return i.rejectResult(ctx, report, outcomeID, attempt, manifest.CreatedAt, now, err)
		}
		artifacts = append(artifacts, artifact)
		if artifact.Kind == domain.ArtifactOutput {
			if _, duplicate := outputs[artifact.Name]; duplicate {
				return report, fmt.Errorf("result import repeats output %q", artifact.Name)
			}
			outputs[artifact.Name] = artifact.MediaType
		}
		switch artifact.Kind {
		case domain.ArtifactSummary:
			summaries++
		case domain.ArtifactLog:
			// The thread archive is the one required log. Preflight evidence is
			// also a log but is optional and unbounded in count, because a task
			// declares how many steps it runs, so the two are counted apart.
			if strings.HasPrefix(artifact.ID, "preflight-") {
				preflightLogs++
			} else {
				logs++
			}
		case domain.ArtifactVerification:
			verifications++
		}
	}
	missingOutputs, err := validateDeclaredResultOutputs(task, outputs)
	if err != nil {
		return report, err
	}
	if summaries != 1 || logs != 1 || verifications > len(task.Verification) {
		return i.rejectResult(ctx, report, outcomeID, attempt, manifest.CreatedAt, now,
			fmt.Errorf("evidence counts summary=%d log=%d verification=%d (preflight logs %d), want 1, 1, at most %d",
				summaries, logs, verifications, preflightLogs, len(task.Verification)))
	}
	payloads := make([][]byte, len(manifest.Objects))
	for index, object := range manifest.Objects {
		reader, err := opener.OpenWorkerUpload(ctx, object)
		if err != nil {
			return report, fmt.Errorf("open worker upload %q: %w", object.ID, err)
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, object.Size+1))
		closeErr := reader.Close()
		if readErr != nil {
			return report, fmt.Errorf("read worker upload %q: %w", object.ID, readErr)
		}
		if closeErr != nil {
			return report, closeErr
		}
		if int64(len(data)) != object.Size {
			return report, fmt.Errorf("worker upload %q size changed", object.ID)
		}
		if err := workerproto.VerifyArtifact(bytes.NewReader(data), object, i.MaxArtifactBytes); err != nil {
			return report, fmt.Errorf("verify worker upload %q: %w", object.ID, err)
		}
		payloads[index] = data
	}
	proposal, err := recoveryProposalFromResult(artifacts, payloads, attempt, assignment)
	if err != nil {
		return report, err
	}
	verificationPassed, failure, summary, err := evaluateResultEvidence(task, assignment.ThreadID, artifacts, payloads, missingOutputs)
	if err != nil {
		return report, err
	}
	for index, artifact := range artifacts {
		published, err := i.Artifacts.Publish(ctx, domain.ArtifactPublication{
			CoordinatorEpoch: i.CoordinatorEpoch, WorkerID: manifest.WorkerID, WorkerEpoch: manifest.WorkerEpoch,
			AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			AttemptRevision: attempt.Revision, Artifact: artifact,
		}, bytes.NewReader(payloads[index]))
		if errors.Is(err, sqlite.ErrArtifactConflict) {
			// The artifact already exists with different immutable metadata, so
			// this result names an identity that is not its own. No retry can
			// resolve that, and leaving it retryable blocks every other result
			// the worker holds.
			return i.rejectResult(ctx, report, outcomeID, attempt, manifest.CreatedAt, now,
				fmt.Errorf("artifact %q: %w", artifact.ID, err))
		}
		if err != nil {
			return report, err
		}
		report.Artifacts = append(report.Artifacts, published)
	}
	if proposal != nil {
		request, requestErr := proposal.RetryRequest()
		if requestErr != nil {
			return report, requestErr
		}
		for _, artifact := range report.Artifacts {
			if artifact.Name == "recovery/proposal.json" {
				request.ProposalReceipt = domain.RecoveryProposalReceipt{
					ProposalArtifact:    domain.ArtifactDigest{ArtifactID: artifact.ID, Digest: artifact.SHA256},
					ActivationAttemptID: attempt.ID,
					AssignmentID:        assignment.ID, AssignmentEpoch: assignment.Epoch,
				}
				break
			}
		}
		receipt, commitErr := i.Store.CommitRecoveryRetry(ctx, request)
		if commitErr != nil {
			return report, fmt.Errorf("result import recovery proposal: %w", commitErr)
		}
		report.Recovery = &receipt
	}
	report.Transition, err = ReconcileTurnOutcomes(ctx, i.Store, []domain.TurnOutcome{{
		ID: outcomeID, AttemptID: attempt.ID,
		Marker: domain.TurnOutcomeDone, VerificationPassed: verificationPassed, Failure: failure,
		FinalSummaryArtifactID: summary.ID, ObservedAt: manifest.CreatedAt,
	}}, now)
	return report, err
}

// refuseWhileWaiting stops a worker result before any artifact enters
// coordinator custody when the attempt is parked on a live task-bound wait.
//
// Waiting until the turn outcome is committed would be too late: by then the
// outputs are published, and published artifacts are immutable. Collection
// happens after the turn that ends with nothing parking the attempt, and the
// outputs written after the wake are the outputs collected.
//
// This refusal is about a live wait, which is a narrower question than whether
// the attempt is parked: a wait that has settled but whose wake has not reached
// the thread still parks it. Keeping the worker from collecting in that window
// is the coordinator's parked statement, not this check.
func (i CoordinatorResultImporter) refuseWhileWaiting(ctx context.Context, attempt domain.Attempt, outcomeID string, now time.Time) error {
	reader, ok := i.Store.(TaskWaitReader)
	if !ok {
		return nil
	}
	live, err := reader.LiveTaskWaitAttempts(ctx)
	if err != nil {
		return fmt.Errorf("load live task waits: %w", err)
	}
	waitID, waiting := live[attempt.ID]
	if !waiting {
		return nil
	}
	refusal := domain.TaskWaitReconciliation{
		ID:        stableCoordinatorID("reconciliation", outcomeID),
		Kind:      domain.TaskWaitReconciliationDoneWhileWaiting,
		AttemptID: attempt.ID,
		WaitID:    waitID,
		ThreadID:  attempt.ThreadID,
		Detail: fmt.Sprintf("worker result for attempt %q arrived while task-bound wait %q was live; refused before collection",
			attempt.ID, waitID),
		ObservedAt: now,
	}
	if err := reader.RecordTaskWaitReconciliations(ctx, []domain.TaskWaitReconciliation{refusal}); err != nil {
		return fmt.Errorf("record task wait reconciliation: %w", err)
	}
	return fmt.Errorf("%w: %s", ErrTurnOutcomeWaiting, refusal.Detail)
}

func validateWorkerUploadCustody(response workerproto.ArtifactUploadResponse, coordinatorID string) error {
	manifest := response.Manifest
	if len(response.Custody) != len(manifest.Objects) {
		return errors.New("result import custody length mismatch")
	}
	previous := ""
	for index, record := range response.Custody {
		object := manifest.Objects[index]
		if err := workerproto.ValidateCustodyRecord(record); err != nil {
			return err
		}
		if record.ManifestID != manifest.ID || record.ObjectID != object.ID || record.Sequence != int64(index+1) ||
			record.From != "worker:"+manifest.WorkerID || record.To != "outbox:"+coordinatorID ||
			record.Size != object.Size || !strings.EqualFold(record.SHA256, object.SHA256) || record.PreviousSHA256 != previous {
			return errors.New("result import custody chain mismatch")
		}
		previous = record.RecordSHA256
	}
	return nil
}

func resultImportBinding(records sqlite.CoordinatorRecords, manifest workerproto.ArtifactTransferManifest) (domain.Assignment, domain.Attempt, domain.Task, error) {
	var assignment domain.Assignment
	for _, candidate := range records.Assignments {
		if candidate.ID == manifest.AssignmentID {
			assignment = candidate
			break
		}
	}
	if assignment.ID == "" || assignment.Epoch != manifest.AssignmentEpoch || assignment.WorkerID != manifest.WorkerID ||
		assignment.WorkerEpoch != manifest.WorkerEpoch || assignment.State != domain.AssignmentCompleted {
		return assignment, domain.Attempt{}, domain.Task{}, errors.New("result import assignment binding is stale")
	}
	var attempt domain.Attempt
	for _, candidate := range records.Attempts {
		if candidate.ID == assignment.AttemptID {
			attempt = candidate
			break
		}
	}
	if attempt.ID == "" || attempt.AssignmentID != assignment.ID ||
		(!attempt.Progress.Terminal() && attempt.Progress != domain.ProgressVerifying) {
		return assignment, attempt, domain.Task{}, fmt.Errorf("result import attempt binding is stale: assignment=%q attempt=%q progress=%q", attempt.AssignmentID, attempt.ID, attempt.Progress)
	}
	if attempt.IsSupervisionActivation() {
		// An overseer activation is dispatched as ordinary assigned work and
		// therefore returns an ordinary result, but it is deliberately not a node
		// of the run's graph, so there is no declared task to bind it to. Its
		// contract is the decision protocol rather than declared outputs and
		// verification commands: what it may return is a final message and a
		// thread archive, which the object identities below already constrain.
		//
		// Looking for a declared task here is what jammed a worker: the lookup
		// failed on every pass, and because the coordinator reconciles a worker
		// in one pass, every unrelated result that worker held stayed stuck
		// behind it.
		return assignment, attempt, domain.Task{ID: attempt.TaskID, WorkflowID: attempt.WorkflowRunID}, nil
	}
	task, _ := domain.TaskForAttempt(attempt, records.WorkflowRuns, records.Tasks)
	if task.ID == "" {
		return assignment, attempt, task, fmt.Errorf("%w: attempt %q names task %q", errResultImportTaskMissing, attempt.ID, attempt.TaskID)
	}
	return assignment, attempt, task, nil
}

// errResultImportTaskMissing marks a result whose attempt names a task the
// coordinator does not have. Task deletion is unsupported, so this cannot
// resolve by waiting and the result is dead-lettered rather than retried.
var errResultImportTaskMissing = errors.New("result import task is missing")

// rejectResult settles an attempt whose result can never be imported.
//
// Discarding the result is not enough on its own. The attempt reached verifying
// when the worker produced a result, and with that result thrown away it can
// neither settle nor be retried, because retry is invalid from verifying. It was
// stranded, and an operator's only move was to cancel it by hand.
//
// The rejection is therefore recorded as the attempt's terminal outcome, with
// the reason, so the run can proceed and the failure is durable state rather
// than a line in a log. This is the dead-letter record: the work is not lost
// silently, it is lost visibly.
func (i CoordinatorResultImporter) rejectResult(
	ctx context.Context,
	report ResultImportReport,
	outcomeID string,
	attempt domain.Attempt,
	observedAt time.Time,
	now time.Time,
	cause error,
) (ResultImportReport, error) {
	rejection := fmt.Errorf("%w: %w", ErrResultImportRejected, cause)
	transition, err := ReconcileTurnOutcomes(ctx, i.Store, []domain.TurnOutcome{{
		ID: outcomeID, AttemptID: attempt.ID, Marker: domain.TurnOutcomeDone,
		VerificationPassed: false, Failure: rejection.Error(), ObservedAt: observedAt,
	}}, now)
	if err != nil {
		// Report the settlement failure rather than the rejection: the caller
		// must not acknowledge a result whose attempt is still unsettled, or the
		// attempt would be stranded exactly as before.
		return report, fmt.Errorf("settle rejected result for attempt %q: %w", attempt.ID, err)
	}
	report.Transition = transition
	return report, rejection
}

// ErrResultImportRejected marks a result that can never be imported, because its
// contents violate the import contract rather than arriving at a bad moment.
//
// It exists so that such a result is discarded once instead of retried forever.
// One malformed result must not stop unrelated work: the coordinator reconciles
// a worker in a single pass, so an error that aborts the pass blocks every other
// result that worker is holding.
var ErrResultImportRejected = errors.New("result import rejected")

func resultArtifact(object workerproto.ArtifactObject, manifest workerproto.ArtifactTransferManifest, attempt domain.Attempt, task domain.Task, now time.Time) (domain.Artifact, error) {
	if err := workerproto.ValidateArtifactObject(object, object.Size+1); err != nil {
		return domain.Artifact{}, err
	}
	name := strings.TrimPrefix(object.Path, "results/")
	if name == object.Path || name == "" || path.Clean(name) != name {
		return domain.Artifact{}, errors.New("result import object path is invalid")
	}
	kind := domain.ArtifactKind(object.Kind)
	switch kind {
	case domain.ArtifactOutput:
	case domain.ArtifactVerification, domain.ArtifactSummary, domain.ArtifactLog:
	case domain.ArtifactInput, domain.ArtifactCheckpoint:
		if !attempt.IsSupervisionActivation() || !strings.HasPrefix(name, "recovery/") {
			return domain.Artifact{}, fmt.Errorf("result import object %q cannot publish recovery content", object.ID)
		}
	default:
		return domain.Artifact{}, fmt.Errorf("result import object %q has invalid kind %q", object.ID, object.Kind)
	}
	if kind == domain.ArtifactSummary {
		if object.ID != "final-message-"+attempt.ID || name != "final-message.md" || object.MediaType != "text/markdown" {
			return domain.Artifact{}, errors.New("result import final summary identity mismatch")
		}
	}
	if kind == domain.ArtifactLog {
		// A log is either the thread archive or preflight evidence. Both have a
		// fixed identity so that an attempt cannot smuggle arbitrary content in
		// under a permissive kind, but they are different identities: preflight
		// runs before the session exists and cannot be the thread archive.
		archive := object.ID == "thread-archive-"+attempt.ID && name == "thread.json" && object.MediaType == "application/json"
		preflight := strings.HasPrefix(object.ID, "preflight-") &&
			strings.HasPrefix(name, "preflight/") &&
			strings.HasPrefix(object.MediaType, "text/plain")
		if !archive && !preflight {
			return domain.Artifact{}, fmt.Errorf("result import log identity mismatch: id=%q name=%q mediaType=%q", object.ID, name, object.MediaType)
		}
	}
	return domain.Artifact{ID: object.ID, WorkflowRunID: attempt.WorkflowRunID, TaskID: task.ID, AttemptID: attempt.ID,
		Kind: kind, Name: name, MediaType: object.MediaType, Size: object.Size, SHA256: strings.ToLower(object.SHA256),
		Producer: "worker:" + manifest.WorkerID, CreatedAt: now}, nil
}

func validateDeclaredResultOutputs(task domain.Task, outputs map[string]string) ([]string, error) {
	want := make(map[string]string, len(task.Outputs))
	for _, output := range task.Outputs {
		name := path.Clean(output.Name)
		if _, duplicate := want[name]; duplicate {
			return nil, fmt.Errorf("result import declarations repeat output %q", name)
		}
		want[name] = output.MediaType
	}
	for name, mediaType := range outputs {
		declaredMediaType, exists := want[name]
		if !exists {
			return nil, fmt.Errorf("result import contains undeclared output %q", name)
		}
		if declaredMediaType != "" && mediaType != declaredMediaType {
			return nil, fmt.Errorf("result import output %q media type %q does not match declaration %q", name, mediaType, declaredMediaType)
		}
	}
	missing := make([]string, 0, len(want)-len(outputs))
	for name := range want {
		if _, exists := outputs[name]; !exists {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	return missing, nil
}

func recoveryProposalFromResult(artifacts []domain.Artifact, payloads [][]byte, attempt domain.Attempt, assignment domain.Assignment) (*domain.RecoveryProposal, error) {
	var proposal *domain.RecoveryProposal
	byID := make(map[string]domain.Artifact)
	recoveryArtifacts := 0
	for index, artifact := range artifacts {
		byID[artifact.ID] = artifact
		if !strings.HasPrefix(artifact.Name, "recovery/") {
			continue
		}
		recoveryArtifacts++
		if artifact.Name != "recovery/proposal.json" {
			continue
		}
		if proposal != nil || artifact.Kind != domain.ArtifactInput || artifact.MediaType != "application/json" ||
			artifact.ID != "recovery-proposal-"+attempt.ID {
			return nil, errors.New("result import recovery proposal identity mismatch")
		}
		var decoded domain.RecoveryProposal
		decoder := json.NewDecoder(bytes.NewReader(payloads[index]))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&decoded); err != nil {
			return nil, fmt.Errorf("result import recovery proposal: %w", err)
		}
		var extra json.RawMessage
		if err := decoder.Decode(&extra); err != io.EOF {
			return nil, errors.New("result import recovery proposal has trailing content")
		}
		proposal = &decoded
	}
	if recoveryArtifacts == 0 {
		return nil, nil
	}
	if proposal == nil || !attempt.IsSupervisionActivation() {
		return nil, errors.New("result import recovery artifacts need a typed activation proposal")
	}
	request, err := proposal.RetryRequest()
	if err != nil {
		return nil, err
	}
	if proposal.RunID != attempt.WorkflowRunID || proposal.ActivationID != attempt.SupervisionActivationID ||
		proposal.ActivationEpoch != attempt.SupervisionActivationEpoch || proposal.AssignmentID != assignment.ID ||
		proposal.AssignmentEpoch != assignment.Epoch || proposal.GraphRevision < 1 {
		return nil, errors.New("result import recovery proposal authority mismatch")
	}
	instruction, ok := byID[request.InstructionArtifact.ArtifactID]
	if !ok || instruction.Name != "recovery/instructions.md" || instruction.Kind != domain.ArtifactInput ||
		instruction.MediaType != "text/markdown" || instruction.SHA256 != request.InstructionArtifact.Digest {
		return nil, errors.New("result import recovery instruction identity mismatch")
	}
	wantCount := 2 + len(request.CheckpointArtifacts)
	for _, digest := range request.CheckpointArtifacts {
		checkpoint, ok := byID[digest.ArtifactID]
		if !ok || checkpoint.Name != "recovery/checkpoint.tar" || checkpoint.Kind != domain.ArtifactCheckpoint ||
			checkpoint.MediaType != "application/x-tar" || checkpoint.SHA256 != digest.Digest {
			return nil, errors.New("result import recovery checkpoint identity mismatch")
		}
	}
	if recoveryArtifacts != wantCount {
		return nil, errors.New("result import recovery proposal contains undeclared payload")
	}
	return proposal, nil
}

func evaluateResultEvidence(task domain.Task, threadID string, artifacts []domain.Artifact, payloads [][]byte, missingOutputs []string) (bool, string, domain.Artifact, error) {
	var summary domain.Artifact
	var archive []byte
	reports := make(map[string][]byte, len(task.Verification))
	for index, artifact := range artifacts {
		switch artifact.Kind {
		case domain.ArtifactSummary:
			summary = artifact
		case domain.ArtifactVerification:
			if artifact.MediaType != "application/json" {
				return false, "", summary, fmt.Errorf("result import verification %q has media type %q", artifact.Name, artifact.MediaType)
			}
			reports[artifact.Name] = payloads[index]
		case domain.ArtifactLog:
			// Only the thread archive is the session transcript. Preflight logs
			// share the kind but are ordinary text, so taking whichever log came
			// last made the archive parse fail on a preflight log's first byte.
			if !strings.HasPrefix(artifact.ID, "preflight-") {
				archive = payloads[index]
			}
		}
	}
	failures := make([]string, 0, 2)
	reason, err := ResultCompletionFailure(archive, threadID, string(payloads[summaryIndex(artifacts)]))
	if err != nil {
		return false, "", summary, err
	}
	if reason != "" {
		failures = append(failures, reason)
	}
	for index, command := range task.Verification {
		name := fmt.Sprintf("verification/%03d.json", index+1)
		raw, exists := reports[name]
		if !exists {
			if index == len(reports) {
				failures = append(failures, fmt.Sprintf("missing verification evidence: %s", name))
				break
			}
			return false, "", summary, fmt.Errorf("result import verification evidence is not contiguous at %q", name)
		}
		var report VerificationReport
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&report); err != nil {
			return false, "", summary, fmt.Errorf("result import verification %q: %w", name, err)
		}
		var extra json.RawMessage
		if err := decoder.Decode(&extra); err != io.EOF {
			return false, "", summary, fmt.Errorf("result import verification %q has trailing content", name)
		}
		if report.Command != command || report.StartedAt.IsZero() || report.CompletedAt.Before(report.StartedAt) {
			return false, "", summary, fmt.Errorf("result import verification %q identity or timing mismatch", name)
		}
		if report.ExitCode != 0 {
			if len(reports) != index+1 {
				return false, "", summary, fmt.Errorf("result import contains evidence after failed verification %q", name)
			}
			failures = append(failures, fmt.Sprintf("verification command failed (%d): %s", report.ExitCode, command))
			break
		}
	}
	if len(missingOutputs) != 0 {
		failures = append(failures, "missing declared output: "+strings.Join(missingOutputs, ", "))
	}
	return len(failures) == 0, strings.Join(failures, "; "), summary, nil
}

// BacklogFailedMarker is the final-message line a worker writes when it fails
// an attempt itself (preparation, dispatch, or recovery failure). The lines
// after it are the reason.
const BacklogFailedMarker = "BACKLOG STATUS: failed"

func backlogFailedReason(message string) (string, bool) {
	lines := strings.Split(message, "\n")
	for index, line := range lines {
		if !strings.EqualFold(strings.TrimSpace(line), BacklogFailedMarker) {
			continue
		}
		reason := strings.TrimSpace(strings.Join(lines[index+1:], "\n"))
		if reason == "" {
			reason = "worker reported a failed attempt"
		}
		return reason, true
	}
	return "", false
}

func summaryIndex(artifacts []domain.Artifact) int {
	for index, artifact := range artifacts {
		if artifact.Kind == domain.ArtifactSummary {
			return index
		}
	}
	return 0
}
