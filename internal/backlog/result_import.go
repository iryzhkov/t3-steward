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
		return ResultImportReport{}, err
	}
	outcomeID := stableCoordinatorID("outcome", manifest.ID)
	if attempt.Progress.Terminal() && attempt.LastTurnOutcomeID != outcomeID {
		return ResultImportReport{}, errors.New("result import attempt already has a different terminal outcome")
	}

	report := ResultImportReport{}
	outputs := make(map[string]string)
	artifacts := make([]domain.Artifact, 0, len(manifest.Objects))
	var summaries, logs, verifications int
	for _, object := range manifest.Objects {
		artifact, err := resultArtifact(object, manifest, attempt, task, manifest.CreatedAt)
		if err != nil {
			return report, err
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
			logs++
		case domain.ArtifactVerification:
			verifications++
		}
	}
	missingOutputs, err := validateDeclaredResultOutputs(task, outputs)
	if err != nil {
		return report, err
	}
	if summaries != 1 || logs != 1 || verifications > len(task.Verification) {
		return report, fmt.Errorf("result import evidence counts summary=%d log=%d verification=%d, want 1, 1, at most %d", summaries, logs, verifications, len(task.Verification))
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
	verificationPassed, failure, summary, err := evaluateResultEvidence(task, artifacts, payloads, missingOutputs)
	if err != nil {
		return report, err
	}
	for index, artifact := range artifacts {
		published, err := i.Artifacts.Publish(ctx, domain.ArtifactPublication{
			CoordinatorEpoch: i.CoordinatorEpoch, WorkerID: manifest.WorkerID, WorkerEpoch: manifest.WorkerEpoch,
			AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			AttemptRevision: attempt.Revision, Artifact: artifact,
		}, bytes.NewReader(payloads[index]))
		if err != nil {
			return report, err
		}
		report.Artifacts = append(report.Artifacts, published)
	}
	report.Transition, err = ReconcileTurnOutcomes(ctx, i.Store, []domain.TurnOutcome{{
		ID: outcomeID, AttemptID: attempt.ID,
		Marker: domain.TurnOutcomeDone, VerificationPassed: verificationPassed, Failure: failure,
		FinalSummaryArtifactID: summary.ID, ObservedAt: manifest.CreatedAt,
	}}, now)
	return report, err
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
	var task domain.Task
	for _, candidate := range records.Tasks {
		if candidate.ID == attempt.TaskID {
			task = candidate
			break
		}
	}
	if task.ID == "" {
		return assignment, attempt, task, errors.New("result import task is missing")
	}
	return assignment, attempt, task, nil
}

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
	default:
		return domain.Artifact{}, fmt.Errorf("result import object %q has invalid kind %q", object.ID, object.Kind)
	}
	if kind == domain.ArtifactSummary {
		if object.ID != "final-message-"+attempt.ID || name != "final-message.md" || object.MediaType != "text/markdown" {
			return domain.Artifact{}, errors.New("result import final summary identity mismatch")
		}
	}
	if kind == domain.ArtifactLog && (object.ID != "thread-archive-"+attempt.ID || name != "thread.json" || object.MediaType != "application/json") {
		return domain.Artifact{}, errors.New("result import thread archive identity mismatch")
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

func evaluateResultEvidence(task domain.Task, artifacts []domain.Artifact, payloads [][]byte, missingOutputs []string) (bool, string, domain.Artifact, error) {
	var summary domain.Artifact
	var summaryDone bool
	reports := make(map[string][]byte, len(task.Verification))
	for index, artifact := range artifacts {
		switch artifact.Kind {
		case domain.ArtifactSummary:
			summary = artifact
			summaryDone = hasBacklogDoneMarker(string(payloads[index]))
		case domain.ArtifactVerification:
			if artifact.MediaType != "application/json" {
				return false, "", summary, fmt.Errorf("result import verification %q has media type %q", artifact.Name, artifact.MediaType)
			}
			reports[artifact.Name] = payloads[index]
		case domain.ArtifactLog:
			if !json.Valid(payloads[index]) {
				return false, "", summary, errors.New("result import thread archive is not valid JSON")
			}
		}
	}
	failures := make([]string, 0, 2)
	if !summaryDone {
		failures = append(failures, "final summary has no done marker")
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

func hasBacklogDoneMarker(message string) bool {
	for _, line := range strings.Split(message, "\n") {
		if strings.EqualFold(strings.TrimSpace(line), "BACKLOG STATUS: done") {
			return true
		}
	}
	return false
}
