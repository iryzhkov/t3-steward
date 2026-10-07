package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type CommitExportRequest struct {
	RunID  string `json:"runId"`
	TaskID string `json:"taskId"`
	Name   string `json:"name"`
	Branch string `json:"branch"`
}
type CommitExportTransport interface {
	ExportCommit(context.Context, CommitExportRequest) (ArtifactContent, error)
}
type CommitBundleOpenFunc func(context.Context, backlog.CommitProvenance, domain.Artifact) (io.ReadCloser, error)

func (s *Service) SetCommitBundleOpener(open CommitBundleOpenFunc) { s.commitBundleOpen = open }

// ExportCommit reads an authenticated declared output, never changes producer refs.
func (s *Service) ExportCommit(ctx context.Context, principal Principal, request CommitExportRequest) (ArtifactContent, error) {
	for _, value := range []string{request.RunID, request.TaskID, request.Name} {
		if value == "" || strings.TrimSpace(value) != value {
			return ArtifactContent{}, fmt.Errorf("%w: run, task and declared commit name required", ErrInvalidQuery)
		}
	}
	if err := backlog.ValidateExportBranch(request.Branch); err != nil {
		return ArtifactContent{}, err
	}
	if err := s.authorizer.Authorize(ctx, principal, Action{Kind: QueryTask, WorkflowRunID: request.RunID, TaskID: request.TaskID}); err != nil {
		return ArtifactContent{}, fmt.Errorf("authorize commit export: %w", err)
	}
	records, err := s.reader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return ArtifactContent{}, err
	}
	var run domain.WorkflowRun
	for _, r := range records.WorkflowRuns {
		if r.ID == request.RunID {
			run = r
			break
		}
	}
	if run.ID == "" {
		return ArtifactContent{}, fmt.Errorf("%w: run %s", ErrNotFound, request.RunID)
	}
	var task domain.Task
	for _, t := range records.Tasks {
		if t.WorkflowID == run.WorkflowID && (t.ID == request.TaskID || t.Name == request.TaskID) {
			if task.ID != "" {
				return ArtifactContent{}, errors.New("ambiguous commit export task")
			}
			task = t
		}
	}
	if task.ID == "" {
		return ArtifactContent{}, fmt.Errorf("%w: task %s", ErrNotFound, request.TaskID)
	}
	declared := false
	for _, output := range task.Outputs {
		if output.Name == request.Name && output.Commit != nil {
			declared = true
		}
	}
	if !declared {
		return ArtifactContent{}, errors.New("commit export: task does not declare this commit")
	}
	var attempt domain.Attempt
	for _, a := range records.Attempts {
		if a.WorkflowRunID == run.ID && a.TaskID == task.ID && (attempt.ID == "" || a.Number > attempt.Number) {
			attempt = a
		}
	}
	if attempt.ID == "" {
		return ArtifactContent{}, errors.New("commit export: producing attempt is unavailable")
	}
	var artifact domain.Artifact
	for _, a := range records.Artifacts {
		if a.WorkflowRunID == run.ID && a.TaskID == task.ID && a.AttemptID == attempt.ID && a.Kind == domain.ArtifactOutput && a.Name == request.Name {
			if artifact.ID != "" {
				return ArtifactContent{}, errors.New("ambiguous commit export output")
			}
			artifact = a
		}
	}
	if artifact.ID == "" || s.artifactOpen == nil {
		return ArtifactContent{}, ErrArtifactContentUnavailable
	}
	if err := s.authorizer.Authorize(ctx, principal, Action{Kind: QueryArtifact, ArtifactID: artifact.ID}); err != nil {
		return ArtifactContent{}, err
	}
	_, record, err := s.artifactOpen(ctx, artifact.ID)
	if err != nil {
		return ArtifactContent{}, err
	}
	if record == nil {
		return ArtifactContent{}, ErrArtifactContentUnavailable
	}
	raw, readErr := io.ReadAll(io.LimitReader(record, 1<<20+1))
	closeErr := record.Close()
	if readErr != nil {
		return ArtifactContent{}, readErr
	}
	if closeErr != nil {
		return ArtifactContent{}, closeErr
	}
	if len(raw) > 1<<20 {
		return ArtifactContent{}, errors.New("commit export: provenance exceeds limit")
	}
	provenance, err := backlog.ParseCommitProvenance(raw)
	if err != nil {
		return ArtifactContent{}, err
	}
	if provenance.WorkflowRunID != run.ID || provenance.TaskID != task.ID || provenance.Name != request.Name {
		return ArtifactContent{}, errors.New("commit export: provenance producer mismatch")
	}
	// A review-declared task's commit is staged work until the coordinator
	// accepts the attempt that staged it, the same acceptance that lets a
	// consumer publish it, so only that accepted attempt's commit is exported.
	if provenance.StagedAttempt != "" || task.ReviewRequirements != nil {
		if provenance.StagedAttempt != attempt.ID {
			return ArtifactContent{}, fmt.Errorf("commit export: the provenance record names the staging of attempt %q, not of attempt %s that declared it",
				provenance.StagedAttempt, attempt.ID)
		}
		if attempt.Progress != domain.ProgressSucceeded {
			return ArtifactContent{}, fmt.Errorf("commit export: attempt %s of review-declared task %s is %s, and its staged commit is exported only once the coordinator accepted it",
				attempt.ID, task.Name, attempt.Progress)
		}
	}
	var source io.ReadCloser
	verificationProvenance := provenance
	if provenance.Bundle != nil {
		for _, a := range records.Artifacts {
			if a.WorkflowRunID == run.ID && a.TaskID == task.ID && a.AttemptID == artifact.AttemptID && a.Name == provenance.Bundle.Artifact && a.Kind == domain.ArtifactGitState {
				if a.Size != provenance.Bundle.Size || a.SHA256 != provenance.Bundle.SHA256 {
					return ArtifactContent{}, errors.New("commit export: bundle metadata disagrees with provenance")
				}
				if source != nil {
					_ = source.Close()
					return ArtifactContent{}, errors.New("commit export: ambiguous retained bundle")
				}
				var opened domain.Artifact
				opened, source, err = s.artifactOpen(ctx, a.ID)
				if err == nil && (opened.ID != a.ID || opened.Size != a.Size || opened.SHA256 != a.SHA256) {
					if source != nil {
						_ = source.Close()
					}
					return ArtifactContent{}, errors.New("commit export: opened bundle metadata changed")
				}
				if err != nil {
					if source != nil {
						_ = source.Close()
						source = nil
					}
					if !errors.Is(err, os.ErrNotExist) {
						return ArtifactContent{}, err
					}
				}
			}
		}
	}
	if source == nil {
		if s.commitBundleOpen == nil {
			return ArtifactContent{}, errors.New("commit export: retained bundle unavailable and producing worker cannot be reached")
		}
		verificationProvenance.Bundle = nil
		source, err = s.commitBundleOpen(ctx, provenance, artifact)
		if err != nil {
			return ArtifactContent{}, err
		}
	}
	if source == nil {
		return ArtifactContent{}, errors.New("commit export: producing worker returned no bundle")
	}
	exported, exportErr := backlog.ExportCommitBundle(ctx, verificationProvenance, request.Branch, source, backlog.DefaultCommitBundleMaxBytes)
	closeErr = source.Close()
	if err := errors.Join(exportErr, closeErr); err != nil {
		if exportErr == nil {
			_ = exported.Close()
		}
		return ArtifactContent{}, err
	}
	file, err := os.Open(exported.Path)
	if err != nil {
		_ = exported.Close()
		return ArtifactContent{}, err
	}
	return ArtifactContent{Provenance: &provenance, Metadata: ArtifactMetadata{ID: artifact.ID, Name: request.Branch + ".bundle", MediaType: backlog.CommitBundleMediaType, Size: exported.Size, SHA256: exported.SHA256}, Content: &exportReadCloser{ReadCloser: file, cleanup: exported.Close}}, nil
}

type exportReadCloser struct {
	io.ReadCloser
	cleanup func() error
}

func (r *exportReadCloser) Close() error { return errors.Join(r.ReadCloser.Close(), r.cleanup()) }

func (c LocalClient) ExportCommit(ctx context.Context, request CommitExportRequest) (ArtifactContent, error) {
	response, content, err := c.exchange(ctx, localRequest{Version: LocalTransportVersion, Operation: localOperationCommitExport, CommitExport: &request}, nil)
	if err != nil {
		return ArtifactContent{}, err
	}
	if err := c.validate(localOperationCommitExport, response); err != nil {
		if content != nil {
			_ = content.Close()
		}
		return ArtifactContent{}, err
	}
	if content == nil || response.ArtifactMetadata == nil || response.CommitProvenance == nil {
		if content != nil {
			_ = content.Close()
		}
		return ArtifactContent{}, c.fail(ClassProtocol, localOperationCommitExport, errors.New("missing commit export metadata"))
	}
	return ArtifactContent{Metadata: *response.ArtifactMetadata, Content: content, Provenance: response.CommitProvenance}, nil
}
func (c *SSHClient) ExportCommit(ctx context.Context, request CommitExportRequest) (ArtifactContent, error) {
	response, exchange, err := c.roundTrip(ctx, localRequest{Version: LocalTransportVersion, Operation: localOperationCommitExport, CommitExport: &request}, nil, true)
	if err != nil {
		return ArtifactContent{}, err
	}
	if response.ArtifactMetadata == nil || response.CommitProvenance == nil || response.ArtifactSize < 0 || response.ArtifactSize != response.ArtifactMetadata.Size || response.ArtifactSize > c.config.MaxArtifactBytes {
		_ = exchange.Close()
		return ArtifactContent{}, c.fail(ClassProtocol, localOperationCommitExport, errors.New("invalid coordinator commit export response"))
	}
	return ArtifactContent{Metadata: *response.ArtifactMetadata, Provenance: response.CommitProvenance, Content: &exactReadCloser{reader: exchange.stdout, closer: exchange, remaining: response.ArtifactSize}}, nil
}
