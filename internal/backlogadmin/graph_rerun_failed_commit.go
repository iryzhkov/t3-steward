package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// failedCommitRerun uses the same latest-attempt planner as ordinary reruns.
// Its provisional scope is never published: every failed ancestor must first
// provide the immutable record of each commit the subtree consumes.
func (s *Service) failedCommitRerun(ctx context.Context, records sqlite.CoordinatorRecords, run domain.WorkflowRun, from string) (domain.RerunScope, map[string]domain.Artifact, []domain.ReusedCommit, error) {
	latest := map[string]domain.Attempt{}
	for _, a := range records.Attempts {
		if a.WorkflowRunID == run.ID && a.Number > latest[a.TaskID].Number {
			latest[a.TaskID] = a
		}
	}
	options := domain.RerunCommitOptions{FailedAttempts: map[string]string{}}
	for id, a := range latest {
		if a.Progress == domain.ProgressFailed {
			options.FailedAttempts[id] = a.ID
		}
	}
	scope, err := domain.PlanRerun(run, records.Tasks, records.Attempts, from, options)
	if err != nil {
		return scope, nil, nil, err
	}
	retained := map[string]domain.Artifact{}
	var reused []domain.ReusedCommit
	for _, producer := range scope.Reuse {
		attempt := latest[producer.ID]
		if attempt.Progress == domain.ProgressSucceeded {
			continue
		}
		required := map[string]bool{}
		for _, consumer := range scope.Rerun {
			for _, name := range consumer.DependencyInputs[producer.Name] {
				if backlog.DeclaresCommit(producer, name) {
					required[name] = true
				}
			}
		}
		if len(required) == 0 {
			return domain.RerunScope{}, nil, nil, fmt.Errorf("cannot reuse failed task %s without a consumed retained commit", producer.Name)
		}
		names := make([]string, 0, len(required))
		for name := range required {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			var artifact domain.Artifact
			for _, candidate := range records.Artifacts {
				if candidate.WorkflowRunID == run.ID && candidate.TaskID == producer.ID && candidate.AttemptID == attempt.ID &&
					candidate.Kind == domain.ArtifactGitState && candidate.Name == backlog.FailedCommitArtifactName(name) {
					if artifact.ID != "" {
						return domain.RerunScope{}, nil, nil, fmt.Errorf("ambiguous retained failed commit %s/%s", producer.Name, name)
					}
					artifact = candidate
				}
			}
			if artifact.ID == "" {
				return domain.RerunScope{}, nil, nil, fmt.Errorf("missing retained failed commit %s/%s from attempt %s", producer.Name, name, attempt.ID)
			}
			opened, content, err := s.artifactOpen(ctx, artifact.ID)
			if err != nil {
				return domain.RerunScope{}, nil, nil, fmt.Errorf("retained failed commit %s/%s is unavailable: %w", producer.Name, name, err)
			}
			if content == nil {
				return domain.RerunScope{}, nil, nil, errors.New("retained failed commit has no content")
			}
			raw, readErr := io.ReadAll(io.LimitReader(content, 1<<20+1))
			closeErr := content.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				return domain.RerunScope{}, nil, nil, err
			}
			if len(raw) > 1<<20 || opened.ID != artifact.ID || opened.SHA256 != artifact.SHA256 || opened.Size != artifact.Size {
				return domain.RerunScope{}, nil, nil, fmt.Errorf("retained failed commit %s/%s content identity mismatch", producer.Name, name)
			}
			p, err := backlog.ParseCommitProvenance(raw)
			if err != nil {
				return domain.RerunScope{}, nil, nil, fmt.Errorf("retained failed commit %s/%s: %w", producer.Name, name, err)
			}
			if p.WorkflowRunID != run.ID || p.TaskID != producer.ID || p.Name != name || p.FailedAttempt == nil ||
				p.FailedAttempt.ID != attempt.ID || strings.Join(p.FailedAttempt.VerificationFailures, "; ") != attempt.Failure {
				return domain.RerunScope{}, nil, nil, fmt.Errorf("retained failed commit %s/%s does not match verification-only failure of attempt %s", producer.Name, name, attempt.ID)
			}
			if p.Commit != p.Base {
				if p.Bundle == nil || p.BundleOmitted != "" {
					return domain.RerunScope{}, nil, nil, fmt.Errorf("retained failed commit %s/%s has no cross-host bundle", producer.Name, name)
				}
				matches := 0
				for _, bundle := range records.Artifacts {
					if bundle.WorkflowRunID == run.ID && bundle.TaskID == producer.ID && bundle.AttemptID == attempt.ID && bundle.Kind == domain.ArtifactGitState && bundle.Name == p.Bundle.Artifact &&
						bundle.Size == p.Bundle.Size && bundle.SHA256 == p.Bundle.SHA256 {
						// reference verifies content custody now; this input is selected by the
						// existing M16-0 package path from the bound source attempt later.
						_, body, err := s.artifactOpen(ctx, bundle.ID)
						if err != nil {
							return domain.RerunScope{}, nil, nil, err
						}
						if body == nil {
							return domain.RerunScope{}, nil, nil, errors.New("retained failed commit bundle has no content")
						}
						_, readErr := io.Copy(io.Discard, body)
						closeErr := body.Close()
						if err := errors.Join(readErr, closeErr); err != nil {
							return domain.RerunScope{}, nil, nil, err
						}
						matches++
					}
				}
				if matches != 1 {
					return domain.RerunScope{}, nil, nil, fmt.Errorf("retained failed commit %s/%s has missing or ambiguous bundle", producer.Name, name)
				}
			}
			retained[producer.ID+"\x00"+name] = artifact
			reused = append(reused, domain.ReusedCommit{Producer: producer.Name, Name: name, Commit: p.Commit, SourceAttemptID: attempt.ID, VerificationFailures: slices.Clone(p.FailedAttempt.VerificationFailures)})
		}
	}
	return scope, retained, reused, nil
}
