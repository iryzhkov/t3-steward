package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// FailedCommitAttempt marks a retained candidate that was never published as output.
type FailedCommitAttempt struct {
	ID                   string   `json:"id"`
	VerificationFailures []string `json:"verificationFailures"`
}

func FailedCampaignRef(run, task, attempt, name string) string {
	return "refs/campaigns-quarantine/" + run + "/" + task + "/" + attempt + "/" + name
}

func FailedCommitArtifactName(name string) string {
	return "git/failed-campaign-commits/" + name + ".json"
}

func failedCommitRecordName(p CommitProvenance) (domain.ArtifactKind, string) {
	if p.FailedAttempt != nil {
		return domain.ArtifactGitState, FailedCommitArtifactName(p.Name)
	}
	return domain.ArtifactOutput, p.Name
}

func validateFailedCommitAttempt(a *FailedCommitAttempt) error {
	if !safePathComponent(a.ID) || len(a.VerificationFailures) == 0 {
		return errors.New("failed commit needs a safe attempt ID and verification failures")
	}
	for _, failure := range a.VerificationFailures {
		if !strings.HasPrefix(failure, "verification command failed (") || strings.TrimSpace(failure) == "" {
			return errors.New("failed commit has a non-verification failure")
		}
	}
	return nil
}

// discardFailedAttempt removes quarantine refs when later finalization adds a
// failure other than verification. The unique attempt namespace cannot affect
// another attempt or its ordinary publication.
func (s CampaignRefStore) discardFailedAttempt(ctx context.Context, run, task, attempt string) error {
	lock, err := acquireFileLock(ctx, s.Root, "campaign-refs")
	if err != nil {
		return err
	}
	defer lock.Close()
	records, err := s.List(run)
	if err != nil {
		return err
	}
	for _, p := range records {
		if p.TaskID != task || p.FailedAttempt == nil || p.FailedAttempt.ID != attempt {
			continue
		}
		if err := runLoggedCommand(ctx, nil, "", s.git(), "--git-dir", filepath.Join(s.Root, "campaigns.git"), "update-ref", "-d", p.Ref, p.Commit); err != nil {
			return err
		}
		name := filepath.Join(s.Root, "provenance", run, task, "failed", attempt, p.Name+".json")
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	// A quarantine ref whose record was never written, because its
	// publication failed between the two, is found by its namespace.
	gitDir := filepath.Join(s.Root, "campaigns.git")
	if _, err := os.Lstat(gitDir); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	names, err := runLoggedCommandOutput(ctx, nil, "", s.git(), "--git-dir", gitDir,
		"for-each-ref", "--format=%(refname)", "refs/campaigns-quarantine/"+run+"/"+task+"/"+attempt+"/")
	if err != nil {
		return fmt.Errorf("list quarantine refs of attempt %s: %w", attempt, err)
	}
	for _, name := range strings.Fields(string(names)) {
		if err := runLoggedCommand(ctx, nil, "", s.git(), "--git-dir", gitDir, "update-ref", "-d", name); err != nil {
			return err
		}
	}
	return nil
}

// ValidateFailedCommitSource requires explicit custody binding before a failed
// candidate can be obtained, including when it names the consuming run.
// Ordinary published commits retain their existing source-validation behavior.
func ValidateFailedCommitSource(p CommitProvenance, source DependencySource) error {
	if p.FailedAttempt == nil {
		return nil
	}
	if source.WorkflowRunID != p.WorkflowRunID || source.TaskID != p.TaskID || source.AttemptID != p.FailedAttempt.ID {
		return errors.New("failed commit provenance does not match its carried dependency source attempt")
	}
	return nil
}

// ValidateCommitProvenance is the shared identity fence for records and transport.
func ValidateCommitProvenance(p CommitProvenance) error {
	if err := validateCommitTarget(p.WorkflowRunID, p.TaskID, p.Name); err != nil {
		return err
	}
	if !validGitObjectID(p.Commit) || !validGitObjectID(p.Base) {
		return errors.New("campaign commit record needs a commit and a base")
	}
	if p.Repository == "" {
		return errors.New("campaign commit record needs its repository")
	}
	want := CampaignRef(p.WorkflowRunID, p.TaskID, p.Name)
	if p.FailedAttempt != nil {
		if err := validateFailedCommitAttempt(p.FailedAttempt); err != nil {
			return err
		}
		want = FailedCampaignRef(p.WorkflowRunID, p.TaskID, p.FailedAttempt.ID, p.Name)
	}
	if (p.Ref != "" || p.FailedAttempt != nil) && p.Ref != want {
		return fmt.Errorf("campaign commit record names ref %q, want %q", p.Ref, want)
	}
	return nil
}
