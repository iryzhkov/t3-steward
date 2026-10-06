package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ExportPublishedBundle reads an existing published commit without creating a
// store or updating refs. The caller removes the returned temporary directory.
func (s CampaignRefStore) ExportPublishedBundle(ctx context.Context, p CommitProvenance, limit int64) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if err := validateCommitTarget(p.WorkflowRunID, p.TaskID, p.Name); err != nil {
		return "", err
	}
	if !validGitObjectID(p.Base) || !validGitObjectID(p.Commit) || p.Ref != CampaignRef(p.WorkflowRunID, p.TaskID, p.Name) {
		return "", errors.New("export campaign commit: invalid provenance")
	}
	if limit < 1 || limit > DefaultCommitBundleMaxBytes {
		return "", errors.New("export campaign commit: invalid size limit")
	}
	if s.MaxBundleBytes > 0 && s.MaxBundleBytes < limit {
		limit = s.MaxBundleBytes
	}
	gitDir := filepath.Join(s.Root, "campaigns.git")
	info, err := os.Lstat(gitDir)
	if err != nil {
		return "", fmt.Errorf("export campaign commit: source store unavailable: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("export campaign commit: source store is not a directory")
	}
	held, err := s.readProvenance(s.provenancePath(p.WorkflowRunID, p.TaskID, p.Name))
	if err != nil {
		return "", err
	}
	if held.Commit != p.Commit || held.Base != p.Base || held.Repository != p.Repository || held.Ref != p.Ref {
		return "", errors.New("export campaign commit: published provenance mismatch")
	}
	head, found, err := s.head(ctx, gitDir, p.Ref, nil)
	if err != nil {
		return "", err
	}
	if !found || head != p.Commit {
		return "", errors.New("export campaign commit: published ref mismatch")
	}
	if _, err := runLoggedCommandOutput(ctx, nil, "", s.git(), "--git-dir", gitDir, "merge-base", "--is-ancestor", p.Base, p.Commit); err != nil {
		return "", fmt.Errorf("export campaign commit: base is not an ancestor: %w", err)
	}
	dir, err := os.MkdirTemp("", "t3-commit-export-")
	if err != nil {
		return "", err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(dir)
		}
	}()
	path := filepath.Join(dir, "commit.bundle")
	args := []string{"--git-dir", gitDir, "bundle", "create", path, p.Ref}
	if p.Commit != p.Base {
		args = append(args, "^"+p.Base)
	}
	if err := runLoggedCommand(ctx, nil, "", s.git(), args...); err != nil {
		return "", err
	}
	info, err = os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.Size() > limit {
		return "", errors.New("export campaign commit: bundle exceeds size limit")
	}
	if err := runLoggedCommand(ctx, nil, "", s.git(), "--git-dir", gitDir, "bundle", "verify", path); err != nil {
		return "", err
	}
	keep = true
	return path, nil
}
