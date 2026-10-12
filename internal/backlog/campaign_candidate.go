package backlog

import (
	"bytes"
	"context"
	"fmt"
	"path"
)

// IsTaskReportPath classifies the exact protocol report basenames at any depth.
func IsTaskReportPath(name string) bool {
	switch path.Base(name) {
	case "continuation.md", "handoff.md":
		return true
	}
	return false
}

// ValidateCampaignCandidate rejects report-only work relative to authenticated
// provenance. The destination must already hold the candidate and base objects.
func ValidateCampaignCandidate(ctx context.Context, repository, candidate, base string) error {
	if !validGitObjectID(candidate) || !validGitObjectID(base) {
		return fmt.Errorf("campaign candidate %q: invalid candidate or recorded base object ID %q", candidate, base)
	}
	run := func(args ...string) ([]byte, error) {
		arguments := append([]string{"--no-replace-objects", "-C", repository}, args...)
		out, err := exportGit(ctx, arguments...).CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("campaign candidate %s: git %s: %w: %s", candidate, args[0], err, out)
		}
		return out, nil
	}
	for _, id := range []string{candidate, base} {
		kind, err := run("cat-file", "-t", id)
		if err != nil {
			return err
		}
		if string(bytes.TrimSpace(kind)) != "commit" {
			return fmt.Errorf("campaign candidate %s: recorded object %s is not a commit", candidate, id)
		}
	}
	if _, err := run("merge-base", "--is-ancestor", base, candidate); err != nil {
		return err
	}
	changed, err := run("diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--ignore-submodules=none", "--name-only", "-z", base, candidate, "--")
	if err != nil {
		return err
	}
	if len(changed) == 0 {
		return nil
	}
	if changed[len(changed)-1] != 0 {
		return fmt.Errorf("campaign candidate %s: invalid changed-path output", candidate)
	}
	for _, name := range bytes.Split(changed[:len(changed)-1], []byte{0}) {
		if !IsTaskReportPath(string(name)) {
			return nil
		}
	}
	return fmt.Errorf("campaign candidate %s is report-only relative to recorded base %s; include source or other documentation changes before importing", candidate, base)
}
