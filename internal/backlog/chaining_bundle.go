package backlog

import (
	"context"
	"fmt"
	"strings"
)

// bundlePrerequisites finds the catalog-visible boundary of the recorded base.
// Include unpublished ancestry in the bundle, including unchanged outputs.
func (f AttemptFinalizer) bundlePrerequisites(ctx context.Context, workspaceDir, base string) ([]string, error) {
	raw, err := runLoggedCommandOutput(ctx, nil, "", f.CampaignRefs.git(), "-C", workspaceDir,
		"rev-list", "--boundary", base, "--not", "--remotes=origin")
	if err != nil {
		return nil, fmt.Errorf("find commit bundle prerequisites: %w", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return []string{base}, nil
	}
	var prerequisites []string
	for _, line := range strings.Fields(string(raw)) {
		if strings.HasPrefix(line, "-") {
			commit := strings.TrimPrefix(line, "-")
			if !validGitObjectID(commit) {
				return nil, fmt.Errorf("invalid commit bundle prerequisite %q", commit)
			}
			prerequisites = append(prerequisites, commit)
		}
	}
	return prerequisites, nil
}
