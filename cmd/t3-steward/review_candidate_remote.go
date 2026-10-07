package main

import (
	"context"
	"fmt"
)

// The disposable store is populated only from the catalog remote. Duplicate
// destination refspecs make both main and origin/main catalog refs resolve.
func reviewFetchProject(ctx context.Context, dir, repository string) error {
	if _, err := reviewCandidateGit(ctx, "init", "--bare", "--quiet", dir); err != nil {
		return err
	}
	if _, err := reviewCandidateGit(ctx, "-C", dir, "fetch", "--quiet", "--no-tags", "--", repository,
		"+refs/*:refs/*", "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return fmt.Errorf("check candidate on project remote: %w", err)
	}
	return nil
}
func reviewRemoteContains(ctx context.Context, dir, commit string) bool {
	refs, err := reviewCandidateGit(ctx, "-C", dir, "for-each-ref", "--contains="+commit, "--format=%(refname)")
	return err == nil && refs != ""
}
