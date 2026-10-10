package main

import (
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/wait"
)

// gitHubChecksForms names every form --github-checks accepts.
const gitHubChecksForms = "owner/name@<sha>, https://github.com/owner/name/commit/<sha>, <sha> with --repo, " +
	"owner/name#<n>, https://github.com/owner/name/pull/<n> or <n>"

// parseGitHubChecksTarget reads the value of --github-checks: one commit, or
// one pull request whose head commit's checks are read. Either way the wait is
// a github wait in state checks-completed, so it is polled, given up on and
// woken exactly as --github is; only the target and the reading differ.
//
// A commit is owner/name@<sha>, its github.com commit URL, or a bare id of 7
// to 40 hexadecimal digits with --repo. A value of digits alone is a pull
// request number, never an abbreviated commit, so the reading of a value does
// not depend on which digits it happens to contain.
func parseGitHubChecksTarget(spec, repo string) (wait.GitHubTarget, error) {
	spec = strings.TrimSpace(spec)
	refuse := fmt.Errorf("--github-checks %q is not a target; --github-checks takes %s", spec, gitHubChecksForms)
	if spec == "" {
		return wait.GitHubTarget{}, refuse
	}
	named, sha := "", ""
	switch {
	case strings.Contains(spec, "github.com/") && strings.Contains(spec, "/commit/"):
		rest := spec[strings.Index(spec, "github.com/")+len("github.com/"):]
		if cut := strings.IndexAny(rest, "?#"); cut >= 0 {
			rest = rest[:cut]
		}
		parts := strings.Split(rest, "/")
		if len(parts) < 4 || parts[0] == "" || parts[1] == "" || parts[2] != "commit" {
			return wait.GitHubTarget{}, refuse
		}
		named, sha = parts[0]+"/"+parts[1], parts[3]
	case strings.Contains(spec, "@"):
		named, sha, _ = strings.Cut(spec, "@")
		if named == "" {
			return wait.GitHubTarget{}, refuse
		}
	case !gitHubNumber(spec) && wait.GitHubCommitSHA.MatchString(spec):
		sha = spec
	default:
		// Every other form is a pull request, read by the --github parser so
		// the two flags accept a pull request in the same forms.
		if strings.Contains(spec, "://") && !strings.Contains(spec, "github.com/") {
			return wait.GitHubTarget{}, refuse
		}
		if strings.Contains(spec, "github.com/") && !strings.Contains(spec, "/pull/") {
			return wait.GitHubTarget{}, refuse
		}
		if !gitHubNumber(spec) && !gitHubQualifiedNumber(spec) && !strings.Contains(spec, "github.com/") {
			return wait.GitHubTarget{}, refuse
		}
		return parseGitHubTarget("pr:"+spec, wait.ChecksCompleted, repo)
	}
	if named != "" && (strings.Count(named, "/") != 1 || strings.HasPrefix(named, "/") || strings.HasSuffix(named, "/")) {
		return wait.GitHubTarget{}, refuse
	}
	if named != "" && repo != "" && !strings.EqualFold(named, repo) {
		return wait.GitHubTarget{}, fmt.Errorf("--github-checks %s names %s but --repo names %s; give one repository", spec, named, repo)
	}
	if named != "" {
		repo = named
	}
	target := wait.GitHubTarget{Kind: "commit", ID: strings.ToLower(sha), State: wait.ChecksCompleted, Repo: repo}
	if err := target.Validate(); err != nil {
		return target, err
	}
	return target, nil
}
