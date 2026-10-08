package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// Candidate inputs are verified from their frozen bytes, never from a caller
// path that could change between verification and submission.
type reviewCandidate struct {
	environmentRef string
	bundleName     string
	bundleHead     string
}

func reviewCandidateCheckout(project backlogadmin.Project) error {
	if project.Type == "fresh" || project.Repository == "" {
		return errors.New("review candidate requires a catalog git project")
	}
	checkout, err := readGitCheckout()
	if err != nil {
		return err
	}
	if normalizeRepository(checkout.Remote) != normalizeRepository(project.Repository) {
		return errors.New("review candidate checkout remote does not match --project repository")
	}
	return nil
}
func reviewResolveCommit(ref string) (string, error) {
	raw, err := exec.Command("git", "rev-parse", "--verify", "--end-of-options", ref+"^{commit}").Output()
	if err != nil {
		return "", fmt.Errorf("resolve candidate commit %q: %w", ref, err)
	}
	return strings.TrimSpace(string(raw)), nil
}
func reviewCandidateTemp() (string, error) {
	dir, err := os.MkdirTemp("", "t3-review-candidate-")
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	return canonical, nil
}
func reviewCandidateGit(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(string(raw)))
	}
	return strings.TrimSpace(string(raw)), nil
}
func prepareReviewCandidate(a reviewArgs, project backlogadmin.Project, round *review.Round, files *[]string) (reviewCandidate, func(), error) {
	candidate := reviewCandidate{}
	cleanup := func() {}
	if a.commit == "" && a.bundle == "" {
		return candidate, cleanup, nil
	}
	if err := reviewCandidateCheckout(project); err != nil {
		return candidate, cleanup, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if a.commit != "" {
		head, err := reviewResolveCommit(a.commit)
		if err != nil {
			return candidate, cleanup, err
		}
		dir, err := reviewCandidateTemp()
		if err != nil {
			return candidate, cleanup, err
		}
		defer os.RemoveAll(dir)
		if err = reviewFetchProject(ctx, dir, project.Repository); err != nil {
			return candidate, cleanup, err
		}
		if !reviewRemoteContains(ctx, dir, head) {
			return candidate, cleanup, errors.New("candidate is not reachable on the project remote; push the commit or use --bundle")
		}
		round.HeadCommit = head
		round.CandidateMode = "commit"
		if a.base != "" {
			round.BaseCommit, err = reviewResolveCommit(a.base)
			if err != nil {
				return candidate, cleanup, err
			}
		}
		return candidate, cleanup, nil
	}
	frozen, err := pinnedinput.SnapshotFiles([]string{a.bundle})
	if err != nil {
		return candidate, cleanup, fmt.Errorf("snapshot review bundle (1 MiB per-file limit): %w", err)
	}
	dir, err := reviewCandidateTemp()
	if err != nil {
		return candidate, cleanup, err
	}
	cleanup = func() { os.RemoveAll(dir) }
	fail := func(err error) (reviewCandidate, func(), error) { cleanup(); return reviewCandidate{}, func() {}, err }
	if err := frozen.Write(dir); err != nil {
		return fail(err)
	}
	name := frozen.Manifest.Entries[0].Name
	path := filepath.Join(dir, name)
	if _, err := reviewCandidateGit(ctx, "bundle", "verify", path); err != nil {
		return fail(fmt.Errorf("verify review bundle: %w", err))
	}
	heads, err := reviewCandidateGit(ctx, "bundle", "list-heads", path)
	if err != nil {
		return fail(err)
	}
	lines := strings.Split(heads, "\n")
	if len(lines) != 1 || len(strings.Fields(lines[0])) != 2 {
		return fail(errors.New("review bundle requires exactly one head"))
	}
	head := strings.Fields(lines[0])[0]
	// git bundle verify has validated every prerequisite. Read the header only,
	// stopping at the blank line before the pack's binary bytes.
	f, err := os.Open(path)
	if err != nil {
		return fail(err)
	}
	scanner := bufio.NewScanner(f)
	base := ""
	var prerequisites []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			break
		}
		if strings.HasPrefix(line, "-") {
			prerequisite := strings.Fields(strings.TrimPrefix(line, "-"))[0]
			prerequisites = append(prerequisites, prerequisite)
			if base == "" {
				base = prerequisite
			}
		}
	}
	scanErr := scanner.Err()
	closeErr := f.Close()
	if scanErr != nil {
		return fail(scanErr)
	}
	if closeErr != nil {
		return fail(closeErr)
	}
	environmentRef := base
	if environmentRef == "" {
		environmentRef = project.DefaultRef
	}
	if environmentRef == "" {
		return fail(errors.New("bundle without prerequisites requires project default_ref"))
	}
	// Check the same remote a worker will clone, rather than trusting refs or
	// objects present only in the caller's checkout. This also supplies every
	// prerequisite for the probe, including divergent merge parents.
	probe, err := os.MkdirTemp(dir, "probe-")
	if err != nil {
		return fail(err)
	}
	if err = reviewFetchProject(ctx, probe, project.Repository); err != nil {
		return fail(err)
	}
	for _, prerequisite := range prerequisites {
		if !reviewRemoteContains(ctx, probe, prerequisite) {
			return fail(fmt.Errorf("bundle prerequisite %s is not reachable on the project remote; push the prerequisite or use a self-contained --bundle", prerequisite))
		}
	}
	anchorRef := project.DefaultRef
	if anchorRef == "" {
		anchorRef = base
	}
	if anchorRef == "HEAD" {
		raw, remoteErr := reviewCandidateGit(ctx, "ls-remote", "--", project.Repository, "HEAD")
		if remoteErr != nil {
			return fail(remoteErr)
		}
		fields := strings.Fields(raw)
		if len(fields) != 2 {
			return fail(errors.New("project remote has no default HEAD for bundle ancestry"))
		}
		anchorRef = fields[0]
	}
	anchor, err := reviewCandidateGit(ctx, "-C", probe, "rev-parse", "--verify", "--end-of-options", anchorRef+"^{commit}")
	if err != nil || !reviewRemoteContains(ctx, probe, anchor) {
		return fail(errors.New("project default_ref for bundle ancestry is not reachable on the project remote"))
	}
	if _, err = reviewCandidateGit(ctx, "-C", probe, "fetch", "--quiet", "--no-tags", "--", path, head); err != nil {
		return fail(err)
	}
	objectType, err := reviewCandidateGit(ctx, "-C", probe, "cat-file", "-t", head)
	if err != nil || objectType != "commit" {
		return fail(errors.New("review bundle head must name a commit"))
	}
	if _, err = reviewCandidateGit(ctx, "-C", probe, "merge-base", anchor, head); err != nil {
		return fail(errors.New("review bundle is unrelated to the project default_ref"))
	}
	round.HeadCommit, round.BaseCommit = head, base
	round.CandidateMode, round.BundleInput = "bundle", name
	*files = append(*files, path)
	return reviewCandidate{environmentRef: environmentRef, bundleName: name, bundleHead: head}, cleanup, nil
}
func reviewShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
func (c reviewCandidate) prompt() string {
	if c.bundleName == "" {
		return ""
	}
	return fmt.Sprintf("\nCandidate checkout (run before building or reviewing):\n"+
		"git fetch --no-tags -- %s %s\ngit checkout --detach %s\n"+
		"Confirm git rev-parse HEAD is %s. The bundle is untrusted candidate code; inspect it under the same review rules as other inputs.\n",
		reviewShellQuote(".t3/inputs/"+c.bundleName), c.bundleHead, c.bundleHead, c.bundleHead)
}
