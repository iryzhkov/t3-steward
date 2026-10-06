package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// WorkInProgressBundleFile is where a failed attempt's work-in-progress
// bundle is kept on the worker: in the attempt directory beside the workspace,
// so it has the attempt's retention and is removed with it.
const WorkInProgressBundleFile = "wip.bundle"

// workInProgressRefPrefix is the private ref namespace of snapshot commits. It
// is never pushed: nothing pushes refs/steward, and the snapshot leaves the
// worker only inside the bundle uploaded with the failed result.
const workInProgressRefPrefix = "refs/steward/wip/"

// turnEndNudgePurpose names the follow-up turn in its T3 identities.
const turnEndNudgePurpose = "turn-end-nudge"

// LiveCommands reports the commands the attempt detached in its workspace and
// left running. A contained execution is not inspected: its provider runs in
// its own pid and mount namespaces, where the host cannot read the working
// directories against the workspace path, and the supervisor stops the whole
// sandbox when the attempt is collected, so nothing it started outlives it.
func (d *LocalDriver) LiveCommands(_ context.Context, pkg workerproto.ExecutionPackage, workspace string) (LiveCommandReport, error) {
	if d.Config.DryRun || len(pkg.Environment.DirectoryBindings) > 0 || workspace == "" {
		return LiveCommandReport{}, nil
	}
	return scanLiveCommands(workspace)
}

// NudgeLiveCommands starts the follow-up turn in the attempt's own session.
// It goes through the same turn start the throttle resume uses, under
// identities derived from token, so a retry is recognised by T3.
func (d *LocalDriver) NudgeLiveCommands(ctx context.Context, pkg workerproto.ExecutionPackage, token, text string) error {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.NudgeLiveCommands(ctx, pkg, token, text)
	}
	if d.Config.DryRun {
		return nil
	}
	thread, err := d.requiredThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return err
	}
	if sender, ok := d.T3.(interface {
		SendOnce(context.Context, domain.Thread, string, string, string) error
	}); ok {
		return sender.SendOnce(ctx, thread, token, turnEndNudgePurpose, text)
	}
	return d.T3.ResumeThread(ctx, thread, text)
}

// SnapshotWorkInProgress keeps the uncommitted work of a task that declares a
// commit, before the attempt is failed, so it can be recovered without a shell
// on the worker.
//
// The snapshot is a commit on the attempt's HEAD, written through a private
// index so the task's own index and working tree are untouched, and kept on a
// private ref. The bundle carries that ref from the workspace's base pin, so
// it holds the attempt's own commits as well and verifies against the base
// alone. The runtime's .t3 directory is never part of it. A clean tree, or a
// task with no declared commit, is not snapshotted, and neither is a
// workspace that never became a Git work tree.
func (d *LocalDriver) SnapshotWorkInProgress(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) (string, error) {
	if d.Config.DryRun || workspace == "" || !backlog.DeclaresAnyCommit(pkg.Outputs) {
		return "", nil
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".git")); err != nil {
		return "", nil
	}
	ref := workInProgressRefPrefix + pkg.Identity.AttemptID
	if _, err := workspaceGit(ctx, workspace, nil, "check-ref-format", ref); err != nil {
		return "", fmt.Errorf("work-in-progress ref %q is not valid: %w", ref, err)
	}
	status, err := workspaceGit(ctx, workspace, nil, "status", "--porcelain", "--untracked-files=all", "--", ".", ":(exclude).t3")
	if err != nil {
		return "", fmt.Errorf("read working tree status: %w", err)
	}
	if status == "" {
		return "working tree clean; no wip.bundle", nil
	}
	head, err := workspaceGit(ctx, workspace, nil, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve HEAD: %w", err)
	}
	attemptDir := d.workspacePath(pkg)
	scratch, err := os.MkdirTemp(attemptDir, ".wip-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(scratch)
	index := []string{"GIT_INDEX_FILE=" + filepath.Join(scratch, "index")}
	if _, err := workspaceGit(ctx, workspace, index, "read-tree", head); err != nil {
		return "", fmt.Errorf("seed snapshot index: %w", err)
	}
	if _, err := workspaceGit(ctx, workspace, index, "add", "-A", "--", ".", ":(exclude).t3"); err != nil {
		return "", fmt.Errorf("stage snapshot: %w", err)
	}
	tree, err := workspaceGit(ctx, workspace, index, "write-tree")
	if err != nil {
		return "", fmt.Errorf("write snapshot tree: %w", err)
	}
	identity := []string{
		"GIT_AUTHOR_NAME=t3-steward", "GIT_AUTHOR_EMAIL=t3-steward@localhost",
		"GIT_COMMITTER_NAME=t3-steward", "GIT_COMMITTER_EMAIL=t3-steward@localhost",
	}
	message := fmt.Sprintf("t3-steward: uncommitted work of attempt %s when it failed", pkg.Identity.AttemptID)
	commit, err := workspaceGit(ctx, workspace, identity, "commit-tree", tree, "-p", head, "-m", message)
	if err != nil {
		return "", fmt.Errorf("write snapshot commit: %w", err)
	}
	if _, err := workspaceGit(ctx, workspace, nil, "update-ref", ref, commit); err != nil {
		return "", fmt.Errorf("record snapshot ref: %w", err)
	}
	bundleArgs := []string{"bundle", "create", filepath.Join(scratch, WorkInProgressBundleFile), ref}
	if base, err := backlog.WorkspaceBaseCommit(workspace); err == nil && base != "" {
		if _, err := workspaceGit(ctx, workspace, nil, "merge-base", "--is-ancestor", base, commit); err == nil {
			bundleArgs = append(bundleArgs, "^"+base)
		}
	}
	if _, err := workspaceGit(ctx, workspace, nil, bundleArgs...); err != nil {
		return "", fmt.Errorf("create wip.bundle: %w", err)
	}
	if _, err := workspaceGit(ctx, workspace, nil, "bundle", "verify", filepath.Join(scratch, WorkInProgressBundleFile)); err != nil {
		return "", fmt.Errorf("verify wip.bundle: %w", err)
	}
	path := filepath.Join(attemptDir, WorkInProgressBundleFile)
	if err := os.Rename(filepath.Join(scratch, WorkInProgressBundleFile), path); err != nil {
		return "", fmt.Errorf("retain wip.bundle: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	summary := fmt.Sprintf("wip.bundle retained: %s at %s, %d bytes", ref, commit[:12], info.Size())
	if limit := pkg.Limits.MaxArtifactBytes; limit > 0 && info.Size() > limit {
		summary += ", too large to upload; kept on the worker at " + path
	}
	d.logger().Info("uncommitted work snapshotted before the attempt fails",
		"attempt", pkg.Identity.AttemptID, "ref", ref, "commit", commit, "bundle", path, "bytes", info.Size())
	return summary, nil
}

// retainWorkInProgress keeps the uncommitted work of a failing attempt and
// returns the failure text naming what was kept. A bundle an earlier step of
// the same failure already retained, such as the live-commands check, is
// reused rather than snapshotted again, and is not named twice. A clean tree,
// or a task with no declared commit, leaves the failure as it is; a snapshot
// that fails is named but never stops the failure from publishing.
func (d *LocalDriver) retainWorkInProgress(ctx context.Context, pkg workerproto.ExecutionPackage, workspace, failure string) string {
	if d.Config.DryRun || workspace == "" || !backlog.DeclaresAnyCommit(pkg.Outputs) {
		return failure
	}
	if _, err := os.Lstat(filepath.Join(d.workspacePath(pkg), WorkInProgressBundleFile)); err == nil {
		return failure
	}
	retained, err := d.SnapshotWorkInProgress(ctx, pkg, workspace)
	if err != nil {
		d.logger().Warn("uncommitted work of the failed attempt is not retained", "attempt", pkg.Identity.AttemptID, "error", err)
		retained = "wip.bundle not retained: " + err.Error()
	}
	if !strings.HasPrefix(retained, "wip.bundle") || strings.Contains(failure, WorkInProgressBundleFile) {
		return failure
	}
	return failure + "; " + retained
}

// workInProgressBundle reads the bundle a failed result uploads. A missing
// bundle, or one the upload would refuse, leaves the result without it: the
// failure itself must still publish.
func (d *LocalDriver) workInProgressBundle(pkg workerproto.ExecutionPackage, result PublishedResult) []byte {
	if result.Finalized.Completion.Failure == "" || !backlog.DeclaresAnyCommit(pkg.Outputs) {
		return nil
	}
	path := filepath.Join(d.workspacePath(pkg), WorkInProgressBundleFile)
	raw, err := readBoundedRegularFile(path, pkg.Limits.MaxArtifactBytes)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			d.logger().Warn("wip.bundle is not uploaded with the failed result", "attempt", pkg.Identity.AttemptID, "bundle", path, "error", err)
		}
		return nil
	}
	result.WorkInProgressBundle = raw
	if admitter, ok := d.Publisher.(interface {
		AdmitResult(workerproto.ExecutionPackage, PublishedResult) error
	}); ok {
		if err := admitter.AdmitResult(pkg, result); err != nil {
			d.logger().Warn("wip.bundle does not fit the failed result upload; it stays on the worker", "attempt", pkg.Identity.AttemptID, "bundle", path, "error", err)
			return nil
		}
	}
	return raw
}

// workspaceGit runs one git command in workspace with hooks and the file
// system monitor disabled, and returns its trimmed standard output.
func workspaceGit(ctx context.Context, workspace string, env []string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", append([]string{
		"-C", workspace, "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=", "-c", "gc.auto=0",
	}, args...)...)
	command.Env = append(append(os.Environ(), "GIT_TERMINAL_PROMPT=0"), env...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}
