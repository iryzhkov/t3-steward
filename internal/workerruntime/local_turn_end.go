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
//
// Git runs here on the worker host, outside any containment the attempt had,
// so it runs in a private repository (see snapshotRepository) and reads
// nothing the task wrote under .git as configuration.
func (d *LocalDriver) SnapshotWorkInProgress(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) (string, error) {
	if d.Config.DryRun || workspace == "" || !backlog.DeclaresAnyCommit(pkg.Outputs) {
		return "", nil
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".git")); err != nil {
		return "", nil
	}
	attemptDir := d.workspacePath(pkg)
	scratch, err := os.MkdirTemp(attemptDir, ".wip-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(scratch)
	git, err := newSnapshotRepository(ctx, workspace, filepath.Join(scratch, "git"))
	if err != nil {
		return "", fmt.Errorf("prepare snapshot repository: %w", err)
	}
	ref := workInProgressRefPrefix + pkg.Identity.AttemptID
	if _, err := git.run(ctx, nil, "check-ref-format", ref); err != nil {
		return "", fmt.Errorf("work-in-progress ref %q is not valid: %w", ref, err)
	}
	head := git.head
	index := []string{"GIT_INDEX_FILE=" + filepath.Join(scratch, "index")}
	if _, err := git.run(ctx, index, "read-tree", head); err != nil {
		return "", fmt.Errorf("seed snapshot index: %w", err)
	}
	if _, err := git.run(ctx, index, "add", "-A", "--", ".", ":(exclude).t3"); err != nil {
		return "", fmt.Errorf("stage snapshot: %w", err)
	}
	tree, err := git.run(ctx, index, "write-tree")
	if err != nil {
		return "", fmt.Errorf("write snapshot tree: %w", err)
	}
	headTree, err := git.run(ctx, nil, "rev-parse", "--verify", head+"^{tree}")
	if err != nil {
		return "", fmt.Errorf("resolve HEAD tree: %w", err)
	}
	if tree == headTree {
		return "working tree clean; no wip.bundle", nil
	}
	identity := []string{
		"GIT_AUTHOR_NAME=t3-steward", "GIT_AUTHOR_EMAIL=t3-steward@localhost",
		"GIT_COMMITTER_NAME=t3-steward", "GIT_COMMITTER_EMAIL=t3-steward@localhost",
	}
	message := fmt.Sprintf("t3-steward: uncommitted work of attempt %s when it failed", pkg.Identity.AttemptID)
	commit, err := git.run(ctx, identity, "commit-tree", tree, "-p", head, "-m", message)
	if err != nil {
		return "", fmt.Errorf("write snapshot commit: %w", err)
	}
	if _, err := git.run(ctx, nil, "update-ref", ref, commit); err != nil {
		return "", fmt.Errorf("record snapshot ref: %w", err)
	}
	bundleArgs := []string{"bundle", "create", filepath.Join(scratch, WorkInProgressBundleFile), ref}
	if base, err := backlog.WorkspaceBaseCommit(workspace); err == nil && base != "" {
		if _, err := git.run(ctx, nil, "merge-base", "--is-ancestor", base, commit); err == nil {
			bundleArgs = append(bundleArgs, "^"+base)
		}
	}
	if _, err := git.run(ctx, nil, bundleArgs...); err != nil {
		return "", fmt.Errorf("create wip.bundle: %w", err)
	}
	if _, err := git.run(ctx, nil, "bundle", "verify", filepath.Join(scratch, WorkInProgressBundleFile)); err != nil {
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
// describes what was kept, or nothing for a clean tree or a task with no
// declared commit. A bundle an earlier step of the same failure already
// retained, such as the live-commands check or a collection whose publication
// did not complete, is reused rather than snapshotted again. A snapshot that
// fails is described but never stops the failure from publishing.
func (d *LocalDriver) retainWorkInProgress(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) string {
	if d.Config.DryRun || workspace == "" || !backlog.DeclaresAnyCommit(pkg.Outputs) {
		return ""
	}
	path := filepath.Join(d.workspacePath(pkg), WorkInProgressBundleFile)
	if info, err := os.Lstat(path); err == nil {
		return fmt.Sprintf("wip.bundle retained: %s, %d bytes", path, info.Size())
	}
	retained, err := d.SnapshotWorkInProgress(ctx, pkg, workspace)
	if err != nil {
		// Git's standard error names the paths it could not use, which may hold
		// an execution credential, so only the checked text is logged or kept.
		checked := d.loggedText(ctx, pkg, err.Error())
		d.logger().Warn("uncommitted work of the failed attempt is not retained", "attempt", pkg.Identity.AttemptID, "error", checked)
		return "wip.bundle not retained: " + checked
	}
	if !strings.HasPrefix(retained, "wip.bundle") {
		return ""
	}
	return retained
}

// withWorkInProgress names what retainWorkInProgress kept in a failure text
// that does not already say the bundle was retained.
func withWorkInProgress(failure, retained string) string {
	if retained == "" || strings.Contains(failure, "wip.bundle retained") {
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
			var secret *SecretScanError
			if errors.As(err, &secret) {
				// The scan covers the whole result, so the refused object may be
				// the archive or the reason; a redacted retry tries the bundle again.
				d.logger().Warn("wip.bundle is left out of a failed result the secret scan refused; it stays on the worker", "attempt", pkg.Identity.AttemptID, "bundle", path,
					"object", secret.Object, "detector", secret.Detector, "byte_offset", secret.Offset, "fingerprint", secret.Fingerprint)
				return nil
			}
			d.logger().Warn("wip.bundle does not fit the failed result upload; it stays on the worker", "attempt", pkg.Identity.AttemptID, "bundle", path, "error", err)
			return nil
		}
	}
	return raw
}

// snapshotRepository is the private Git directory a snapshot runs in, so that
// the task's repository configuration never runs with the worker's authority.
//
// Git takes commands from configuration: clean filters run while files are
// staged, signing programs while commits are written, and hooks and the file
// system monitor around both. The task can write its repository's
// configuration, attributes and hooks, and a snapshot runs on the worker host,
// outside any containment the attempt had, so a filter the task named would
// run there and could read what the attempt was never given. Overriding the
// settings one by one cannot keep up with a configuration the task may still
// be changing. The snapshot therefore runs with GIT_DIR naming a directory
// the worker wrote, with no configuration beyond the object format, no hooks
// and no attributes, borrowing the workspace's objects through an alternate
// and keeping its own objects and the snapshot ref. The working tree is the
// workspace, read as plain files. System and global configuration are not
// read either, and the worker's environment is not passed on.
//
// Filters the workspace's attributes name are undefined here, so files are
// staged as they are on disk. A workspace whose .git is not a directory of its
// own, or whose objects come from another repository, is not snapshotted: the
// worker would read objects the task chose from outside the workspace.
type snapshotRepository struct {
	workspace string
	gitDir    string
	// home is the worker's scratch directory, given to Git as its home so
	// that no per-user file is read either.
	home string
	// head is the workspace's HEAD commit when the snapshot started.
	head string
}

func newSnapshotRepository(ctx context.Context, workspace, gitDir string) (snapshotRepository, error) {
	source := filepath.Join(workspace, ".git")
	objects := filepath.Join(source, "objects")
	for _, dir := range []string{source, objects} {
		info, err := os.Lstat(dir)
		if err != nil {
			return snapshotRepository{}, err
		}
		if !info.IsDir() {
			return snapshotRepository{}, fmt.Errorf("%s is not a directory of the workspace's own", dir)
		}
	}
	if _, err := os.Lstat(filepath.Join(objects, "info", "alternates")); !errors.Is(err, os.ErrNotExist) {
		return snapshotRepository{}, errors.New("the workspace borrows objects from another repository")
	}
	// HEAD is read from the files, not by Git in the workspace's repository:
	// that would read the task's configuration, and a promisor remote there
	// makes Git fetch a missing commit by running a command the task named.
	// The object format follows from the length of the object name.
	home := filepath.Dir(gitDir)
	head, err := readWorkspaceHead(source)
	if err != nil {
		return snapshotRepository{}, fmt.Errorf("resolve HEAD: %w", err)
	}
	config := "[core]\n\trepositoryformatversion = 0\n\tbare = false\n"
	if len(head) == 64 {
		config = "[core]\n\trepositoryformatversion = 1\n\tbare = false\n[extensions]\n\tobjectformat = sha256\n"
	}
	for _, dir := range []string{"objects/info", "refs/heads", "refs/tags", "info"} {
		if err := os.MkdirAll(filepath.Join(gitDir, filepath.FromSlash(dir)), 0o700); err != nil {
			return snapshotRepository{}, err
		}
	}
	files := map[string]string{
		"HEAD":                    "ref: refs/heads/snapshot\n",
		"config":                  config,
		"objects/info/alternates": objects + "\n",
	}
	// The task's ignore rules and a shallow history are data, not commands,
	// and the snapshot keeps honouring them.
	for _, name := range []string{"info/exclude", "shallow"} {
		raw, err := readBoundedRegularFile(filepath.Join(source, filepath.FromSlash(name)), 4<<20)
		if err == nil {
			files[name] = string(raw)
		}
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(gitDir, filepath.FromSlash(name)), []byte(content), 0o600); err != nil {
			return snapshotRepository{}, err
		}
	}
	git := snapshotRepository{workspace: workspace, gitDir: gitDir, home: home}
	// The commit itself must be there: the private repository has no remote
	// to fetch it from.
	if git.head, err = git.run(ctx, nil, "rev-parse", "--verify", "--end-of-options", head+"^{commit}"); err != nil {
		return snapshotRepository{}, fmt.Errorf("resolve HEAD: %w", err)
	}
	return git, nil
}

// readWorkspaceHead is the object name HEAD names in the repository at
// gitDir, read from HEAD, the loose ref files and packed-refs, following
// symbolic refs a few levels.
func readWorkspaceHead(gitDir string) (string, error) {
	name := "HEAD"
	for range 5 {
		var value string
		if raw, err := readBoundedRegularFile(filepath.Join(gitDir, filepath.FromSlash(name)), 4096); err == nil {
			value = strings.TrimSpace(string(raw))
		} else if !errors.Is(err, os.ErrNotExist) || name == "HEAD" {
			return "", err
		} else if value, err = packedRef(gitDir, name); err != nil {
			return "", err
		}
		target, symbolic := strings.CutPrefix(value, "ref: ")
		if !symbolic {
			if !objectName(value) {
				return "", fmt.Errorf("%s does not name an object", name)
			}
			return value, nil
		}
		target = strings.TrimSpace(target)
		if !strings.HasPrefix(target, "refs/") || strings.Contains(target, "..") || strings.ContainsAny(target, "\\\x00") {
			return "", fmt.Errorf("%s names an unusable ref %q", name, target)
		}
		name = target
	}
	return "", errors.New("HEAD is a chain of symbolic refs")
}

// packedRef is ref's object name in gitDir's packed-refs.
func packedRef(gitDir, ref string) (string, error) {
	raw, err := readBoundedRegularFile(filepath.Join(gitDir, "packed-refs"), 64<<20)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("ref %s does not exist", ref)
		}
		return "", err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if value, name, ok := strings.Cut(strings.TrimSpace(line), " "); ok && name == ref {
			return value, nil
		}
	}
	return "", fmt.Errorf("ref %s does not exist", ref)
}

// objectName reports whether value is a full SHA-1 or SHA-256 object name.
func objectName(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// run runs one git command with a minimal environment and returns its
// trimmed standard output. Hooks, the file system monitor and commit signing
// are also turned off on the command line, where they outrank every file.
func (r snapshotRepository) run(ctx context.Context, env []string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", append([]string{
		"-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=", "-c", "gc.auto=0", "-c", "commit.gpgSign=false",
	}, args...)...)
	command.Dir = r.workspace
	command.Env = append([]string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + r.home, "XDG_CONFIG_HOME=" + r.home, "LC_ALL=C",
		"GIT_DIR=" + r.gitDir, "GIT_WORK_TREE=" + r.workspace,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_ATTR_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
	}, env...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}
