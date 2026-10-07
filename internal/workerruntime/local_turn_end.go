package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
//
// The snapshot runs while a failure waits to publish, sometimes under the
// worker's lock, so it is bounded by the driver's SnapshotTimeout whatever
// its caller's context: the Git it runs reads the work tree, and Git waits on
// a FIFO the task named .gitignore until it is stopped.
func (d *LocalDriver) SnapshotWorkInProgress(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) (string, error) {
	if d.Config.DryRun || workspace == "" || !backlog.DeclaresAnyCommit(pkg.Outputs) {
		return "", nil
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".git")); err != nil {
		return "", nil
	}
	limit := d.Config.SnapshotTimeout
	if limit <= 0 {
		limit = DefaultSnapshotTimeout
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	summary, err := d.snapshotWorkInProgress(bounded, pkg, workspace)
	if err != nil && ctx.Err() == nil && errors.Is(bounded.Err(), context.DeadlineExceeded) {
		return "", fmt.Errorf("snapshot did not finish within %s: %w", limit, err)
	}
	return summary, err
}

// DefaultSnapshotTimeout bounds a work-in-progress snapshot when the driver
// sets no SnapshotTimeout. A snapshot copies the workspace's object store and
// stages its work tree once.
const DefaultSnapshotTimeout = 5 * time.Minute

func (d *LocalDriver) snapshotWorkInProgress(ctx context.Context, pkg workerproto.ExecutionPackage, workspace string) (string, error) {
	attemptDir := d.workspacePath(pkg)
	scratch, err := os.MkdirTemp(attemptDir, ".wip-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(scratch)
	git, err := newSnapshotRepository(ctx, workspace, filepath.Join(scratch, "git"))
	if errors.Is(err, errUnbornHead) && onlyGitAndRuntime(workspace) {
		// Nothing was ever committed and nothing was written: as clean as a
		// tree gets.
		return "working tree clean; no wip.bundle", nil
	}
	if err != nil {
		return "", fmt.Errorf("prepare snapshot repository: %w", err)
	}
	ref := workInProgressRefPrefix + pkg.Identity.AttemptID
	if _, err := git.run(ctx, nil, "check-ref-format", ref); err != nil {
		return "", fmt.Errorf("work-in-progress ref %q is not valid: %w", ref, err)
	}
	head := git.head
	// The task's own index seeds the snapshot where Git can use it; an index
	// it cannot, such as a split one, gives way to HEAD's tree.
	tree, err := git.stage(ctx, filepath.Join(git.gitDir, "task-index"), "")
	if err != nil {
		tree, err = git.stage(ctx, filepath.Join(scratch, "index"), head)
	}
	if err != nil {
		return "", err
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
// the task's repository never runs a command, or hands over a file, with the
// worker's authority.
//
// Git takes commands from configuration: clean filters run while files are
// staged, signing programs while commits are written, a promisor remote's
// command while a missing object is fetched, and hooks and the file system
// monitor around all of them. The task can write its repository's
// configuration, attributes and hooks, and a snapshot runs on the worker host,
// outside any containment the attempt had, so a command the task named would
// run there and could read what the attempt was never given. Overriding the
// settings one by one cannot keep up with a configuration the task may still
// be changing. The snapshot therefore runs with GIT_DIR naming a directory
// the worker wrote, with no configuration beyond the object format, no hooks
// and no attributes. The working tree is the workspace, read as plain files.
// System and global configuration are not read either, and the worker's
// environment is not passed on.
//
// The objects are copied into that directory rather than borrowed, through
// an os.Root on the workspace and skipping every symbolic link: a link, or an
// alternate written while the snapshot runs, would otherwise make the worker
// read objects from outside the workspace into the bundle. HEAD, the ignore
// rules and the task's index are read the same way, as data.
//
// Filters the workspace's attributes name are undefined here, so files are
// staged as they are on disk. A workspace whose .git or objects is not a
// directory of its own, or whose objects come from another repository, is not
// snapshotted.
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
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return snapshotRepository{}, err
	}
	defer root.Close()
	for _, dir := range []string{".git", ".git/objects"} {
		info, err := root.Lstat(dir)
		if err != nil {
			return snapshotRepository{}, err
		}
		if !info.IsDir() {
			return snapshotRepository{}, fmt.Errorf("%s is not a directory of the workspace's own", dir)
		}
	}
	for _, alternates := range []string{".git/objects/info/alternates", ".git/objects/info/http-alternates"} {
		if _, err := root.Lstat(alternates); !errors.Is(err, os.ErrNotExist) {
			return snapshotRepository{}, errors.New("the workspace borrows objects from another repository")
		}
	}
	// The object format follows from the length of the object name.
	home := filepath.Dir(gitDir)
	head, err := readWorkspaceHead(ctx, root)
	if err != nil {
		if errors.Is(err, errUnbornHead) {
			return snapshotRepository{}, err
		}
		return snapshotRepository{}, fmt.Errorf("resolve HEAD: %w", err)
	}
	config := "[core]\n\trepositoryformatversion = 0\n\tbare = false\n"
	if len(head) == 64 {
		config = "[core]\n\trepositoryformatversion = 1\n\tbare = false\n[extensions]\n\tobjectformat = sha256\n"
	}
	for _, dir := range []string{"objects/info", "objects/pack", "refs/heads", "refs/tags", "info"} {
		if err := os.MkdirAll(filepath.Join(gitDir, filepath.FromSlash(dir)), 0o700); err != nil {
			return snapshotRepository{}, err
		}
	}
	if err := copyWorkspaceObjects(ctx, root, filepath.Join(gitDir, "objects")); err != nil {
		return snapshotRepository{}, fmt.Errorf("copy the workspace's objects: %w", err)
	}
	files := map[string]string{"HEAD": "ref: refs/heads/snapshot\n", "config": config}
	// The task's ignore rules and a shallow history are data, not commands,
	// and the snapshot keeps honouring them; one it cannot read fails the
	// snapshot rather than letting ignored files into it.
	for _, name := range []string{"info/exclude", "shallow"} {
		raw, err := readRootFile(ctx, root, ".git/"+name, 16<<20)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return snapshotRepository{}, fmt.Errorf("read .git/%s: %w", name, err)
		}
		files[name] = string(raw)
	}
	// The task's index is data too. Seeding the snapshot from it keeps what
	// Git already knows about the work tree: files whose recorded state still
	// matches are not read again, so a file a filter checked out is kept as
	// the task's index has it, and paths outside a sparse checkout stay. One
	// it cannot read gives way to HEAD's tree, but an index that is not a
	// file at all fails the snapshot like any other special file.
	if raw, err := readRootFile(ctx, root, ".git/index", 1<<30); err == nil {
		files["task-index"] = string(raw)
	} else if errors.Is(err, errFileType) || ctx.Err() != nil {
		return snapshotRepository{}, fmt.Errorf("read .git/index: %w", err)
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

// copyWorkspaceObjects copies the loose objects and packs of the workspace's
// repository into objects. Every path is opened through root, so no link
// leads outside the workspace, and links themselves are skipped, so a link to
// another repository's pack is never read. Indexes Git can rebuild, such as
// the commit graph, are not copied.
func copyWorkspaceObjects(ctx context.Context, root *os.Root, objects string) error {
	entries, err := readRootDir(root, ".git/objects")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || (name != "pack" && (len(name) != 2 || !objectName(name+strings.Repeat("0", 38)))) {
			continue
		}
		files, err := readRootDir(root, ".git/objects/"+name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(objects, name), 0o700); err != nil {
			return err
		}
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !file.Type().IsRegular() {
				continue
			}
			if ext := filepath.Ext(file.Name()); name == "pack" && ext != ".pack" && ext != ".idx" && ext != ".rev" {
				continue
			}
			if err := copyRootFile(ctx, root, ".git/objects/"+name+"/"+file.Name(), filepath.Join(objects, name, file.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// errFileType is a path the snapshot reads that is not the regular file or
// directory it expects, such as a FIFO or a device the task put there.
var errFileType = errors.New("not a regular file or directory")

// openRootFile opens the regular file name in root for reading. Opening a
// FIFO for reading waits for a writer the task need never provide, and os.Root
// confines the path but does not change that, so a special file is refused
// before it is opened, and the open does not wait either, so one swapped in
// after that check is refused from the opened descriptor. A symbolic link is
// followed inside root and its target checked the same way.
func openRootFile(root *os.Root, name string) (*os.File, os.FileInfo, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
		return nil, nil, fmt.Errorf("%s: %w", name, errFileType)
	}
	return openRootNonblocking(root, name, false)
}

// openRootNonblocking opens name in root for reading without waiting on a
// FIFO, and keeps it only if the opened file is a regular file, or a
// directory when dir is set.
func openRootNonblocking(root *os.Root, name string, dir bool) (*os.File, os.FileInfo, error) {
	flags := os.O_RDONLY | syscall.O_NONBLOCK | syscall.O_NOCTTY
	if dir {
		flags |= syscall.O_DIRECTORY
	}
	f, err := root.OpenFile(name, flags, 0)
	if dir && errors.Is(err, syscall.ENOTDIR) {
		return nil, nil, fmt.Errorf("%s: %w", name, errFileType)
	}
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if (dir && !info.IsDir()) || (!dir && !info.Mode().IsRegular()) {
		f.Close()
		return nil, nil, fmt.Errorf("%s: %w", name, errFileType)
	}
	return f, info, nil
}

// readRootDir lists dir in root.
func readRootDir(root *os.Root, dir string) ([]os.DirEntry, error) {
	f, _, err := openRootNonblocking(root, dir, true)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.ReadDir(-1)
}

// copyRootFile copies the regular file name in root to destination.
func copyRootFile(ctx context.Context, root *os.Root, name, destination string) error {
	source, _, err := openRootFile(root, name)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := copyChunks(ctx, target, source); err != nil {
		target.Close()
		return err
	}
	return target.Close()
}

// readRootFile reads the regular file name in root, of at most limit bytes.
func readRootFile(ctx context.Context, root *os.Root, name string, limit int64) ([]byte, error) {
	f, info, err := openRootFile(root, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if info.Size() > limit {
		return nil, fmt.Errorf("%s is larger than %d bytes", name, limit)
	}
	// The file may grow after the check; the limit still holds.
	var raw bytes.Buffer
	if err := copyChunks(ctx, &raw, io.LimitReader(f, limit)); err != nil {
		return nil, err
	}
	return raw.Bytes(), nil
}

// copyChunks copies src to dst a few megabytes at a time, so that a long
// copy stops once ctx ends.
func copyChunks(ctx context.Context, dst io.Writer, src io.Reader) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.CopyN(dst, src, 8<<20); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// readWorkspaceHead is the object name HEAD names in the workspace's
// repository, read from HEAD, the loose ref files and packed-refs, following
// symbolic refs a few levels.
func readWorkspaceHead(ctx context.Context, root *os.Root) (string, error) {
	name := "HEAD"
	for range 5 {
		var value string
		if raw, err := readRootFile(ctx, root, ".git/"+name, 4096); err == nil {
			value = strings.TrimSpace(string(raw))
		} else if !errors.Is(err, os.ErrNotExist) || name == "HEAD" {
			return "", err
		} else if value, err = packedRef(ctx, root, name); err != nil {
			if name != "HEAD" && errors.Is(err, os.ErrNotExist) {
				return "", fmt.Errorf("%w: %s", errUnbornHead, name)
			}
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

// errUnbornHead is HEAD naming a branch that has no commit yet.
var errUnbornHead = errors.New("HEAD names a branch with no commit")

// packedRef is ref's object name in the workspace repository's packed-refs,
// or an error wrapping os.ErrNotExist when the ref is not there.
func packedRef(ctx context.Context, root *os.Root, ref string) (string, error) {
	raw, err := readRootFile(ctx, root, ".git/packed-refs", 64<<20)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if value, name, ok := strings.Cut(strings.TrimSpace(line), " "); ok && name == ref {
			return value, nil
		}
	}
	return "", fmt.Errorf("ref %s: %w", ref, os.ErrNotExist)
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

// stage writes the tree of the work tree as it is, through the index file at
// index, seeded first from the tree head when head is set, and returns the
// tree's object name.
func (r snapshotRepository) stage(ctx context.Context, index, head string) (string, error) {
	env := []string{"GIT_INDEX_FILE=" + index}
	if head != "" {
		if _, err := r.run(ctx, env, "read-tree", head); err != nil {
			return "", fmt.Errorf("seed snapshot index: %w", err)
		}
	} else if _, err := os.Lstat(index); err != nil {
		return "", err
	}
	if _, err := r.run(ctx, env, "add", "-A", "--", ".", ":(exclude).t3"); err != nil {
		return "", fmt.Errorf("stage snapshot: %w", err)
	}
	tree, err := r.run(ctx, env, "write-tree")
	if err != nil {
		return "", fmt.Errorf("write snapshot tree: %w", err)
	}
	return tree, nil
}

// onlyGitAndRuntime reports whether workspace holds nothing but .git and the
// runtime's .t3 directory.
func onlyGitAndRuntime(workspace string) bool {
	entries, err := os.ReadDir(workspace)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.Name() != ".git" && entry.Name() != ".t3" {
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
	// Git is stopped when ctx ends; nothing it started may keep the snapshot
	// waiting on its output after that.
	command.WaitDelay = 5 * time.Second
	command.Env = append([]string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + r.home, "XDG_CONFIG_HOME=" + r.home, "LC_ALL=C",
		"GIT_DIR=" + r.gitDir, "GIT_WORK_TREE=" + r.workspace,
		"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_ATTR_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
	}, env...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", fmt.Errorf("git %s stopped: %w", args[0], ctxErr)
		}
		return "", fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}
