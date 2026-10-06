package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// WorkspaceHeadArtifactName is where a review-declared task's result carries
// the worker's report of its workspace HEAD.
const WorkspaceHeadArtifactName = "git-state/workspace-head.json"

// WorkspaceHeadArtifactID is the fixed identity of that report, so a result
// cannot carry a second one or smuggle other content under the kind.
func WorkspaceHeadArtifactID(attemptID string) string {
	return "workspace-head-" + attemptID
}

// CaptureWorkspaceHead reads the physical HEAD of a task workspace and whether
// tracked files differ from it. Declared file outputs and the task's .t3 and
// .t3-steward directories are not the reviewed work and are not counted;
// neither are untracked files. A workspace without a usable HEAD is reported
// through Error, never as an empty clean head.
//
// The workspace's index and configuration belong to the executor, so neither
// is trusted to say what changed. Files marked assume-unchanged or
// skip-worktree, a stat cache with forged timestamps, and a configured file
// system monitor all let "git status" skip a file. The worktree is therefore
// also compared by content with a fresh index read from HEAD, which carries no
// flags and no cached stat data, with the monitor and sparse checkout off. A
// configured core.worktree could point every query at a clean copy elsewhere,
// so each query names the task workspace as its worktree.
//
// Conversion rules decide what "equal" means, and the executor controls them
// too: a clean filter or an untracked .gitattributes can turn edited bytes
// back into the committed blob, and core.symlinks=false lets a regular file
// stand in for a tracked symlink. The content comparison therefore runs in a
// scratch repository of the worker's own, see compareWorkspaceWithHead.
func CaptureWorkspaceHead(ctx context.Context, gitBinary, workspace string, outputs []domain.ArtifactDeclaration) domain.WorkspaceHead {
	captured := domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema}
	if gitBinary == "" {
		gitBinary = "git"
	}
	workspace, err := filepath.Abs(workspace)
	if err != nil {
		captured.Error = "resolve workspace path: " + err.Error()
		return captured
	}
	head, err := workspaceGit(ctx, gitBinary, workspace, nil, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
	if err != nil {
		captured.Error = "resolve workspace HEAD: " + err.Error()
		return captured
	}
	captured.Head = strings.TrimSpace(string(head))
	if !validGitObjectID(captured.Head) {
		captured.Head, captured.Error = "", fmt.Sprintf("resolve workspace HEAD: Git returned %q", captured.Head)
		return captured
	}
	statusArgs := []string{"status", "--porcelain=v1", "-z", "--untracked-files=no", "--ignore-submodules=none"}
	// The workspace's own index also reports staged changes.
	status, err := workspaceGit(ctx, gitBinary, workspace, nil, statusArgs...)
	if err != nil {
		captured.Head, captured.Error = "", "read workspace status: "+err.Error()
		return captured
	}
	physical, err := compareWorkspaceWithHead(ctx, gitBinary, workspace, captured.Head, statusArgs, 0)
	if err != nil {
		captured.Head, captured.Error = "", "compare the workspace with HEAD: "+err.Error()
		return captured
	}
	excluded := make(map[string]struct{}, len(outputs))
	for _, output := range outputs {
		if output.Commit == nil {
			excluded[path.Clean(output.Name)] = struct{}{}
		}
	}
	seen := make(map[string]struct{})
	countWorkspaceChanges(&captured, status, excluded, seen)
	countWorkspaceChanges(&captured, physical, excluded, seen)
	return captured
}

// compareWorkspaceWithHead lists the tracked files of the workspace whose
// content, mode or type differs from the commit head. It reads head into a
// fresh index of a scratch repository that borrows the workspace's objects
// through an alternate, which is safe because objects are named by their
// content. That repository has the worker's own configuration, so neither the
// workspace's configuration and info/attributes nor the system and global
// configuration and attributes take part: there are no filter drivers, line
// endings are not converted by configuration, symbolic links and file modes
// are compared, and the file system monitor and sparse checkout are off.
// Attributes are read from head's tree rather than the worktree, so only the
// reviewed .gitattributes apply. Git older than 2.40 ignores GIT_ATTR_SOURCE
// and reads the worktree's .gitattributes, which can still convert line
// endings and encodings but cannot run a filter.
//
// A file that a filter driver, such as Git LFS, produced in the worktree
// differs from its blob here and is reported as changed, which fails closed.
//
// Git compares a submodule's content inside the submodule, with the
// submodule's own configuration, so every populated submodule is compared the
// same way against the commit head records for it, and its changes are
// reported under its path.
func compareWorkspaceWithHead(ctx context.Context, gitBinary, workspace, head string, statusArgs []string, depth int) ([]byte, error) {
	if depth > maxWorkspaceSubmoduleDepth {
		return nil, fmt.Errorf("submodules are nested deeper than %d levels", maxWorkspaceSubmoduleDepth)
	}
	location, err := workspaceGit(ctx, gitBinary, workspace, nil, "rev-parse", "--show-object-format", "--git-path", "objects")
	if err != nil {
		return nil, fmt.Errorf("locate the workspace's objects: %w", err)
	}
	format, objects, _ := strings.Cut(strings.TrimSuffix(string(location), "\n"), "\n")
	if format != "sha1" && format != "sha256" {
		return nil, fmt.Errorf("workspace object format %q is not supported", format)
	}
	if objects == "" || strings.ContainsAny(objects, "\n\r") {
		return nil, fmt.Errorf("workspace object directory %q is not usable", objects)
	}
	if !filepath.IsAbs(objects) {
		objects = filepath.Join(workspace, objects)
	}
	scratch, err := os.MkdirTemp("", "workspace-head-")
	if err != nil {
		return nil, fmt.Errorf("create scratch repository: %w", err)
	}
	defer os.RemoveAll(scratch)
	repository := filepath.Join(scratch, "repository")
	config := "[core]\n\trepositoryformatversion = 0\n"
	if format == "sha256" {
		config = "[core]\n\trepositoryformatversion = 1\n[extensions]\n\tobjectformat = sha256\n"
	}
	config += "[core]\n\tbare = false\n\tfilemode = true\n\tsymlinks = true\n\tignorecase = false\n\tprecomposeunicode = false\n" +
		"\tautocrlf = false\n\tfsmonitor = false\n\tsparseCheckout = false\n" +
		"\tattributesFile = " + os.DevNull + "\n"
	for name, content := range map[string]string{
		"HEAD":                    head + "\n",
		"config":                  config,
		"objects/info/alternates": objects + "\n",
		"refs/.keep":              "",
	} {
		file := filepath.Join(repository, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
			return nil, fmt.Errorf("create scratch repository: %w", err)
		}
		if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
			return nil, fmt.Errorf("create scratch repository: %w", err)
		}
	}
	if _, err := trustedWorkspaceGit(ctx, gitBinary, workspace, repository, head, "read-tree", head); err != nil {
		return nil, fmt.Errorf("read HEAD into a scratch index: %w", err)
	}
	status, err := trustedWorkspaceGit(ctx, gitBinary, workspace, repository, head, statusArgs...)
	if err != nil {
		return nil, err
	}
	tree, err := trustedWorkspaceGit(ctx, gitBinary, workspace, repository, head, "ls-tree", "-r", "-z", "--full-tree", head)
	if err != nil {
		return nil, fmt.Errorf("list HEAD's submodules: %w", err)
	}
	for _, entry := range strings.Split(string(tree), "\x00") {
		meta, name, found := strings.Cut(entry, "\t")
		fields := strings.Fields(meta)
		if !found || len(fields) != 3 || fields[0] != "160000" {
			continue
		}
		directory := filepath.Join(workspace, filepath.FromSlash(name))
		if _, err := os.Lstat(filepath.Join(directory, ".git")); errors.Is(err, os.ErrNotExist) {
			// Not checked out; the status above reports a submodule that
			// went away.
			continue
		}
		nested, err := compareWorkspaceWithHead(ctx, gitBinary, directory, fields[2], statusArgs, depth+1)
		if err != nil {
			return nil, fmt.Errorf("submodule %s: %w", name, err)
		}
		status = append(status, prefixWorkspaceChanges(nested, name+"/")...)
	}
	return status, nil
}

// maxWorkspaceSubmoduleDepth bounds how deeply submodules are compared.
const maxWorkspaceSubmoduleDepth = 8

// prefixWorkspaceChanges puts prefix in front of every path of a porcelain
// v1 -z listing, including the source path that follows a rename or copy.
func prefixWorkspaceChanges(status []byte, prefix string) []byte {
	var prefixed []byte
	entries := strings.Split(string(status), "\x00")
	for index := 0; index < len(entries); index++ {
		entry := entries[index]
		if len(entry) < 4 {
			continue
		}
		prefixed = append(prefixed, entry[:3]+prefix+entry[3:]+"\x00"...)
		if strings.ContainsAny(entry[:2], "RC") && index+1 < len(entries) {
			index++
			prefixed = append(prefixed, prefix+entries[index]+"\x00"...)
		}
	}
	return prefixed
}

// trustedWorkspaceGit runs one Git query on the workspace's worktree through
// the scratch repository, with no system or global configuration or
// attributes and with attributes read from head.
func trustedWorkspaceGit(ctx context.Context, gitBinary, workspace, repository, head string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, gitBinary, append([]string{
		"--no-optional-locks", "--no-replace-objects",
		"--git-dir", repository, "--work-tree", workspace,
	}, args...)...)
	command.Dir = workspace
	command.Env = append(slices.DeleteFunc(os.Environ(), func(variable string) bool {
		name, _, _ := strings.Cut(variable, "=")
		return slices.Contains(workspaceGitLocationVariables, name) || slices.Contains(trustedGitDroppedVariables, name) ||
			strings.HasPrefix(name, "GIT_CONFIG") || strings.HasPrefix(name, "GIT_ATTR")
	}), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_ATTR_NOSYSTEM=1", "GIT_ATTR_SOURCE="+head)
	return runWorkspaceGit(command)
}

// trustedGitDroppedVariables would point the scratch repository at other
// objects, or at part of them.
var trustedGitDroppedVariables = []string{"GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_NAMESPACE"}

// countWorkspaceChanges adds the tracked paths of one porcelain v1 -z listing
// to the report, once each.
func countWorkspaceChanges(captured *domain.WorkspaceHead, status []byte, excluded, seen map[string]struct{}) {
	entries := strings.Split(string(status), "\x00")
	for index := 0; index < len(entries); index++ {
		entry := entries[index]
		if len(entry) < 4 {
			continue
		}
		changed := []string{entry[3:]}
		if strings.ContainsAny(entry[:2], "RC") && index+1 < len(entries) {
			// A rename or copy, staged or not, is followed by its source path,
			// which is part of the same entry. A renamed source is a tracked
			// file that went away, and counts even when the destination is a
			// declared output; a copied source is unchanged.
			index++
			if strings.Contains(entry[:2], "R") {
				changed = append(changed, entries[index])
			}
		}
		for _, name := range changed {
			if _, skip := excluded[name]; skip || workspaceHeadIgnored(name) {
				continue
			}
			if _, counted := seen[name]; counted {
				continue
			}
			seen[name] = struct{}{}
			captured.Dirty = true
			if len(captured.DirtyPaths) < domain.MaxWorkspaceHeadDirtyPaths {
				captured.DirtyPaths = append(captured.DirtyPaths, name)
			}
		}
	}
}

func workspaceHeadIgnored(name string) bool {
	for _, directory := range []string{".t3", domain.TaskIdentityDir} {
		if name == directory || strings.HasPrefix(name, directory+"/") {
			return true
		}
	}
	return false
}

// workspaceGit runs one Git query in the workspace and returns its standard
// output alone, so a warning on standard error cannot corrupt a porcelain
// listing. It writes nothing in the workspace: optional locks are off, so
// status does not refresh the workspace's index, and the workspace's file
// system monitor and sparse checkout settings are overridden. The worktree is
// pinned to the workspace itself, because core.worktree in the workspace's
// configuration, or GIT_WORK_TREE and GIT_DIR in the worker's environment,
// would otherwise choose which directory Git examines. Replace refs are
// ignored, because one could substitute another tree for HEAD's while HEAD
// still names the accepted commit, and file modes are always compared, because
// core.fileMode=false hides a mode change. env adds to the worker's
// environment.
func workspaceGit(ctx context.Context, gitBinary, workspace string, env []string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, gitBinary, append([]string{
		"--no-optional-locks", "--no-replace-objects",
		"-c", "core.fsmonitor=false", "-c", "core.sparseCheckout=false", "-c", "core.fileMode=true",
		"-C", workspace, "--work-tree", workspace,
	}, args...)...)
	command.Env = append(slices.DeleteFunc(os.Environ(), func(variable string) bool {
		name, _, _ := strings.Cut(variable, "=")
		return slices.Contains(workspaceGitLocationVariables, name)
	}), env...)
	return runWorkspaceGit(command)
}

// runWorkspaceGit returns a query's standard output alone.
func runWorkspaceGit(command *exec.Cmd) ([]byte, error) {
	command.WaitDelay = time.Second
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return nil, fmt.Errorf("%w: %s", err, message)
		}
		return nil, err
	}
	return output, nil
}

// workspaceGitLocationVariables are the environment variables that tell Git
// where a repository, its worktree and its replace refs are. The workspace is
// located by its own path alone.
var workspaceGitLocationVariables = []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_REPLACE_REF_BASE"}

// MarshalWorkspaceHead encodes the report the worker publishes.
func MarshalWorkspaceHead(head domain.WorkspaceHead) ([]byte, error) {
	raw, err := json.MarshalIndent(head, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode workspace head: %w", err)
	}
	return append(raw, '\n'), nil
}

// ParseWorkspaceHead decodes the report strictly: unknown fields, trailing
// content and another schema are refused rather than read as a clean head.
func ParseWorkspaceHead(raw []byte) (domain.WorkspaceHead, error) {
	var head domain.WorkspaceHead
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&head); err != nil {
		return domain.WorkspaceHead{}, fmt.Errorf("decode workspace head: %w", err)
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return domain.WorkspaceHead{}, errors.New("decode workspace head: trailing content")
	}
	if head.Schema != domain.WorkspaceHeadSchema {
		return domain.WorkspaceHead{}, fmt.Errorf("workspace head schema %q is not supported", head.Schema)
	}
	return head, nil
}
