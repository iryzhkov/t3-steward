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
	scratch, err := os.MkdirTemp("", "workspace-head-")
	if err != nil {
		captured.Head, captured.Error = "", "create scratch index: "+err.Error()
		return captured
	}
	defer os.RemoveAll(scratch)
	freshIndex := []string{"GIT_INDEX_FILE=" + filepath.Join(scratch, "index")}
	if _, err := workspaceGit(ctx, gitBinary, workspace, freshIndex, "read-tree", captured.Head); err != nil {
		captured.Head, captured.Error = "", "read HEAD into a scratch index: "+err.Error()
		return captured
	}
	physical, err := workspaceGit(ctx, gitBinary, workspace, freshIndex, statusArgs...)
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
// would otherwise choose which directory Git examines. env adds to the
// worker's environment.
func workspaceGit(ctx context.Context, gitBinary, workspace string, env []string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, gitBinary, append([]string{
		"--no-optional-locks", "-c", "core.fsmonitor=false", "-c", "core.sparseCheckout=false",
		"-C", workspace, "--work-tree", workspace,
	}, args...)...)
	command.Env = append(slices.DeleteFunc(os.Environ(), func(variable string) bool {
		name, _, _ := strings.Cut(variable, "=")
		return slices.Contains(workspaceGitLocationVariables, name)
	}), env...)
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
// where a repository and its worktree are. The workspace is located by its own
// path alone.
var workspaceGitLocationVariables = []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"}

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
