package backlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path"
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
func CaptureWorkspaceHead(ctx context.Context, gitBinary, workspace string, outputs []domain.ArtifactDeclaration) domain.WorkspaceHead {
	captured := domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema}
	if gitBinary == "" {
		gitBinary = "git"
	}
	head, err := workspaceGit(ctx, gitBinary, workspace, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
	if err != nil {
		captured.Error = "resolve workspace HEAD: " + err.Error()
		return captured
	}
	captured.Head = strings.TrimSpace(string(head))
	if !validGitObjectID(captured.Head) {
		captured.Head, captured.Error = "", fmt.Sprintf("resolve workspace HEAD: Git returned %q", captured.Head)
		return captured
	}
	status, err := workspaceGit(ctx, gitBinary, workspace, "status", "--porcelain=v1", "-z", "--untracked-files=no", "--ignore-submodules=none")
	if err != nil {
		captured.Head, captured.Error = "", "read workspace status: "+err.Error()
		return captured
	}
	excluded := make(map[string]struct{}, len(outputs))
	for _, output := range outputs {
		if output.Commit == nil {
			excluded[path.Clean(output.Name)] = struct{}{}
		}
	}
	entries := strings.Split(string(status), "\x00")
	for index := 0; index < len(entries); index++ {
		entry := entries[index]
		if len(entry) < 4 {
			continue
		}
		name := entry[3:]
		if entry[0] == 'R' || entry[0] == 'C' {
			// A rename or copy is followed by its source path, which is part of
			// the same change and not a second entry.
			index++
		}
		if _, skip := excluded[name]; skip || workspaceHeadIgnored(name) {
			continue
		}
		captured.Dirty = true
		if len(captured.DirtyPaths) < domain.MaxWorkspaceHeadDirtyPaths {
			captured.DirtyPaths = append(captured.DirtyPaths, name)
		}
	}
	return captured
}

func workspaceHeadIgnored(name string) bool {
	for _, directory := range []string{".t3", domain.TaskIdentityDir} {
		if name == directory || strings.HasPrefix(name, directory+"/") {
			return true
		}
	}
	return false
}

// workspaceGit runs one read-only Git query in the workspace and returns its
// standard output alone, so a warning on standard error cannot corrupt a
// porcelain listing.
func workspaceGit(ctx context.Context, gitBinary, workspace string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, gitBinary, append([]string{"-C", workspace}, args...)...)
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
