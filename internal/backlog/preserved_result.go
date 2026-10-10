package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PreservedResultSchema names the format of a PreservedResult.
const PreservedResultSchema = "t3-steward/preserved-result/v1"

// Values a PreservedEntry takes when there is nothing to hash.
const (
	preservedAbsent     = "absent"
	preservedUnresolved = "unresolved"
)

// ErrPreservedWorkspaceMissing reports that the workspace a preserved result
// describes no longer exists.
var ErrPreservedWorkspaceMissing = errors.New("workspace is missing")

// PreservedEntry is one declared output or commit and what it was: the
// SHA-256 of an output's content, the object ID of a commit, or "absent" and
// "unresolved" when there was none.
type PreservedEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// PreservedResult is the digest of the declared work a finished turn left in
// its workspace: the content of every declared output file and the commit
// every declared commit resolved to. A worker records it once a collection
// has captured the result, and a later collection of the same attempt, after
// the first failed to publish, reuses the workspace only when the digest is
// unchanged. The turn is never run again for it; what is retried is the
// collection.
type PreservedResult struct {
	Schema  string           `json:"schema"`
	Digest  string           `json:"digest"`
	Outputs []PreservedEntry `json:"outputs,omitempty"`
	Commits []PreservedEntry `json:"commits,omitempty"`
}

// CapturePreservedResult computes the preserved result of a workspace for the
// declared outputs. gitBinary empty means "git". A missing workspace is
// ErrPreservedWorkspaceMissing; an output that is absent and a commit that
// does not resolve are recorded as such rather than refused, because their
// absence is part of what the turn left.
func CapturePreservedResult(ctx context.Context, gitBinary, workspace string, outputs []domain.ArtifactDeclaration) (PreservedResult, error) {
	if gitBinary == "" {
		gitBinary = "git"
	}
	info, err := os.Stat(workspace)
	if errors.Is(err, os.ErrNotExist) {
		return PreservedResult{}, ErrPreservedWorkspaceMissing
	}
	if err != nil {
		return PreservedResult{}, fmt.Errorf("inspect workspace: %w", err)
	}
	if !info.IsDir() {
		return PreservedResult{}, fmt.Errorf("workspace %q is not a directory", workspace)
	}
	result := PreservedResult{Schema: PreservedResultSchema}
	for _, output := range outputs {
		if output.Commit != nil {
			revision := output.Commit.Revision
			if revision == "" {
				revision = "HEAD"
			}
			value := preservedUnresolved
			if raw, err := workspaceGit(ctx, gitBinary, workspace, nil, "rev-parse", "--verify", "--end-of-options", revision+"^{commit}"); err == nil {
				if id := strings.TrimSpace(string(raw)); validGitObjectID(id) {
					value = id
				}
			} else if ctx.Err() != nil {
				return PreservedResult{}, ctx.Err()
			}
			result.Commits = append(result.Commits, PreservedEntry{Name: output.Name, Value: value})
			continue
		}
		value, err := preservedOutputDigest(workspace, output.Name)
		if err != nil {
			return PreservedResult{}, fmt.Errorf("digest declared output %q: %w", output.Name, err)
		}
		result.Outputs = append(result.Outputs, PreservedEntry{Name: output.Name, Value: value})
	}
	result.Digest = result.computeDigest()
	return result, nil
}

// Difference describes the first way current differs from p, or is empty
// when the two describe the same work.
func (p PreservedResult) Difference(current PreservedResult) string {
	if p.Schema != current.Schema {
		return fmt.Sprintf("schema %q, now %q", p.Schema, current.Schema)
	}
	if p.Digest != p.computeDigest() {
		return "the preserved record does not match its own digest"
	}
	if p.Digest == current.Digest {
		return ""
	}
	for _, pair := range [][2][]PreservedEntry{{p.Outputs, current.Outputs}, {p.Commits, current.Commits}} {
		before, after := pair[0], pair[1]
		if len(before) != len(after) {
			return fmt.Sprintf("%d declared entries, now %d", len(before), len(after))
		}
		for i := range before {
			if before[i] != after[i] {
				return fmt.Sprintf("declared %q was %s, now %s", before[i].Name, shortValue(before[i].Value), shortValue(after[i].Value))
			}
		}
	}
	return fmt.Sprintf("digest %s, now %s", shortValue(p.Digest), shortValue(current.Digest))
}

func (p PreservedResult) computeDigest() string {
	raw, _ := json.Marshal(struct {
		Schema  string           `json:"schema"`
		Outputs []PreservedEntry `json:"outputs"`
		Commits []PreservedEntry `json:"commits"`
	}{p.Schema, p.Outputs, p.Commits})
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func shortValue(value string) string {
	value = strings.TrimPrefix(value, "sha256:")
	if len(value) > 12 {
		return value[:12]
	}
	return value
}

// preservedOutputDigest hashes a declared output file by its content. It
// resolves the file the way collection does, so an output that escapes the
// workspace or is not a regular file is refused here as it is there.
func preservedOutputDigest(workspace, name string) (string, error) {
	resolved, err := safeBundleFile(workspace, name)
	if errors.Is(err, os.ErrNotExist) {
		return preservedAbsent, nil
	}
	if err != nil {
		return "", err
	}
	file, err := os.Open(resolved)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
