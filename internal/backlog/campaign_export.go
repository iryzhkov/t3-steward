package backlog

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ExportedCommitBundle owns one verified export; Close removes its temporary directory.
type ExportedCommitBundle struct {
	Path   string
	Size   int64
	SHA256 string
}

func (b ExportedCommitBundle) Close() error { return os.RemoveAll(filepath.Dir(b.Path)) }

// ValidateExportBranch accepts a branch name, never a revision or a Git option.
func ValidateExportBranch(branch string) error {
	if branch == "" || len(branch) > 1024 || strings.HasPrefix(branch, "-") || strings.TrimSpace(branch) != branch {
		return errors.New("commit export: a valid branch name of at most 1024 bytes is required")
	}
	if err := validateGitRef("refs/heads/" + branch); err != nil {
		return fmt.Errorf("commit export branch: %w", err)
	}
	return nil
}

func fmtExportHash(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

// ExportCommitBundle validates the authenticated source bundle against its
// provenance and rewrites only the advertised ref. Git validates the pack in a
// private repository; no source or worker refs are changed. The base prerequisite
// is deliberately retained, so consumers must already have the campaign base.
func ExportCommitBundle(ctx context.Context, p CommitProvenance, branch string, source io.Reader, limit int64) (ExportedCommitBundle, error) {
	var result ExportedCommitBundle
	if err := ValidateExportBranch(branch); err != nil {
		return result, err
	}
	if !validGitObjectID(p.Commit) || !validGitObjectID(p.Base) {
		return result, errors.New("commit export: invalid provenance commit or base")
	}
	if err := validateCommitTarget(p.WorkflowRunID, p.TaskID, p.Name); err != nil {
		return result, err
	}
	if p.Ref != CampaignRef(p.WorkflowRunID, p.TaskID, p.Name) {
		return result, errors.New("commit export: provenance ref does not name declared commit")
	}
	if source == nil {
		return result, errors.New("commit export: bundle source is unavailable")
	}
	if limit <= 0 {
		limit = DefaultCommitBundleMaxBytes
	}
	if limit > DefaultCommitBundleMaxBytes {
		limit = DefaultCommitBundleMaxBytes
	}
	directory, err := os.MkdirTemp("", "t3-commit-export-")
	if err != nil {
		return result, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(directory)
		}
	}()
	sourcePath := filepath.Join(directory, "source.bundle")
	input, err := os.OpenFile(sourcePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return result, err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(input, hash), io.LimitReader(source, limit+1))
	if err := errors.Join(copyErr, input.Close()); err != nil {
		return result, fmt.Errorf("commit export: receive bundle: %w", err)
	}
	if size > limit {
		return result, fmt.Errorf("commit export: bundle exceeds %d byte limit", limit)
	}
	if p.Bundle != nil && (p.Bundle.Size != size || !strings.EqualFold(p.Bundle.SHA256, fmt.Sprintf("%x", hash.Sum(nil)))) {
		return result, errors.New("commit export: bundle digest or size differs from provenance")
	}
	heads, prerequisites, err := readBundleHeader(sourcePath)
	if err != nil {
		return result, fmt.Errorf("commit export: %w", err)
	}
	if len(heads) != 1 || heads[0].commit != p.Commit || heads[0].ref != p.Ref {
		return result, errors.New("commit export: bundle must advertise exactly its declared commit and ref")
	}
	if (p.Commit != p.Base && (len(prerequisites) != 1 || prerequisites[0] != p.Base)) ||
		(p.Commit == p.Base && len(prerequisites) != 0 && (len(prerequisites) != 1 || prerequisites[0] != p.Base)) {
		return result, errors.New("commit export: bundle must have exactly the declared base prerequisite")
	}
	file, err := os.Open(sourcePath)
	if err != nil {
		return result, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	signature, err := reader.ReadString('\n')
	if err != nil {
		return result, err
	}
	var capabilities []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return result, err
		}
		if line == "\n" {
			break
		}
		if strings.HasPrefix(line, "@") {
			capabilities = append(capabilities, line)
		}
	}
	repo := filepath.Join(directory, "verify.git")
	cmd := exec.CommandContext(ctx, "git", "init", "--bare", "--quiet", repo)
	if out, err := cmd.CombinedOutput(); err != nil {
		return result, fmt.Errorf("commit export: init verifier: %w: %s", err, out)
	}
	// index-pack checks object decoding, delta resolution and the pack checksum.
	// Git bundle packs are self-contained even when history has prerequisites.
	cmd = exec.CommandContext(ctx, "git", "--git-dir", repo, "index-pack", "--stdin")
	cmd.Stdin = reader
	if out, err := cmd.CombinedOutput(); err != nil {
		return result, fmt.Errorf("commit export: invalid bundle pack: %w: %s", err, out)
	}
	cmd = exec.CommandContext(ctx, "git", "--git-dir", repo, "cat-file", "-t", p.Commit)
	if out, err := cmd.Output(); err != nil || strings.TrimSpace(string(out)) != "commit" {
		return result, errors.New("commit export: pack does not contain declared commit")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	reader.Reset(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return result, err
		}
		if line == "\n" {
			break
		}
	}
	result.Path = filepath.Join(directory, "export.bundle")
	output, err := os.OpenFile(result.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return ExportedCommitBundle{}, err
	}
	outHash := sha256.New()
	writer := io.MultiWriter(output, outHash)
	_, headerErr := io.WriteString(writer, signature+strings.Join(capabilities, "")+"-"+p.Base+" campaign base\n"+p.Commit+" refs/heads/"+branch+"\n\n")
	packSize, packErr := io.Copy(writer, reader)
	closeErr := output.Close()
	if err := errors.Join(headerErr, packErr, closeErr); err != nil {
		return ExportedCommitBundle{}, err
	}
	info, err := os.Stat(result.Path)
	if err != nil {
		return ExportedCommitBundle{}, err
	}
	result.Size = info.Size()
	result.SHA256 = fmt.Sprintf("%x", outHash.Sum(nil))
	if packSize <= 0 || result.Size > limit {
		return ExportedCommitBundle{}, errors.New("commit export: empty or oversized output bundle")
	}
	keep = true
	return result, nil
}
