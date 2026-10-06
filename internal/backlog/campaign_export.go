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
	for _, component := range strings.Split(branch, "/") {
		if strings.HasSuffix(component, ".lock") {
			return errors.New("commit export: branch components cannot end in .lock")
		}
	}
	if err := validateGitRef("refs/heads/" + branch); err != nil {
		return fmt.Errorf("commit export branch: %w", err)
	}
	return nil
}

func fmtExportHash(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

// thinPackUnresolved is how unpack-objects, run with exportGit's C locale,
// reports deltas whose bases only the bundle's prerequisites hold.
const thinPackUnresolved = "fatal: unresolved deltas left after unpacking"

// exportGit runs Git for export verification isolated from the caller's
// environment and configuration: an inherited GIT_OBJECT_DIRECTORY or
// GIT_ALTERNATE_OBJECT_DIRECTORIES would let the verifier find the declared
// commit outside the received pack, and the C locale keeps diagnostics
// comparable.
func exportGit(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", args...)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "GIT_") || key == "LANG" || key == "LANGUAGE" || strings.HasPrefix(key, "LC_") {
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull, "LC_ALL=C")
	return cmd
}

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
	headerSize := int64(len(signature))
	var capabilities []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return result, err
		}
		headerSize += int64(len(line))
		if line == "\n" {
			break
		}
		if strings.HasPrefix(line, "@") {
			capabilities = append(capabilities, line)
		}
	}
	if signature == "# v2 git bundle\n" {
		if len(capabilities) != 0 {
			return result, errors.New("commit export: version 2 bundle cannot declare capabilities")
		}
	} else if signature == "# v3 git bundle\n" {
		if len(capabilities) != 1 || capabilities[0] != "@object-format=sha1\n" {
			return result, errors.New("commit export: version 3 bundle requires exactly the supported sha1 object format")
		}
	} else {
		return result, errors.New("commit export: unsupported bundle version")
	}
	repo := filepath.Join(directory, "verify.git")
	cmd := exportGit(ctx, "init", "--bare", "--quiet", repo)
	if out, err := cmd.CombinedOutput(); err != nil {
		return result, fmt.Errorf("commit export: init verifier: %w: %s", err, out)
	}
	// Bundle packs are thin: objects may be deltas against prerequisite objects
	// that only a repository at the base holds, so index-pack cannot resolve
	// them here. A dry-run unpack-objects still inflates every object and
	// checks the object count and the pack checksum, while queuing deltas
	// against absent bases. Git reads the pack from a file descriptor that
	// shares this process's offset and copies any bytes it read past the pack
	// to standard output, so the pack ends exactly at the end of the file only
	// when that output is empty and the offset reached the end; bytes beyond
	// its last read would otherwise go unseen.
	pack, err := os.Open(sourcePath)
	if err != nil {
		return result, err
	}
	defer pack.Close()
	if _, err := pack.Seek(headerSize, io.SeekStart); err != nil {
		return result, err
	}
	var trailing, diagnostics strings.Builder
	cmd = exportGit(ctx, "--git-dir", repo, "unpack-objects", "-n", "-q")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = pack, &trailing, &diagnostics
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, ctxErr
		}
		return result, fmt.Errorf("commit export: invalid bundle pack: %w: %s", err, diagnostics.String())
	}
	consumed, err := pack.Seek(0, io.SeekCurrent)
	if err != nil {
		return result, err
	}
	if trailing.Len() != 0 || consumed != size {
		return result, errors.New("commit export: invalid bundle pack: data after pack checksum")
	}
	// Unpacking for real applies every delta whose base is in the pack and
	// writes every object it can resolve, so cat-file can then find the
	// declared commit. Deltas left against prerequisite objects make Git stop
	// with exactly thinPackUnresolved after all other objects are written;
	// that one failure is expected, and any other means the pack is broken.
	if _, err := pack.Seek(headerSize, io.SeekStart); err != nil {
		return result, err
	}
	cmd = exportGit(ctx, "--git-dir", repo, "unpack-objects", "-q")
	var unpacked strings.Builder
	cmd.Stdin, cmd.Stderr = pack, &unpacked
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, ctxErr
		}
		if strings.TrimSpace(unpacked.String()) != thinPackUnresolved {
			return result, fmt.Errorf("commit export: invalid bundle pack: %w: %s", err, unpacked.String())
		}
	}
	cmd = exportGit(ctx, "--git-dir", repo, "cat-file", "-t", p.Commit)
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
