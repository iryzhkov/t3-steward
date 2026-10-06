package workerruntime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func (s *CustodyStore) scanResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult, planned []resultObject) error {
	config := s.config.SecretScan
	var canaries []string
	if config.Canaries != nil {
		var err error
		canaries, err = config.Canaries(ctx, pkg)
		if err != nil {
			return &SecretScanError{Object: "execution", Detector: "credential-resolution", Offset: 0}
		}
	}
	scanner := newResultScanner(config, canaries, nil)
	allow, err := loadSecretAllowlist(result.WorkspaceDir)
	if err != nil {
		return &SecretScanError{Object: ".t3/secret-scan-allow", Detector: "allowlist-read", Offset: 0}
	}
	scanner.allow = allow
	declarations := map[string]bool{}
	for _, output := range pkg.Outputs {
		if output.Commit != nil {
			declarations[output.Name] = true
		}
	}
	for _, entry := range planned {
		if err := ctx.Err(); err != nil {
			return err
		}
		kind := "output"
		if entry.object.Kind == "log" {
			kind = "archive"
		}
		if entry.object.Kind == "summary" {
			kind = "summary"
		}
		if declarations[entry.artifactName()] {
			kind = "commit"
		}
		if entry.object.MediaType == backlog.CommitBundleMediaType {
			kind = "bundle"
		}
		var reader io.Reader = bytes.NewReader(entry.data)
		var file *os.File
		var source string
		if entry.artifact != nil {
			source, err = finalizedArtifactPath(result.Finalized, *entry.artifact)
			if err != nil {
				return err
			}
			file, err = openRegular(source)
			if err != nil {
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "object-read", Offset: 0}
			}
			reader = file
		}
		scanErr := scanner.scan(entry.object.Path, kind, strings.NewReader(entry.object.Path))
		if scanErr == nil {
			scanErr = scanner.scan(entry.object.Path, kind, reader)
		}
		if file != nil {
			closeErr := file.Close()
			if scanErr == nil {
				scanErr = closeErr
			}
		}
		if scanErr != nil {
			return scanErr
		}
		if kind == "commit" {
			if result.WorkspaceDir == "" {
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "missing-git-context", Offset: 0}
			}
			raw, readErr := readScanBounded(source, 1<<20)
			if readErr != nil {
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "commit-record", Offset: 0}
			}
			provenance, parseErr := backlog.ParseCommitProvenance(raw)
			if parseErr != nil {
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "commit-record", Offset: 0}
			}
			listing, gitErr := scanGitOutput(ctx, result.WorkspaceDir, "rev-list", "--objects", provenance.Base+".."+provenance.Commit, "--")
			if gitErr != nil {
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "git-objects", Offset: 0}
			}
			if err := scanner.scanGitObjects(ctx, result.WorkspaceDir, entry.object.Path, "commit", string(listing)); err != nil {
				return err
			}
		}
		if kind == "bundle" {
			if err := scanner.scanBundle(ctx, result.WorkspaceDir, entry.object.Path, source); err != nil {
				return err
			}
		}
	}
	return nil
}
func (e resultObject) artifactName() string {
	if e.artifact != nil {
		return e.artifact.Name
	}
	return ""
}

var fixtureFingerprint = regexp.MustCompile(`^.{1,4}:[a-f0-9]{12}$`)

func loadSecretAllowlist(workspace string) (map[string]bool, error) {
	allow := map[string]bool{}
	if workspace == "" {
		return allow, nil
	}
	// Refuse symlinked parents as well as a symlinked allowlist.
	dir := filepath.Join(workspace, ".t3")
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return allow, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("unsafe allowlist directory")
	}
	path := filepath.Join(dir, "secret-scan-allow")
	raw, err := readScanBounded(path, 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		return allow, nil
	}
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !fixtureFingerprint.MatchString(line) {
			return nil, errors.New("invalid fixture fingerprint")
		}
		allow[line] = true
	}
	return allow, nil
}
func readScanBounded(path string, limit int64) ([]byte, error) {
	f, err := openRegular(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(raw)) > limit {
		return nil, errors.New("scan input exceeds bound")
	}
	return raw, err
}

// Git stderr can contain credential-bearing filenames or commit messages.
// Never incorporate it (or command stdout on failure) in public errors.
func scanGitOutput(ctx context.Context, repo string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repo}, args...)...)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.New("git scan pipe")
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.New("git scan start")
	}
	raw, readErr := io.ReadAll(io.LimitReader(pipe, 8<<20+1))
	if len(raw) > 8<<20 || readErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	if readErr != nil || waitErr != nil || len(raw) > 8<<20 {
		return nil, errors.New("git scan failed or exceeded bound")
	}
	return raw, nil
}

var gitObjectID = regexp.MustCompile(`^[a-f0-9]{40,64}$`)

func (s *resultScanner) scanGitObjects(ctx context.Context, repo, object, kind, listing string) error {
	var ids strings.Builder
	names := map[string]string{}
	for _, line := range strings.Split(listing, "\n") {
		fields := strings.SplitN(line, " ", 2)
		if !gitObjectID.MatchString(fields[0]) {
			continue
		}
		ids.WriteString(fields[0] + "\n")
		if len(fields) > 1 {
			names[fields[0]] = fields[1]
		}
	}
	cmd := exec.CommandContext(ctx, "git", "-C", repo, "cat-file", "--batch")
	cmd.Stdin = strings.NewReader(ids.String())
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return &SecretScanError{Object: s.safeName(object), Detector: "git-read", Offset: 0}
	}
	if err := cmd.Start(); err != nil {
		return &SecretScanError{Object: s.safeName(object), Detector: "git-read", Offset: 0}
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	reader := bufio.NewReader(pipe)
	for {
		header, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return &SecretScanError{Object: s.safeName(object), Detector: "git-read", Offset: 0}
		}
		fields := strings.Fields(header)
		if len(fields) != 3 {
			return &SecretScanError{Object: s.safeName(object), Detector: "git-read", Offset: 0}
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 {
			return &SecretScanError{Object: s.safeName(object), Detector: "git-read", Offset: 0}
		}
		label := fmt.Sprintf("%s/%s (%s %d bytes)", object, names[fields[0]], fields[1], size)
		// Tree names (including binary files) and commit metadata are scanned too.
		if err := s.scan(object, kind, strings.NewReader(label)); err != nil {
			return err
		}
		if size > s.config.MaxBytes {
			return &SecretScanError{Object: s.safeName(label), Detector: "byte-cap", Offset: s.config.MaxBytes}
		}
		if err := s.scan(label, kind, io.LimitReader(reader, size)); err != nil {
			return err
		}
		if newline, err := reader.ReadByte(); err != nil || newline != '\n' {
			return &SecretScanError{Object: s.safeName(object), Detector: "git-read", Offset: 0}
		}
	}
	err = cmd.Wait()
	waited = true
	if err != nil {
		return &SecretScanError{Object: s.safeName(object), Detector: "git-read", Offset: 0}
	}
	return nil
}
func (s *resultScanner) scanBundle(ctx context.Context, workspace, object, path string) error {
	failure := func() *SecretScanError {
		return &SecretScanError{Object: s.safeName(object), Detector: "bundle-decode", Offset: 0}
	}
	if workspace == "" || path == "" {
		return failure()
	}
	dir, err := os.MkdirTemp("", "steward-secret-bundle-")
	if err != nil {
		return failure()
	}
	defer os.RemoveAll(dir)
	// Borrow prerequisite objects locally; no fetch or network operation.
	repo := filepath.Join(dir, "repo.git")
	if _, err := scanGitOutput(ctx, workspace, "clone", "--shared", "--bare", "--no-hardlinks", "--", workspace, repo); err != nil {
		return failure()
	}
	if _, err := scanGitOutput(ctx, repo, "bundle", "unbundle", path); err != nil {
		return failure()
	}
	indexes, err := filepath.Glob(filepath.Join(repo, "objects", "pack", "*.idx"))
	if err != nil || len(indexes) == 0 {
		return failure()
	}
	for _, index := range indexes {
		raw, err := scanGitOutput(ctx, repo, "verify-pack", "-v", index)
		if err != nil {
			return failure()
		}
		var listing strings.Builder
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 5 && gitObjectID.MatchString(fields[0]) {
				listing.WriteString(fields[0] + "\n")
			}
		}
		if err := s.scanGitObjects(ctx, repo, object, "bundle", listing.String()); err != nil {
			return err
		}
	}
	return nil
}
