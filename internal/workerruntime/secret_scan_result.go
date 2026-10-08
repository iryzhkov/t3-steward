package workerruntime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// executionScanner builds the scanner for one execution: its current
// credentials, their recorded history and the trusted baseline allowlist.
func (s *CustodyStore) executionScanner(ctx context.Context, pkg workerproto.ExecutionPackage) (*resultScanner, *scanBaseline, error) {
	config := s.config.SecretScan
	canaries, err := s.resolveCanaries(ctx, pkg)
	if err != nil {
		return nil, nil, err
	}
	scanner := newResultScanner(config, canaries, nil)
	if scanner.skipped > 0 {
		logger := config.Log
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("result secret scan skips execution credentials too short to match", "attempt", pkg.Identity.AttemptID, "count", scanner.skipped, "minimum_bytes", minCanaryBytes)
	}
	if err := s.addSecretHistory(pkg, scanner); err != nil {
		return nil, nil, &SecretScanError{Object: "execution", Detector: "credential-history", Offset: 0}
	}
	// The allowlist comes only from the baseline recorded before the task
	// ran; the workspace copy is task-writable and never consulted.
	baseline, err := s.loadScanBaseline(pkg)
	if err != nil {
		return nil, nil, &SecretScanError{Object: "execution", Detector: "scan-baseline", Offset: 0}
	}
	scanner.allow = baseline.allowSet()
	return scanner, baseline, nil
}

// resolveCanaries resolves the execution's current credentials and records
// them in its credential history before they are used. A value the provider
// refreshed after dispatch is then known to every later scan, even when the
// task deletes or rewrites the login file it was read from.
func (s *CustodyStore) resolveCanaries(ctx context.Context, pkg workerproto.ExecutionPackage) ([]string, error) {
	if s.config.SecretScan.Canaries == nil {
		return nil, nil
	}
	canaries, err := s.config.SecretScan.Canaries(ctx, pkg)
	if err != nil {
		return nil, &SecretScanError{Object: "execution", Detector: "credential-resolution", Offset: 0}
	}
	if err := s.RecordSecretValues(ctx, pkg, canaries); err != nil {
		return nil, err
	}
	return canaries, nil
}

// RedactText replaces every known credential of the execution, in any
// encoding the scanner recognizes, and every secret pattern in text, so the
// text can leave the worker as coordinator metadata. A failed credential
// resolution or history read fails the redaction, so the caller withholds the
// text: the recorded history does not cover a credential that was never
// recorded, such as a protocol credential quoted by a reason an earlier
// release journaled, and a partial canary set cannot prove the text clean.
func (s *CustodyStore) RedactText(ctx context.Context, pkg workerproto.ExecutionPackage, text string) (string, error) {
	canaries, err := s.resolveCanaries(ctx, pkg)
	if err != nil {
		return "", err
	}
	scanner := newResultScanner(s.config.SecretScan, canaries, nil)
	if err := s.addSecretHistory(pkg, scanner); err != nil {
		return "", &SecretScanError{Object: "execution", Detector: "credential-history", Offset: 0}
	}
	return scanner.safeName(text), nil
}

func (s *CustodyStore) scanResult(ctx context.Context, pkg workerproto.ExecutionPackage, result PublishedResult, planned []resultObject) error {
	scanner, baseline, err := s.executionScanner(ctx, pkg)
	if err != nil {
		return err
	}
	declarations := map[string]bool{}
	for _, output := range pkg.Outputs {
		if output.Commit != nil {
			declarations[output.Name] = true
			declarations[backlog.FailedCommitArtifactName(output.Name)] = true
		}
	}
	var bundleRoot string
	defer func() {
		if bundleRoot != "" {
			_ = os.RemoveAll(bundleRoot)
		}
	}()
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
		var record bytes.Buffer
		var retained *os.File
		if kind == "commit" {
			if entry.object.Size > 1<<20 {
				if file != nil {
					_ = file.Close()
				}
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "commit-record", Offset: 0}
			}
			reader = io.TeeReader(reader, &record)
		}
		if kind == "bundle" {
			if bundleRoot == "" {
				bundleRoot, err = os.MkdirTemp(s.config.Root, ".secret-scan-")
			}
			if err == nil {
				retained, err = os.CreateTemp(bundleRoot, "bundle-")
			}
			if err != nil {
				if file != nil {
					_ = file.Close()
				}
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "bundle-retain", Offset: 0}
			}
			source = retained.Name()
			reader = io.TeeReader(reader, retained)
		}
		digest := sha256.New()
		count := &scanCountingReader{reader: io.TeeReader(reader, digest)}
		scanErr := scanner.scan(entry.object.Path, kind, strings.NewReader(entry.object.Path))
		if scanErr == nil {
			scanErr = scanner.scan(entry.object.Path, kind, count)
		}
		if scanErr == nil && (count.bytes != entry.object.Size || hex.EncodeToString(digest.Sum(nil)) != strings.ToLower(entry.object.SHA256)) {
			scanErr = &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "object-digest", Offset: count.bytes}
		}
		if file != nil {
			closeErr := file.Close()
			if scanErr == nil && closeErr != nil {
				scanErr = &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "object-read", Offset: count.bytes}
			}
		}
		if retained != nil {
			if err := retained.Close(); scanErr == nil && err != nil {
				scanErr = &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "bundle-retain", Offset: count.bytes}
			}
		}
		if scanErr != nil {
			return scanErr
		}
		if kind == "commit" {
			if result.WorkspaceDir == "" {
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "missing-git-context", Offset: 0}
			}
			provenance, parseErr := backlog.ParseCommitProvenance(record.Bytes())
			if parseErr != nil {
				return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "commit-record", Offset: 0}
			}
			// The recorded base bounds the range even when the task rewrote
			// .t3/base-commit, which the provenance base was read from.
			bases := []string{provenance.Base}
			if baseline != nil && baseline.Base != "" && baseline.Base != provenance.Base {
				bases = []string{baseline.Base, provenance.Base}
			}
			for _, base := range bases {
				listing, gitErr := scanCommitListing(ctx, result.WorkspaceDir, base, provenance.Commit)
				if gitErr != nil {
					return &SecretScanError{Object: scanner.safeName(entry.object.Path), Detector: "git-objects", Offset: 0}
				}
				if err := scanner.scanGitObjects(ctx, result.WorkspaceDir, entry.object.Path, "commit", string(listing)); err != nil {
					return err
				}
			}
		}
		if kind == "bundle" {
			scanner.tempRoot = bundleRoot
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

func parseSecretAllowlist(raw []byte) (map[string]bool, error) {
	allow := map[string]bool{}
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
	cmd := scanGitCommand(ctx, repo, args...)
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

// scanGitCommand reads objects as stored. Replace refs, grafts, shallow
// markers and commit-graph files live in the task-writable repository and
// could otherwise hide content or history from the scan.
func scanGitCommand(ctx context.Context, repo string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", append([]string{"--no-replace-objects", "-c", "core.commitGraph=false", "-C", repo}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_NO_REPLACE_OBJECTS=1", "GIT_GRAFT_FILE="+os.DevNull, "GIT_SHALLOW_FILE="+os.DevNull)
	// Git is stopped when ctx ends; nothing it started may keep the scan
	// waiting on its output after that.
	cmd.WaitDelay = 5 * time.Second
	return cmd
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
	cmd := scanGitCommand(ctx, repo, "cat-file", "--batch")
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
		name := names[fields[0]]
		if name == "" {
			name = fields[0]
		}
		label := fmt.Sprintf("%s/%s (%s %d bytes)", object, name, fields[1], size)
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

// bundleHeader reads the object format the bundle at path declares, sha256
// when its header says so and otherwise sha1, and the prerequisite commits it
// lists. A bundle whose header cannot be read is refused by the decode that
// follows.
func bundleHeader(path string) (format string, prerequisites []string) {
	format = "sha1"
	f, err := openRegular(path)
	if err != nil {
		return format, nil
	}
	defer f.Close()
	reader := bufio.NewReader(io.LimitReader(f, 1<<20))
	for index := 0; ; index++ {
		line, err := reader.ReadString('\n')
		line = strings.TrimSuffix(line, "\n")
		if err != nil || (index > 0 && line == "") {
			return format, prerequisites
		}
		if line == "@object-format=sha256" {
			format = "sha256"
		}
		if oid, ok := strings.CutPrefix(line, "-"); ok {
			if oid, _, _ = strings.Cut(oid, " "); gitObjectID.MatchString(oid) {
				prerequisites = append(prerequisites, oid)
			}
		}
	}
}

func (s *resultScanner) scanBundle(ctx context.Context, workspace, object, path string) error {
	failure := func() *SecretScanError {
		return &SecretScanError{Object: s.safeName(object), Detector: "bundle-decode", Offset: 0}
	}
	if workspace == "" || path == "" {
		return failure()
	}
	dir, err := os.MkdirTemp(s.tempRoot, "steward-secret-bundle-")
	if err != nil {
		return failure()
	}
	defer os.RemoveAll(dir)
	// The prerequisite objects are borrowed from the workspace through an
	// alternate, with no fetch or network operation. Git never runs in the
	// task's repository, whose configuration, HEAD and refs the task wrote and
	// can make Git wait on. The objects are still the task's, and Git waits on
	// a FIFO among them too, so the decode has its own time limit whatever the
	// caller's context; running out of it refuses the bundle.
	timeout := s.config.DecodeTimeout
	if timeout <= 0 {
		timeout = DefaultBundleDecodeTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	repo := filepath.Join(dir, "repo.git")
	format, prerequisites := bundleHeader(path)
	if _, err := scanGitOutput(ctx, dir, "init", "-q", "--bare", "--object-format="+format, "--", repo); err != nil {
		return failure()
	}
	objects := filepath.Join(workspace, ".git", "objects")
	if err := os.WriteFile(filepath.Join(repo, "objects", "info", "alternates"), []byte(objects+"\n"), 0o600); err != nil {
		return failure()
	}
	// Refs at the prerequisites end the unbundle's connectivity walk there,
	// as the task's own refs did when the workspace was cloned. A missing
	// prerequisite fails here or in the unbundle.
	for index, prerequisite := range prerequisites {
		if _, err := scanGitOutput(ctx, repo, "update-ref", fmt.Sprintf("refs/prerequisites/%d", index), prerequisite); err != nil {
			return failure()
		}
	}
	// The unbundle would otherwise list the alternate's refs by running Git
	// in the task's repository.
	if _, err := scanGitOutput(ctx, repo, "-c", "core.alternateRefsCommand=true", "bundle", "unbundle", path); err != nil {
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
