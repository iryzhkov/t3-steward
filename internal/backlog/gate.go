package backlog

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// GateReport is worker-owned evidence, captured outside the producer's turn.
type GateReport struct {
	Commands         []GateCommandResult `json:"commands"`
	TreeHash         string              `json:"treeHash"`
	Worker           string              `json:"worker"`
	ToolVersions     map[string]string   `json:"toolVersions"`
	StartedAt        time.Time           `json:"startedAt"`
	CompletedAt      time.Time           `json:"completedAt"`
	Cached           bool                `json:"cached"`
	OriginalAttempt  string              `json:"originalAttempt"`
	CacheKey         string              `json:"cacheKey"`
	Passed           bool                `json:"passed"`
	Failure          *GateFailure        `json:"failure,omitempty"`
	LogTruncated     bool                `json:"logTruncated"`
	LogArtifact      string              `json:"logArtifact"`
	OutputLimitation string              `json:"outputLimitation,omitempty"`
	// attestedCommit is this attempt's HEAD commit, whose tree the gate
	// attests. It is not evidence, since a cached report may come from another
	// commit with the same tree. Finalize publishes a declared commit only if
	// it still resolves to this one.
	attestedCommit string
}
type GateCommandResult struct {
	Command     string        `json:"command"`
	ExitCode    int           `json:"exitCode"`
	StartedAt   time.Time     `json:"startedAt"`
	CompletedAt time.Time     `json:"completedAt"`
	Duration    time.Duration `json:"duration"`
	Error       string        `json:"error,omitempty"`
}
type GateFailure struct {
	Command  string `json:"command"`
	ExitCode int    `json:"exitCode"`
	Reason   string `json:"reason"`
}

const gateLogLimit = 1 << 20

type gateCacheRecord struct {
	Report GateReport `json:"report"`
	Log    []byte     `json:"log"`
}

func (f AttemptFinalizer) runGate(ctx context.Context, req AttemptFinalization) (GateReport, []byte, error) {
	gate := req.Task.Gate
	report := GateReport{StartedAt: f.now(), OriginalAttempt: req.Attempt.ID, Worker: req.WorkerID, LogArtifact: "gate/log.txt", ToolVersions: map[string]string{}}
	if report.Worker == "" {
		report.Worker = "local"
	}
	fail := func(command string, code int, reason string) (GateReport, []byte, error) {
		report.CompletedAt = f.now()
		report.Failure = &GateFailure{Command: command, ExitCode: code, Reason: gateStructuredReason(reason)}
		log := boundedBuffer{limit: gateLogLimit}
		_, _ = log.Write([]byte(reason + "\n"))
		report.LogTruncated = log.truncated
		raw := []byte(log.String())
		if log.truncated {
			raw = append(raw, []byte("\n[output truncated; structured result: gate; log: gate/log.txt]\n")...)
		}
		return report, raw, nil
	}
	if err := gate.Validate(); err != nil {
		return fail("gate", 1, err.Error())
	}
	if gate.Timeout <= 0 || gate.Timeout > 6*time.Hour || (f.GateTimeoutMax > 0 && gate.Timeout > f.GateTimeoutMax) || len(gate.Commands) == 0 {
		return fail("gate", 1, "invalid gate commands or timeout exceeds worker maximum")
	}
	for _, command := range gate.Commands {
		if strings.TrimSpace(command) == "" || strings.ContainsRune(command, 0) {
			return fail("gate", 1, "invalid gate command")
		}
	}
	tree, err := gateGit(ctx, req.WorkspaceDir, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return fail("git rev-parse HEAD^{tree}", 1, err.Error())
	}
	report.TreeHash = strings.TrimSpace(tree)
	if _, err = hex.DecodeString(report.TreeHash); err != nil || (len(report.TreeHash) != 40 && len(report.TreeHash) != 64) {
		return fail("git rev-parse HEAD^{tree}", 1, "invalid commit tree hash")
	}
	head, reason, err := gateDeclaredCommitsAtHead(ctx, req)
	if err != nil {
		return fail("git rev-parse", 1, err.Error())
	} else if reason != "" {
		return fail("git rev-parse", 1, reason)
	}
	report.attestedCommit = head
	clean, err := gateCleanTree(ctx, req)
	if err != nil {
		return fail("git status", 1, err.Error())
	}
	if !clean {
		return fail("git status", 1, "gate requires an unchanged tracked tree and no undeclared untracked files")
	}
	tools, err := gateToolVersions(ctx)
	if err != nil {
		return fail("toolchain", 1, err.Error())
	}
	report.ToolVersions = tools
	report.ToolVersions["lane"] = f.GateToolchainIdentity
	if report.ToolVersions["lane"] == "" {
		report.ToolVersions["lane"] = "local"
	}
	if f.GateCacheDisabled {
		report.OutputLimitation = "Contained supervisor exposes exit status only; gate/log.txt retains the invocation receipt, not command stdout/stderr. Tool versions describe the host; cache reuse is disabled for this lane."
	}
	keyBytes, _ := json.Marshal(struct {
		Tree     string
		Commands []string
		Timeout  time.Duration
		Tools    map[string]string
	}{report.TreeHash, gate.Commands, gate.Timeout, tools})
	key := sha256.Sum256(keyBytes)
	report.CacheKey = hex.EncodeToString(key[:])
	cacheRoot := filepath.Join(f.StorageRoot, "gate-cache")
	lock, err := acquireFileLock(ctx, cacheRoot, report.CacheKey)
	if err != nil {
		return report, nil, err
	}
	defer lock.Close()
	cachePath := filepath.Join(cacheRoot, report.CacheKey+".json")
	if f.GateCacheAge > 0 && !f.GateCacheDisabled {
		cached, ok := readGateCache(cachePath, report.CacheKey, f.now(), f.GateCacheAge)
		// The record is only a pointer: it is reused when the coordinator
		// attests that its original attempt passed here, and the coordinator
		// checks the replayed report against the one it recorded.
		if ok && !cached.Report.Cached && cached.Report.OriginalAttempt != req.Attempt.ID && slices.Contains(f.GateCacheOrigins, cached.Report.OriginalAttempt) {
			// Recheck after taking the cross-process lock: another finalizer could have
			// waited while the workspace was changed.
			clean, err = gateCleanTree(ctx, req)
			current, treeErr := gateGit(ctx, req.WorkspaceDir, "rev-parse", "HEAD^{tree}")
			currentHead, moved, headErr := gateDeclaredCommitsAtHead(ctx, req)
			if err == nil && treeErr == nil && headErr == nil && clean && moved == "" && (!gateDeclaresCommit(req) || currentHead == head) && strings.TrimSpace(current) == report.TreeHash {
				cached.Report.Cached = true
				cached.Report.Worker = report.Worker
				cached.Report.attestedCommit = head
				return cached.Report, cached.Log, nil
			}
			return fail("git status", 1, "workspace changed while waiting for gate cache")
		}
	}
	log := boundedBuffer{limit: gateLogLimit}
	for index, command := range gate.Commands {
		started := f.now()
		fmt.Fprintf(&log, "$ %s\n", command)
		commandCtx, cancel := context.WithTimeout(ctx, gate.Timeout)
		result, runErr := f.processRunner().Run(commandCtx, ProcessRequest{
			ID: fmt.Sprintf("verify-%s-gate-%d", req.Attempt.ID, index), Dir: req.WorkspaceDir, Program: "/bin/sh",
			Args: []string{"-c", `umask 022 && exec "$0" "$@"`, "/bin/sh", "-c", command},
			Log:  &log, MaxOutputBytes: gateLogLimit, Timeout: gate.Timeout,
		})
		deadlineErr := commandCtx.Err()
		cancel()
		// Some fixture/custom runners expose only Output, while production runners
		// also write their log. Keep Output when no bytes were written for it.
		if result.Output != "" && !strings.Contains(log.String(), result.Output) {
			_, _ = log.Write([]byte(result.Output))
		}
		code := result.ExitCode
		reason := ""
		if runErr != nil {
			if ctx.Err() != nil {
				return report, []byte(log.String()), ctx.Err()
			}
			var exitErr *ProcessExitError
			switch {
			case errors.Is(deadlineErr, context.DeadlineExceeded):
				code = 124
				reason = "gate command timeout"
			case errors.As(runErr, &exitErr):
				code = exitErr.ExitCode
				reason = exitErr.Error()
			default:
				code = 1
				reason = runErr.Error()
			}
		}
		if len(reason) > 16000 {
			fmt.Fprintln(&log, reason)
		}
		reason = gateStructuredReason(reason)
		if code != 0 && reason == "" {
			reason = fmt.Sprintf("command exited %d", code)
		}
		completed := f.now()
		report.Commands = append(report.Commands, GateCommandResult{Command: command, ExitCode: code, StartedAt: started, CompletedAt: completed, Duration: completed.Sub(started), Error: reason})
		report.LogTruncated = report.LogTruncated || result.Truncated || log.truncated
		fmt.Fprintf(&log, "\nexit %d\n", code)
		if code != 0 {
			report.Failure = &GateFailure{Command: command, ExitCode: code, Reason: reason}
			break
		}
	}
	report.CompletedAt = f.now()
	if report.Failure == nil {
		clean, err = gateCleanTree(ctx, req)
		finalTree, treeErr := gateGit(ctx, req.WorkspaceDir, "rev-parse", "HEAD^{tree}")
		finalHead, moved, headErr := gateDeclaredCommitsAtHead(ctx, req)
		switch {
		case err != nil || treeErr != nil || !clean || strings.TrimSpace(finalTree) != report.TreeHash:
			report.Failure = &GateFailure{Command: "git status", ExitCode: 1, Reason: "gate changed the committed tree or workspace"}
		case headErr != nil || moved != "" || (gateDeclaresCommit(req) && finalHead != head):
			// A command that moves HEAD or a declared revision would otherwise
			// have an ungated commit published under this passing report. A task
			// that publishes no commit keeps the tree-only check.
			reason := "HEAD or a declared commit revision moved during the gate"
			if moved != "" {
				reason += ": " + moved
			}
			report.Failure = &GateFailure{Command: "git rev-parse", ExitCode: 1, Reason: reason}
		default:
			report.Passed = true
		}
	}
	// Enforce the transport/evidence cap before writing reusable cache evidence.
	// Unusual tool output must become an importable failure, never poison a run.
	structured, marshalErr := json.Marshal(report)
	if marshalErr != nil {
		return report, nil, marshalErr
	}
	if len(structured) > GateEvidenceMaxBytes {
		fmt.Fprintf(&log, "\n[gate metadata exceeded coordinator limit]\n%s\n", structured)
		report.Passed = false
		report.Failure = &GateFailure{Command: "gate metadata", ExitCode: 1, Reason: "structured gate metadata exceeds coordinator limit; see gate/log.txt"}
		report.Commands = nil
		report.ToolVersions = map[string]string{}
		report.CacheKey = ""
	}
	report.LogTruncated = report.LogTruncated || log.truncated
	rawLog := []byte(log.String())
	if report.LogTruncated {
		rawLog = append(rawLog, []byte("\n[output truncated at 1 MiB; structured result: gate; retained log: gate/log.txt]\n")...)
	}
	if report.Passed && f.GateCacheAge > 0 && !f.GateCacheDisabled {
		raw, err := json.Marshal(gateCacheRecord{Report: report, Log: rawLog})
		if err != nil {
			return report, rawLog, err
		}
		file, err := os.CreateTemp(cacheRoot, ".gate-")
		if err != nil {
			return report, rawLog, err
		}
		path := file.Name()
		defer os.Remove(path)
		if _, err = file.Write(raw); err == nil {
			err = file.Sync()
		}
		err = errors.Join(err, file.Close())
		if err != nil {
			return report, rawLog, err
		}
		if err = os.Rename(path, cachePath); err != nil {
			return report, rawLog, err
		}
		dir, err := os.Open(cacheRoot)
		if err != nil {
			return report, rawLog, err
		}
		err = errors.Join(dir.Sync(), dir.Close())
		if err != nil {
			return report, rawLog, err
		}
	}
	return report, rawLog, nil
}

// gatedOutputMatches reports why a captured declared output that is a regular
// file tracked in the gated commit differs from that commit's content, or ""
// when it matches or is not tracked there.
func gatedOutputMatches(ctx context.Context, workspace, commit, relative, captured string) (string, error) {
	entry, err := gateGit(ctx, workspace, "ls-tree", "-z", commit, "--", filepath.ToSlash(relative))
	if err != nil {
		return "", err
	}
	meta, _, found := strings.Cut(strings.TrimSuffix(entry, "\x00"), "\t")
	fields := strings.Fields(meta)
	if !found || len(fields) != 3 || fields[1] != "blob" || (fields[0] != "100644" && fields[0] != "100755") {
		return "", nil
	}
	got, err := gateGit(ctx, workspace, "hash-object", "--no-filters", "--", captured)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(got) != fields[2] {
		return "changed after the gate: it differs from the gated commit " + commit, nil
	}
	return "", nil
}

func gateDeclaresCommit(req AttemptFinalization) bool {
	return slices.ContainsFunc(req.Task.Outputs, func(output domain.ArtifactDeclaration) bool { return output.Commit != nil })
}

// gateDeclaredCommitsAtHead requires every declared commit to be HEAD, the
// commit whose tree the gate attests, and returns HEAD's commit. Otherwise a
// task could gate one commit and publish another under the declared revision.
func gateDeclaredCommitsAtHead(ctx context.Context, req AttemptFinalization) (string, string, error) {
	resolved, err := gateGit(ctx, req.WorkspaceDir, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", "", err
	}
	head := strings.TrimSpace(resolved)
	for _, output := range req.Task.Outputs {
		if output.Commit == nil {
			continue
		}
		revision := output.Commit.Revision
		if revision == "" {
			revision = "HEAD"
		}
		if strings.HasPrefix(revision, "-") {
			return head, fmt.Sprintf("declared commit %q has an invalid revision", output.Name), nil
		}
		resolved, err := gateGit(ctx, req.WorkspaceDir, "rev-parse", "--verify", revision+"^{commit}")
		if err != nil {
			return head, fmt.Sprintf("declared commit %q revision %q does not resolve", output.Name, revision), nil
		}
		if strings.TrimSpace(resolved) != head {
			return head, fmt.Sprintf("gate attests HEAD, but declared commit %q revision %q is a different commit", output.Name, revision), nil
		}
	}
	return head, "", nil
}

func gateStructuredReason(reason string) string {
	reason = strings.ToValidUTF8(reason, "?")
	if len(reason) <= 16000 {
		return reason
	}
	buffer := boundedBuffer{limit: 16000}
	_, _ = buffer.Write([]byte(reason))
	return buffer.String() + " [truncated; see gate/log.txt]"
}

func readGateCache(path, key string, now time.Time, age time.Duration) (gateCacheRecord, bool) {
	var record gateCacheRecord
	file, err := os.Open(path)
	if err != nil {
		return record, false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 2*gateLogLimit {
		return record, false
	}
	decoder := json.NewDecoder(io.LimitReader(file, 2*gateLogLimit))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&record) != nil {
		return record, false
	}
	var extra json.RawMessage
	if decoder.Decode(&extra) != io.EOF {
		return record, false
	}
	r := record.Report
	if r.CacheKey != key || !r.Passed || r.Failure != nil || r.CompletedAt.IsZero() || r.CompletedAt.After(now) || now.Sub(r.CompletedAt) > age || r.OriginalAttempt == "" || len(r.Commands) == 0 {
		return record, false
	}
	for _, c := range r.Commands {
		if c.ExitCode != 0 || c.StartedAt.IsZero() || c.CompletedAt.Before(c.StartedAt) {
			return record, false
		}
	}
	return record, true
}
func gateGit(ctx context.Context, dir string, args ...string) (string, error) {
	// Metadata runs on the worker even for contained gates. Never allow a
	// repository's fsmonitor, hook or conversion filter to execute on the host.
	// Replace refs would let the attested tree differ from the commit that is
	// published, which is read without them.
	base := []string{"--no-replace-objects", "--work-tree=" + dir, "-c", "core.bare=false", "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false", "-c", "core.hooksPath=/dev/null"}
	keys, err := gateMetadataCommand(ctx, dir, "git", append(append([]string(nil), base...), "config", "--null", "--name-only", "--get-regexp", `^filter\..*\.(clean|process|required)$`)...)
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			return "", err
		}
	}
	for _, key := range strings.Split(keys, "\x00") {
		if key == "" {
			continue
		}
		if !gateFilterKey.MatchString(key) {
			return "", errors.New("repository filter configuration has an unsupported key")
		}
		value := ""
		if strings.HasSuffix(key, ".required") {
			value = "false"
		}
		base = append(base, "-c", key+"="+value)
	}
	return gateMetadataCommandBounded(ctx, dir, "git", gateLogLimit, append(base, args...)...)
}

var gateFilterKey = regexp.MustCompile(`^filter\.[A-Za-z0-9_.-]+\.(clean|process|required)$`)

func gateMetadataCommand(ctx context.Context, dir, program string, args ...string) (string, error) {
	return gateMetadataCommandBounded(ctx, dir, program, 64<<10, args...)
}
func gateMetadataCommandBounded(ctx context.Context, dir, program string, limit int, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	output := boundedBuffer{limit: limit}
	command := exec.CommandContext(ctx, program, args...)
	command.Dir = dir
	command.WaitDelay = time.Second
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("%s: %w: %s", program, err, output.String())
	}
	if output.truncated {
		return "", fmt.Errorf("%s metadata exceeds bound", program)
	}
	return output.String(), nil
}
func gateCleanTree(ctx context.Context, req AttemptFinalization) (bool, error) {
	// Submodule worktrees are not described by the outer commit tree alone.
	// Reject this unsupported layout rather than attest or cache a different tree.
	if _, err := os.Lstat(filepath.Join(req.WorkspaceDir, ".gitmodules")); err == nil {
		return false, errors.New("worker-owned gate requires a repository without submodules")
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	flags, err := gateGit(ctx, req.WorkspaceDir, "ls-files", "-v", "-z")
	if err != nil {
		return false, err
	}
	for _, entry := range strings.Split(flags, "\x00") {
		if entry != "" && (entry[0] == 'S' || (entry[0] >= 'a' && entry[0] <= 'z')) {
			return false, errors.New("gate cannot attest assume-unchanged or skip-worktree index entries")
		}
	}
	clean, err := gateCommittedBytes(ctx, req.WorkspaceDir)
	if err != nil || !clean {
		return false, err
	}
	status, err := gateGit(ctx, req.WorkspaceDir, "status", "--porcelain", "--untracked-files=no", "--ignore-submodules=all")
	if err != nil || status != "" {
		return false, err
	}
	others, err := gateGit(ctx, req.WorkspaceDir, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return false, err
	}
	allowed := map[string]bool{}
	for _, output := range req.Task.Outputs {
		if output.Commit == nil {
			allowed[filepath.ToSlash(output.Name)] = true
		}
	}
	for _, name := range strings.Split(others, "\x00") {
		if name != "" && !strings.HasPrefix(name, ".t3/") && !allowed[name] {
			return false, nil
		}
	}
	return true, nil
}

func gateCommittedBytes(ctx context.Context, workspace string) (bool, error) {
	listing, err := gateGit(ctx, workspace, "ls-tree", "-r", "-z", "HEAD")
	if err != nil {
		return false, err
	}
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return false, err
	}
	defer root.Close()
	for _, entry := range strings.Split(listing, "\x00") {
		if entry == "" {
			continue
		}
		metadata, name, ok := strings.Cut(entry, "\t")
		fields := strings.Fields(metadata)
		if !ok || len(fields) != 3 || validateRelativePath(name, false) != nil {
			return false, errors.New("invalid committed gate path")
		}
		mode, kind, digest := fields[0], fields[1], fields[2]
		if mode == "160000" || kind == "commit" {
			return false, errors.New("worker-owned gate requires a repository without gitlinks")
		}
		info, err := root.Lstat(name)
		if err != nil {
			return false, err
		}
		var h hash.Hash
		switch len(digest) {
		case 40:
			h = sha1.New()
		case 64:
			h = sha256.New()
		default:
			return false, errors.New("invalid committed blob hash")
		}
		if mode == "120000" {
			if info.Mode()&os.ModeSymlink == 0 {
				return false, nil
			}
			target, err := root.Readlink(name)
			if err != nil {
				return false, err
			}
			fmt.Fprintf(h, "blob %d\x00", len(target))
			_, _ = io.WriteString(h, target)
		} else {
			if !info.Mode().IsRegular() || kind != "blob" || (mode != "100644" && mode != "100755") {
				return false, nil
			}
			if (mode == "100755") != (info.Mode().Perm()&0111 != 0) {
				return false, nil
			}
			file, err := root.Open(name)
			if err != nil {
				return false, err
			}
			actual, statErr := file.Stat()
			if statErr != nil {
				file.Close()
				return false, statErr
			}
			fmt.Fprintf(h, "blob %d\x00", actual.Size())
			_, copyErr := io.Copy(h, gateContextReader{ctx: ctx, reader: file})
			closeErr := file.Close()
			if err = errors.Join(copyErr, closeErr); err != nil {
				return false, err
			}
		}
		if hex.EncodeToString(h.Sum(nil)) != digest {
			return false, nil
		}
	}
	return true, nil
}

type gateContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r gateContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func gateToolVersions(ctx context.Context) (map[string]string, error) {
	tools := map[string]string{"workerRuntime": runtime.Version() + "/" + runtime.GOOS + "/" + runtime.GOARCH}
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(binary)
	if err != nil {
		return nil, err
	}
	workerHash := sha256.New()
	_, err = io.Copy(workerHash, file)
	err = errors.Join(err, file.Close())
	if err != nil {
		return nil, err
	}
	tools["workerExecutableSHA256"] = hex.EncodeToString(workerHash.Sum(nil))
	for _, tool := range []struct {
		name string
		args []string
	}{{"git", []string{"--version"}}, {"go", []string{"version"}}, {"make", []string{"--version"}}, {"/bin/sh", nil}} {
		path, err := exec.LookPath(tool.name)
		if err != nil {
			if errors.Is(err, exec.ErrNotFound) {
				tools[tool.name] = "unavailable"
				continue
			}
			return nil, err
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		sum := sha256.New()
		_, err = io.Copy(sum, file)
		err = errors.Join(err, file.Close())
		if err != nil {
			return nil, err
		}
		identity := path + " sha256:" + hex.EncodeToString(sum.Sum(nil))
		if tool.args != nil {
			version, err := gateMetadataCommand(ctx, "", path, tool.args...)
			if err != nil {
				return nil, err
			}
			identity += " " + strings.TrimSpace(version)
		}
		if len(identity) > 3500 {
			full := sha256.Sum256([]byte(identity))
			buffer := boundedBuffer{limit: 3500}
			_, _ = buffer.Write([]byte(strings.ToValidUTF8(identity, "?")))
			identity = buffer.String() + " identity-sha256:" + hex.EncodeToString(full[:])
		}
		tools[tool.name] = identity
	}
	tools["environmentSHA256"] = gateEnvironmentDigest(os.Environ())
	return tools, nil
}

// gateEnvironmentNames and gateEnvironmentPrefixes name the inherited
// variables that can change what a gate command builds or how it runs.
//
// The digest covers only these. Hashing the whole environment made the cache
// useless across service restarts: systemd sets INVOCATION_ID, JOURNAL_STREAM,
// SYSTEMD_EXEC_PID, MANAGERPID and MEMORY_PRESSURE_WATCH afresh on every start,
// and a desktop session adds its own instance signatures, so an unchanged tree
// missed after every restart or converge.
var (
	gateEnvironmentNames = map[string]struct{}{
		"PATH": {}, "HOME": {}, "SHELL": {}, "TMPDIR": {}, "TZ": {}, "LANG": {}, "LANGUAGE": {},
		// The gate shell itself: bash as /bin/sh imports these.
		"SHELLOPTS": {}, "BASHOPTS": {}, "CDPATH": {}, "BASH_ENV": {}, "ENV": {},
		"CC": {}, "CXX": {}, "AR": {}, "CFLAGS": {}, "CPPFLAGS": {}, "CXXFLAGS": {}, "LDFLAGS": {},
		"MAKEFLAGS": {}, "GNUMAKEFLAGS": {}, "MFLAGS": {}, "MAKEFILES": {},
		"LD_LIBRARY_PATH": {}, "LD_PRELOAD": {}, "PKG_CONFIG_PATH": {}, "PKG_CONFIG_LIBDIR": {},
		"XDG_CACHE_HOME": {}, "XDG_CONFIG_HOME": {}, "XDG_DATA_HOME": {},
	}
	gateEnvironmentPrefixes = []string{
		"GO", "CGO_", "LC_", "GIT_",
		// Toolchains a gate command may run besides Go.
		"PYTHON", "NODE_", "NPM_CONFIG_", "npm_config_", "CARGO_", "RUST", "JAVA_",
	}
)

// gateEnvironmentDigest hashes the allowlisted part of env. Only a digest is
// stored, because even allowlisted values may carry credentials.
func gateEnvironmentDigest(env []string) string {
	// A child process sees the last of duplicate variables, so the last wins
	// here too; otherwise two different effective values could share a key.
	last := make(map[string]string)
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if _, ok := gateEnvironmentNames[name]; ok || slices.ContainsFunc(gateEnvironmentPrefixes, func(prefix string) bool {
			return strings.HasPrefix(name, prefix)
		}) {
			last[name] = entry
		}
	}
	selected := make([]string, 0, len(last))
	for _, entry := range last {
		selected = append(selected, entry)
	}
	sort.Strings(selected)
	sum := sha256.Sum256([]byte(strings.Join(selected, "\x00")))
	return hex.EncodeToString(sum[:])
}
