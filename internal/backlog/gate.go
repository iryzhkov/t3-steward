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
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// GateReport is worker-owned evidence, captured outside the producer's turn.
type GateReport struct {
	Commands     []GateCommandResult `json:"commands"`
	TreeHash     string              `json:"treeHash"`
	Worker       string              `json:"worker"`
	ToolVersions map[string]string   `json:"toolVersions"`
	StartedAt    time.Time           `json:"startedAt"`
	CompletedAt  time.Time           `json:"completedAt"`
	// Attempt is the attempt whose finalization ran these commands. Every
	// report is produced by its own attempt; a gate result is never reused.
	Attempt          string       `json:"attempt"`
	Passed           bool         `json:"passed"`
	Failure          *GateFailure `json:"failure,omitempty"`
	LogTruncated     bool         `json:"logTruncated"`
	LogArtifact      string       `json:"logArtifact"`
	OutputLimitation string       `json:"outputLimitation,omitempty"`
	// attestedCommit is this attempt's HEAD commit, whose tree the gate
	// attests. Finalize publishes a declared commit only if it still resolves
	// to this one.
	attestedCommit string
	// outputDigests holds the SHA-256 of each declared file output as the
	// gate saw it, by declared name; an output absent then has no entry.
	// Finalize refuses a capture that differs, tracked in Git or not.
	outputDigests map[string]string
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

func (f AttemptFinalizer) runGate(ctx context.Context, req AttemptFinalization) (GateReport, []byte, error) {
	gate := req.Task.Gate
	report := GateReport{StartedAt: f.now(), Attempt: req.Attempt.ID, Worker: req.WorkerID, LogArtifact: "gate/log.txt", ToolVersions: map[string]string{}}
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
	if f.GateContained {
		report.OutputLimitation = "Contained supervisor exposes exit status only; gate/log.txt retains the invocation receipt, not command stdout/stderr. Tool versions describe the host."
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
		// Taken before the final checks, so a tracked output that changes in
		// between fails them and one that changes afterwards differs at capture.
		report.outputDigests = gateOutputDigests(req)
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
	// Enforce the transport/evidence cap here: unusual tool output must become
	// an importable failure, never poison a run.
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
	}
	report.LogTruncated = report.LogTruncated || log.truncated
	rawLog := []byte(log.String())
	if report.LogTruncated {
		rawLog = append(rawLog, []byte("\n[output truncated at 1 MiB; structured result: gate; retained log: gate/log.txt]\n")...)
	}
	return report, rawLog, nil
}

// gateOutputDigests hashes each declared file output as Finalize would capture
// it. An output that cannot be read now has no entry, so a capture of it
// differs.
func gateOutputDigests(req AttemptFinalization) map[string]string {
	digests := make(map[string]string)
	for _, declaration := range req.Task.Outputs {
		if declaration.Commit != nil {
			continue
		}
		resolved, err := safeBundleFile(req.WorkspaceDir, declaration.Name)
		if err != nil {
			continue
		}
		file, err := os.Open(resolved)
		if err != nil {
			continue
		}
		h := sha256.New()
		info, err := file.Stat()
		if err == nil && info.Mode().IsRegular() {
			_, err = io.Copy(h, file)
		} else if err == nil {
			err = errors.New("not a regular file")
		}
		if file.Close() == nil && err == nil {
			digests[declaration.Name] = fmt.Sprintf("%x", h.Sum(nil))
		}
	}
	return digests
}

// gatedOutputMatches reports why a captured declared output differs from what
// the gate attested, or "" when it matches. digests are the gate's
// outputDigests. A declared path tracked as a regular file in the gated commit
// must also still be that file, not a symlink to another, with the commit's
// content.
func gatedOutputMatches(ctx context.Context, workspace, commit string, digests map[string]string, declared, relative, captured, capturedDigest string) (string, error) {
	if want, ok := digests[declared]; !ok || want != capturedDigest {
		return "changed after the gate: its content differs from what the gate saw", nil
	}
	entry, err := gateGit(ctx, workspace, "ls-tree", "-z", commit, "--", filepath.ToSlash(filepath.Clean(declared)))
	if err != nil {
		return "", err
	}
	meta, _, found := strings.Cut(strings.TrimSuffix(entry, "\x00"), "\t")
	fields := strings.Fields(meta)
	if !found || len(fields) != 3 || fields[1] != "blob" || (fields[0] != "100644" && fields[0] != "100755") {
		return "", nil
	}
	if filepath.Clean(relative) != filepath.Clean(declared) {
		return "changed after the gate: it no longer is the tracked file the gate attested", nil
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

// gateCaptureCommand names the post-capture check in a gate report failed
// because a declared output changed after the gate.
const gateCaptureCommand = "git hash-object"

// gateFailureReasonMax is the longest failure reason the coordinator accepts.
const gateFailureReasonMax = 16384

// amendGateForChangedOutputs turns a passing gate report into a failed one
// for declared outputs that changed after the gate, so that the coordinator,
// which decides from the uploaded evidence, fails the attempt as well. The
// result stays within the coordinator's evidence limit; if the report cannot,
// it becomes a preparation-style failure for this attempt, as runGate does.
func amendGateForChangedOutputs(report GateReport, changed []string) ([]byte, error) {
	reason := strings.Join(changed, "; ")
	if len(reason) > gateFailureReasonMax {
		const suffix = " [truncated]"
		reason = strings.ToValidUTF8(reason[:gateFailureReasonMax-len(suffix)], "") + suffix
	}
	report.Passed = false
	report.Failure = &GateFailure{Command: gateCaptureCommand, ExitCode: 1, Reason: reason}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(raw)+1 > GateEvidenceMaxBytes {
		report.Commands = nil
		report.ToolVersions = map[string]string{}
		if raw, err = json.MarshalIndent(report, "", "  "); err != nil {
			return nil, err
		}
	}
	return append(raw, '\n'), nil
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
	// Reject this unsupported layout rather than attest a different tree.
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
	return tools, nil
}
