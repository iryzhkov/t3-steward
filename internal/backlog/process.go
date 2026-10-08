package backlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/procgroup"
)

type ProcessRequest struct {
	ID      string
	Dir     string
	Program string
	Args    []string
	Log     io.Writer
	// Timeout bounds worker-owned commands; contained runners enforce it internally too.
	Timeout time.Duration
	// KillRemaining kills whatever is still running under this request's ID
	// before the command starts and after it exits. A retried request reuses
	// its ID, and a process the command left behind would otherwise keep the
	// unit alive, refuse the retry and go on changing the workspace. When the
	// scope cannot be shown to be empty, Run fails rather than reporting the
	// command's own result.
	KillRemaining bool
	// MaxOutputBytes bounds the combined standard output and standard error
	// while they are being accumulated, rather than after the process has
	// finished. A command that writes far more than the caller will ever keep
	// is otherwise buffered in full first and truncated afterwards, which turns
	// a chatty remote into a memory hazard. Zero means unbounded, which is what
	// every caller that does not care still gets.
	MaxOutputBytes int
	// Limits is the task's reservation. A sized request's scope is told the
	// parallelism to use and, where the user manager can enforce them, given
	// cpu and memory limits; see ProcessLimits.
	Limits ProcessLimits
}

type ProcessResult struct {
	ExitCode int
	Output   string
	// Truncated reports that the process wrote more than MaxOutputBytes and the
	// remainder was discarded as it arrived. Output is never silently dropped.
	Truncated bool
}

// boundedBuffer accumulates at most limit bytes and discards the rest as it
// arrives. It always reports a successful write, because refusing one would
// make the child process fail on a broken pipe when the only thing that went
// wrong is that the caller has seen enough.
type boundedBuffer struct {
	builder   strings.Builder
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return b.builder.Write(p)
	}
	// The reported count is always the whole slice. Reporting the retained
	// count instead would be a short write, and the child process would die of
	// a pipe error for no reason other than that the caller has seen enough.
	offered := len(p)
	room := b.limit - b.builder.Len()
	if room <= 0 {
		b.truncated = true
		return offered, nil
	}
	if len(p) > room {
		b.truncated = true
		p = p[:room]
	}
	if _, err := b.builder.Write(p); err != nil {
		return 0, err
	}
	return offered, nil
}

// String returns the accumulated bytes without splitting a trailing rune.
func (b *boundedBuffer) String() string {
	value := b.builder.String()
	if !b.truncated {
		return value
	}
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

type ProcessExitError struct {
	ExitCode int
	Err      error
}

func (e *ProcessExitError) Error() string {
	return fmt.Sprintf("process exited with code %d: %v", e.ExitCode, e.Err)
}

func (e *ProcessExitError) Unwrap() error { return e.Err }

// ProcessRunner executes preparation or task child processes inside a containment boundary.
type ProcessRunner interface {
	Run(context.Context, ProcessRequest) (ProcessResult, error)
}

// SystemdScopeRunner executes a process in a transient user scope. Cancellation
// kills every process in the scope before returning.
type SystemdScopeRunner struct {
	SystemdRunBinary string
	SystemctlBinary  string
	// ScopeCleanupTimeout bounds how long a KillRemaining request waits for
	// its scope to empty. A cancelled run spends at most this long killing
	// its scope and as long again reaping its launcher. Zero means
	// scopeCleanupTimeout.
	ScopeCleanupTimeout time.Duration
	// LookupEnv reads the worker's own environment, which a sized request's
	// GOFLAGS extends; nil means os.LookupEnv.
	LookupEnv func(string) (string, bool)
	// Controllers reports whether the user manager can enforce cpu and memory
	// limits; nil reads the delegated cgroup controllers once per process.
	Controllers func() (cpu, memory bool)
}

func (r SystemdScopeRunner) Run(ctx context.Context, request ProcessRequest) (ProcessResult, error) {
	if err := validateProcessRequest(request); err != nil {
		return ProcessResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ProcessResult{}, err
	}
	log := request.Log
	if log == nil {
		log = io.Discard
	}
	unit := processScopeUnit(request.ID)
	args := []string{
		"--user",
		"--scope",
		"--collect",
		"--quiet",
		"--unit=" + unit,
		"--property=KillMode=control-group",
		"--working-directory=" + request.Dir,
	}
	args = append(args, r.scopeLimitArguments(request.Limits)...)
	args = append(args, "--", request.Program)
	args = append(args, request.Args...)
	fmt.Fprintf(log, "$ %s %s\n", r.systemdRun(), strings.Join(args, " "))

	if request.KillRemaining {
		if err := r.clearScope(unit); err != nil {
			fmt.Fprintf(log, "! %v\n", err)
			return ProcessResult{}, err
		}
	}
	// The run owns the read end of the launcher's output pipe and the one
	// goroutine copying from it, rather than leaving them to exec.Cmd, so that
	// a cancelled run can close the pipe and join the copy before it returns
	// even when a process outside the launcher still holds the write end.
	outputReader, outputWriter, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(log, "! %v\n", err)
		return ProcessResult{}, err
	}
	defer outputReader.Close()
	output := boundedBuffer{limit: request.MaxOutputBytes}
	command := exec.Command(r.systemdRun(), args...)
	command.Stdout = outputWriter
	command.Stderr = outputWriter
	err = command.Start()
	_ = outputWriter.Close()
	if err != nil {
		fmt.Fprintf(log, "! %v\n", err)
		return ProcessResult{}, err
	}
	// waited delivers the launcher's exit only once its output has been read
	// to the end, as exec.Cmd.Wait does with a writer it copies to.
	waited := make(chan error, 1)
	go func() {
		_, _ = io.Copy(&output, outputReader)
		waited <- command.Wait()
	}()

	select {
	case err := <-waited:
		var cleanupErr error
		if request.KillRemaining {
			cleanupErr = r.clearScope(unit)
		}
		_, _ = io.WriteString(log, output.String())
		result := ProcessResult{Output: output.String(), Truncated: output.truncated}
		if cleanupErr != nil {
			// A process the command left behind may still be running and
			// changing the workspace, so whatever the command reported cannot
			// stand as a success.
			fmt.Fprintf(log, "! %v\n", cleanupErr)
			if err != nil {
				cleanupErr = errors.Join(err, cleanupErr)
			}
			var exitError *exec.ExitError
			if errors.As(err, &exitError) {
				result.ExitCode = exitError.ExitCode()
			}
			return result, cleanupErr
		}
		if err == nil {
			return result, nil
		}
		fmt.Fprintf(log, "! %v\n", err)
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			result.ExitCode = exitError.ExitCode()
			return result, &ProcessExitError{ExitCode: result.ExitCode, Err: err}
		}
		return result, err
	case <-ctx.Done():
		// The task's context is already done, so cleanup gets bounds of its
		// own: one for the scope kill and one for reaping the launcher. The
		// scope is killed while the launcher still holds it, because systemd
		// collects a scope whose last process has exited and then refuses to
		// kill it as not loaded. The launcher is killed whatever the user
		// manager answered.
		killCtx, cancelKill := context.WithTimeout(context.Background(), r.cleanupTimeout())
		killErr := r.killScope(killCtx, log, unit)
		cancelKill()
		_ = command.Process.Kill()
		reap := time.NewTimer(r.cleanupTimeout())
		defer reap.Stop()
		select {
		case <-waited:
		case <-reap.C:
			// A process outside the launcher still holds its output open
			// after the scope kill, so the scope is not shown to be empty.
			// Closing the read end ends the copy, and the killed launcher is
			// reaped, before the output is read and the run returns.
			if killErr == nil {
				killErr = errors.New("launcher output still held open after the cleanup timeout")
			}
			_ = outputReader.Close()
			<-waited
			fmt.Fprintf(log, "! %v\n", killErr)
		}
		_, _ = io.WriteString(log, output.String())
		if killErr != nil {
			return ProcessResult{Output: output.String(), Truncated: output.truncated}, errors.Join(ctx.Err(), fmt.Errorf("kill process scope %s: %w", unit, killErr))
		}
		return ProcessResult{Output: output.String(), Truncated: output.truncated}, ctx.Err()
	}
}

func (r SystemdScopeRunner) killScope(ctx context.Context, log io.Writer, unit string) error {
	args := []string{"--user", "kill", "--kill-who=all", "--signal=KILL", unit}
	fmt.Fprintf(log, "$ %s %s\n", r.systemctl(), strings.Join(args, " "))
	command := r.systemctlCommand(ctx, args...)
	command.Stdout = log
	command.Stderr = log
	if err := procgroup.ProgramResult(ctx, command.Run()); err != nil {
		fmt.Fprintf(log, "! %v\n", err)
		return err
	}
	return nil
}

// scopeCleanupTimeout is the default bound on the whole of clearScope, so a
// user manager that stops answering fails the request instead of hanging the
// worker.
const scopeCleanupTimeout = 30 * time.Second

// scopeCleanupPoll is how often clearScope asks whether the unit has stopped.
const scopeCleanupPoll = 20 * time.Millisecond

// clearScope kills any process still in unit and returns only once systemd
// reports the unit inactive, which is when no process is left in it. Usually
// the unit is already gone, and systemctl refusing to kill a unit that is not
// loaded is expected; the state query that follows proves the absence. Any
// other outcome, a state query that fails or a unit still active at the
// deadline, is an error: the caller cannot rule out a process that keeps
// changing the workspace, and must not report success.
func (r SystemdScopeRunner) clearScope(unit string) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cleanupTimeout())
	defer cancel()
	killOutput, killErr := r.systemctlCommand(ctx, "--user", "kill", "--kill-who=all", "--signal=KILL", unit).CombinedOutput()
	killErr = procgroup.ProgramResult(ctx, killErr)
	// observed is the last state the user manager answered with. The deadline
	// can expire while a query is running, and the query is then killed; that
	// is still the deadline, not a user manager that stopped answering.
	observed := ""
	for {
		out, err := r.systemctlCommand(ctx, "--user", "show", "--property=ActiveState", "--value", unit).CombinedOutput()
		err = procgroup.ProgramResult(ctx, err)
		state := strings.TrimSpace(string(out))
		if err == nil && (state == "inactive" || state == "failed") {
			return nil
		}
		if err == nil {
			observed = state
		} else if ctx.Err() != nil && observed != "" {
			return scopeCleanupError(unit, observed, killErr, killOutput)
		}
		if err != nil {
			// Without an answer from the user manager nothing more can be
			// learned by waiting.
			return scopeCleanupError(unit, fmt.Sprintf("unknown (%v: %s)", err, state), killErr, killOutput)
		}
		select {
		case <-ctx.Done():
			return scopeCleanupError(unit, state, killErr, killOutput)
		case <-time.After(scopeCleanupPoll):
		}
	}
}

// ScopeCleanupError reports that a KillRemaining request could not show its
// scope to be empty. Callers check for it before any deadline of their own,
// because the wait for the scope can outlast the command's deadline.
type ScopeCleanupError struct {
	Unit   string
	Detail string
}

func (e *ScopeCleanupError) Error() string {
	return fmt.Sprintf("process scope %s could not be cleared: %s", e.Unit, e.Detail)
}

func scopeCleanupError(unit, state string, killErr error, killOutput []byte) error {
	detail := "state " + state
	if killErr != nil {
		detail += fmt.Sprintf("; kill: %v: %s", killErr, strings.TrimSpace(string(killOutput)))
	}
	return &ScopeCleanupError{Unit: unit, Detail: detail}
}

// cleanupTimeout bounds scope cleanup, both clearScope and the kill after a
// cancellation.
func (r SystemdScopeRunner) cleanupTimeout() time.Duration {
	if r.ScopeCleanupTimeout > 0 {
		return r.ScopeCleanupTimeout
	}
	return scopeCleanupTimeout
}

func (r SystemdScopeRunner) systemdRun() string {
	if r.SystemdRunBinary != "" {
		return r.SystemdRunBinary
	}
	return "systemd-run"
}

// systemctlCommand is every systemctl call the runner makes. Each one is
// bounded by its cleanup context: when the context ends the call is killed
// with any child it started, and its output pipes are closed after
// scopeCleanupPoll even if something else still holds them, so a user
// manager that stops answering cannot outlast the cleanup timeout. Callers
// pass the call's error through procgroup.ProgramResult, so an answer whose
// child lingers is still an answer.
func (r SystemdScopeRunner) systemctlCommand(ctx context.Context, args ...string) *exec.Cmd {
	return procgroup.CommandContext(ctx, scopeCleanupPoll, r.systemctl(), args...)
}

func (r SystemdScopeRunner) systemctl() string {
	if r.SystemctlBinary != "" {
		return r.SystemctlBinary
	}
	return "systemctl"
}

func validateProcessRequest(request ProcessRequest) error {
	if strings.TrimSpace(request.ID) != request.ID || request.ID == "" {
		return errors.New("process ID must be nonempty and trimmed")
	}
	if request.Dir == "" || !filepath.IsAbs(request.Dir) {
		return errors.New("process working directory must be absolute")
	}
	if strings.TrimSpace(request.Program) != request.Program || request.Program == "" {
		return errors.New("process program must be nonempty and trimmed")
	}
	if request.MaxOutputBytes < 0 {
		return errors.New("process output bound cannot be negative")
	}
	return nil
}

func processScopeUnit(id string) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("t3-steward-%x.scope", sum[:12])
}
