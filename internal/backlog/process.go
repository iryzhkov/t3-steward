package backlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
)

type ProcessRequest struct {
	ID      string
	Dir     string
	Program string
	Args    []string
	Log     io.Writer
}

type ProcessResult struct {
	ExitCode int
	Output   string
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
		"--",
		request.Program,
	}
	args = append(args, request.Args...)
	fmt.Fprintf(log, "$ %s %s\n", r.systemdRun(), strings.Join(args, " "))

	var output strings.Builder
	command := exec.Command(r.systemdRun(), args...)
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		fmt.Fprintf(log, "! %v\n", err)
		return ProcessResult{}, err
	}
	waited := make(chan error, 1)
	go func() {
		waited <- command.Wait()
	}()

	select {
	case err := <-waited:
		_, _ = io.WriteString(log, output.String())
		result := ProcessResult{Output: output.String()}
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
		killErr := r.killScope(log, unit)
		_ = command.Process.Kill()
		<-waited
		_, _ = io.WriteString(log, output.String())
		if killErr != nil {
			return ProcessResult{Output: output.String()}, errors.Join(ctx.Err(), fmt.Errorf("kill process scope %s: %w", unit, killErr))
		}
		return ProcessResult{Output: output.String()}, ctx.Err()
	}
}

func (r SystemdScopeRunner) killScope(log io.Writer, unit string) error {
	args := []string{"--user", "kill", "--kill-who=all", "--signal=KILL", unit}
	fmt.Fprintf(log, "$ %s %s\n", r.systemctl(), strings.Join(args, " "))
	command := exec.Command(r.systemctl(), args...)
	command.Stdout = log
	command.Stderr = log
	if err := command.Run(); err != nil {
		fmt.Fprintf(log, "! %v\n", err)
		return err
	}
	return nil
}

func (r SystemdScopeRunner) systemdRun() string {
	if r.SystemdRunBinary != "" {
		return r.SystemdRunBinary
	}
	return "systemd-run"
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
	return nil
}

func processScopeUnit(id string) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("t3-steward-%x.scope", sum[:12])
}
