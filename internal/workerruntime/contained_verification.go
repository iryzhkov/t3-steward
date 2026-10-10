package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func (d *LocalDriver) containedManager(pkg workerproto.ExecutionPackage) *ContainedT3 {
	if d.Config.DryRun || len(pkg.Environment.DirectoryBindings) == 0 {
		return nil
	}
	switch manager := d.ScopedT3.(type) {
	case ContainedT3:
		return &manager
	case *ContainedT3:
		return manager
	default:
		return nil
	}
}

func (d *LocalDriver) StopPreparation(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	if manager := d.containedManager(pkg); manager != nil {
		return manager.Quiesce(ctx, pkg, true)
	}
	return nil
}

// stopContext bounds each durable owner's forced stop independently of the
// caller's transport deadline. It grants no release authority on timeout.
func (p ContainedT3) stopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (p ContainedT3) stopVerifications(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	path, err := p.recordPath(pkg)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		return err
	}
	var failures []error
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), filepath.Base(path)+".verify.") {
			continue
		}
		raw, err := readBoundedRegularFile(filepath.Join(filepath.Dir(path), entry.Name()), 1<<20)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		var plan containedPreparation
		if err = json.Unmarshal(raw, &plan); err != nil {
			failures = append(failures, err)
			continue
		}
		if plan.Identity != pkg.Identity || plan.WorkerID != pkg.WorkerID || !strings.HasPrefix(plan.Launch.ExecutionID, pkg.Identity.ThreadID+":verify-") {
			failures = append(failures, errors.New("verification custody identity mismatch"))
			continue
		}
		// Each validated owner gets its own bounded stop opportunity; an earlier
		// timeout must not starve an independently stoppable process.
		stopCtx, cancel := p.stopContext(ctx)
		stopped, err := p.Supervisor.Stop(stopCtx, plan.Launch)
		cancel()
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if !stopped.Stopped {
			failures = append(failures, errors.New("verification process custody unproven"))
		}
	}
	return errors.Join(failures...)
}

// Keep existing millisecond invocation bytes stable for durable replay, while
// preserving sub-millisecond declarations instead of rounding them to zero.
func containedVerificationTimeout(timeout time.Duration) string {
	if timeout%time.Millisecond != 0 {
		return fmt.Sprintf("%.9fs", timeout.Seconds())
	}
	return fmt.Sprintf("%.3fs", timeout.Seconds())
}

type containedVerifier struct {
	manager ContainedT3
	pkg     workerproto.ExecutionPackage
}

func (v containedVerifier) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	plan, err := v.manager.preparation(v.pkg)
	if err != nil {
		return backlog.ProcessResult{}, err
	}
	stopped, err := v.manager.Supervisor.Observe(ctx, plan.Launch)
	if err != nil {
		return backlog.ProcessResult{}, err
	}
	if !stopped.Stopped {
		return backlog.ProcessResult{}, errors.New("provider supervisor must stop before verification")
	}
	// Accept only the historical shell form and the finalizer's exact umask
	// wrapper. The command stays a separate argument in both forms.
	plainShell := len(request.Args) == 2 && request.Args[0] == "-c"
	umaskShell := len(request.Args) == 5 && request.Args[0] == "-c" &&
		request.Args[1] == `umask 022 && exec "$0" "$@"` &&
		request.Args[2] == "/bin/sh" && request.Args[3] == "-c"
	if request.ID == "" || request.Dir != plan.Launch.Spec.Workspace.Registration.Path || request.Program != "/bin/sh" || (!plainShell && !umaskShell) {
		return backlog.ProcessResult{}, errors.New("contained verification request does not match prepared workspace")
	}
	timeout := v.pkg.Limits.VerificationTimeout
	if request.Timeout < 0 || request.Timeout > timeout {
		return backlog.ProcessResult{}, errors.New("contained verification deadline exceeds worker limit or is negative")
	}
	if request.Timeout > 0 {
		timeout = request.Timeout
	}
	if timeout <= 0 {
		return backlog.ProcessResult{}, errors.New("contained verification needs a bounded deadline")
	}
	spec := plan.Launch.Spec
	spec.Control = nil
	spec.ControlPort = 0
	spec.ProviderHosts = nil
	spec.Command = append([]string{"/usr/bin/timeout", "--kill-after=5s", containedVerificationTimeout(timeout), request.Program}, request.Args...)
	launch := providercontainment.Launch{ExecutionID: v.pkg.Identity.ThreadID + ":" + request.ID, Spec: spec}
	path, err := v.manager.recordPath(v.pkg)
	if err != nil {
		return backlog.ProcessResult{}, err
	}
	key := sha256.Sum256([]byte(request.ID))
	if err = privateJSON(path+".verify."+hex.EncodeToString(key[:]), containedPreparation{Identity: v.pkg.Identity, WorkerID: v.pkg.WorkerID, Launch: launch}); err != nil {
		return backlog.ProcessResult{}, err
	}
	if _, err = v.manager.Supervisor.Start(ctx, launch); err != nil {
		return backlog.ProcessResult{}, err
	}
	for {
		result, err := v.manager.Supervisor.Result(ctx, launch)
		if err == nil {
			response := backlog.ProcessResult{ExitCode: result.ExitCode, Output: fmt.Sprintf("contained invocation %s exited %d; process group stopped", result.InvocationID, result.ExitCode)}
			if request.Log != nil {
				if _, err = fmt.Fprintln(request.Log, response.Output); err != nil {
					return response, err
				}
			}
			if result.ExitCode != 0 {
				return response, &backlog.ProcessExitError{ExitCode: result.ExitCode, Err: errors.New(response.Output)}
			}
			return response, nil
		}
		if !errors.Is(err, providercontainment.ErrCommandRunning) {
			return backlog.ProcessResult{}, err
		}
		// Losing the worker request does not start or stop the durable command.
		// Its own timeout bounds the command; a later pass observes the same unit.
		select {
		case <-ctx.Done():
			return backlog.ProcessResult{}, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
