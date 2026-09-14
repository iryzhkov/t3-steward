package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// containedPreflighter runs one preflight step inside the attempt's containment
// unit. It mirrors containedVerifier so that a contained worker preflights the
// same way it verifies, with one deliberate difference: it does not require the
// provider supervisor to have stopped, because preflight runs before that
// supervisor has anything to stop.
type containedPreflighter struct {
	manager ContainedT3
	pkg     workerproto.ExecutionPackage
}

func (p containedPreflighter) Run(ctx context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	plan, err := p.manager.preparation(p.pkg)
	if err != nil {
		return backlog.ProcessResult{}, err
	}
	if request.ID == "" || request.Program == "" || request.Dir != plan.Launch.Spec.Workspace.Registration.Path {
		return backlog.ProcessResult{}, errors.New("contained preflight request does not match prepared workspace")
	}
	timeout := p.pkg.Limits.VerificationTimeout
	if timeout <= 0 {
		return backlog.ProcessResult{}, errors.New("contained preflight needs a bounded deadline")
	}
	spec := plan.Launch.Spec
	spec.Control = nil
	spec.ControlPort = 0
	spec.ProviderHosts = nil
	spec.Command = append([]string{"/usr/bin/timeout", "--kill-after=5s", fmt.Sprintf("%.3fs", timeout.Seconds()), request.Program}, request.Args...)
	launch := providercontainment.Launch{ExecutionID: p.pkg.Identity.ThreadID + ":preflight-" + request.ID, Spec: spec}
	path, err := p.manager.recordPath(p.pkg)
	if err != nil {
		return backlog.ProcessResult{}, err
	}
	key := sha256.Sum256([]byte(request.ID))
	if err = privateJSON(path+".preflight."+hex.EncodeToString(key[:]), containedPreparation{Identity: p.pkg.Identity, WorkerID: p.pkg.WorkerID, Launch: launch}); err != nil {
		return backlog.ProcessResult{}, err
	}
	if _, err = p.manager.Supervisor.Start(ctx, launch); err != nil {
		return backlog.ProcessResult{}, err
	}
	for {
		result, err := p.manager.Supervisor.Result(ctx, launch)
		if err == nil {
			response := backlog.ProcessResult{
				ExitCode: result.ExitCode,
				Output:   fmt.Sprintf("contained preflight %s exited %d; process group stopped", result.InvocationID, result.ExitCode),
			}
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
		// The durable command outlives a lost worker request; its own timeout
		// bounds it and a later pass observes the same unit.
		select {
		case <-ctx.Done():
			return backlog.ProcessResult{}, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
