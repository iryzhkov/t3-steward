package workerruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// containedLimits are the limits a contained run of the package enforces: the
// demand the coordinator accounted, and nothing when it sent none. The worker
// never derives a limit of its own.
func containedLimits(pkg workerproto.ExecutionPackage) *providercontainment.Limits {
	if pkg.ResourceDemand == nil {
		return nil
	}
	return providercontainment.LimitsFor(*pkg.ResourceDemand)
}

// endedRun reports the observation of an attached run whose unit has already
// ended, which only a forced quiesce acts on: a run the memory limit killed
// has no T3 server left to capture an outcome from, and refusing to stop it
// would leave the attempt unable to end. Any uncertainty, including a unit
// that is not loaded, answers nil and keeps the existing path.
func (p ContainedT3) endedRun(ctx context.Context, record ContainedAttachment, loadErr error, force bool) *providercontainment.SupervisorObservation {
	if loadErr != nil || !force {
		return nil
	}
	observe := p.Supervisor.Observe
	if p.observe != nil {
		observe = p.observe
	}
	obs, err := observe(ctx, record.Launch)
	if err != nil {
		return nil
	}
	if obs.Stopped || strings.HasPrefix(obs.State, "failed/") || strings.HasPrefix(obs.State, "inactive/") {
		return &obs
	}
	return nil
}

// containedFailure is the error for a supervisor that is no longer running the
// provider. Without a known cause it is uncertain custody. A known cause, such
// as the memory limit killing the run, is a definite outcome systemd already
// ended: it is named, and counts as a failed preparation instead of an
// uncertain one that would be retried for ever.
func containedFailure(obs providercontainment.SupervisorObservation) error {
	if obs.Failure != "" {
		return fmt.Errorf("%s (supervisor is %s)", obs.Failure, obs.State)
	}
	return fmt.Errorf("%w: supervisor is %s", ErrContainedCustody, obs.State)
}
