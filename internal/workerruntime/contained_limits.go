package workerruntime

import (
	"fmt"

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

// containedFailure is the custody error for a supervisor that is no longer
// running the provider. A known cause, such as the memory limit killing the
// run, is named instead of leaving only the unit state.
func containedFailure(obs providercontainment.SupervisorObservation) error {
	if obs.Failure != "" {
		return fmt.Errorf("%w: %s (supervisor is %s)", ErrContainedCustody, obs.Failure, obs.State)
	}
	return fmt.Errorf("%w: supervisor is %s", ErrContainedCustody, obs.State)
}
