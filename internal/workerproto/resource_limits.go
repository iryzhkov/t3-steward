package workerproto

import (
	"errors"
	"fmt"
	"slices"
)

// PackageCapabilityContainedLimits says the package carries the resource
// demand the coordinator accounted for the attempt, in ResourceDemand, and that
// the worker enforces it as the CPU and memory limits of a contained run. The
// coordinator offers it only to a worker that advertises it, so an older worker
// keeps receiving exactly the package it received before.
const PackageCapabilityContainedLimits = "contained-resource-limits-v1"

// validateResourceDemand ties the demand to its capability in both directions
// and refuses a demand that sizes nothing, which would declare limits it
// cannot enforce.
func validateResourceDemand(pkg ExecutionPackage) error {
	declared := slices.Contains(pkg.RequiredCapabilities, PackageCapabilityContainedLimits)
	if pkg.ResourceDemand == nil {
		if declared {
			return errors.New("execution package: contained resource limits capability requires a resource demand")
		}
		return nil
	}
	if !declared {
		return errors.New("execution package: resource demand requires the contained resource limits capability")
	}
	if pkg.Supervision != nil {
		return errors.New("execution package: an activation carries no resource demand")
	}
	if err := pkg.ResourceDemand.Validate(); err != nil {
		return fmt.Errorf("execution package: %w", err)
	}
	if !(pkg.ResourceDemand.CPUUnits > 0) && pkg.ResourceDemand.MemoryMB <= 0 {
		return errors.New("execution package: resource demand must size CPU or memory")
	}
	return nil
}
