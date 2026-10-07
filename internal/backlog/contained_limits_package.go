package backlog

import (
	"context"
	"slices"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// offerResourceDemand carries the demand this coordinator accounted for the
// assignment to a worker that enforces it as the limits of a contained run.
//
// The demand is the one committed with the assignment, never a later graph
// projection, so the limit a worker enforces is the reservation the scheduler
// counted. It is offered, never required: a worker that does not advertise the
// capability, an unknown worker, an unreadable inventory, an assignment with
// no demand evidence and a demand that sizes neither CPU nor memory all get
// exactly the package an earlier coordinator built.
func (b CoordinatorOfferBuilder) offerResourceDemand(ctx context.Context, pkg *workerproto.ExecutionPackage, attempt domain.Attempt, assignment domain.Assignment) {
	demand, known := domain.AssignmentExecutorDemand(attempt, assignment)
	if !known || pkg.Supervision != nil || !(demand.CPUUnits > 0) && demand.MemoryMB <= 0 || demand.Validate() != nil {
		return
	}
	advertised, workerKnown, err := b.advertisedCapabilities(ctx, pkg.WorkerID)
	if err != nil || !workerKnown || !slices.Contains(advertised, workerproto.PackageCapabilityContainedLimits) {
		return
	}
	pkg.ResourceDemand = &demand
	pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilityContainedLimits)
}
