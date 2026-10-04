package backlog

import (
	"context"
	"slices"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Optional presentation never makes ordinary work depend on inventory discovery.
// Mandatory capability checks remain in declarePackageCapabilities.
func (b CoordinatorOfferBuilder) addSessionDisplay(ctx context.Context, pkg *workerproto.ExecutionPackage, workflow, task string, judge bool) {
	advertised, known, err := b.advertisedCapabilities(ctx, pkg.WorkerID)
	if err != nil || !known || !slices.Contains(advertised, workerproto.PackageCapabilitySessionDisplay) {
		return
	}
	pkg.Display = &workerproto.SessionDisplay{WorkflowName: workerproto.SanitizeDisplayName(workflow), TaskName: workerproto.SanitizeDisplayName(task), ReviewJudge: judge}
	pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilitySessionDisplay)
}
