package backlog

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type assignmentDisplayStore interface {
	FreezeAssignmentDisplay(context.Context, int64, string, domain.Assignment, workerproto.ExecutionIdentity, *workerproto.SessionDisplay) (*workerproto.SessionDisplay, error)
}

// Optional inventory uncertainty proposes omission. The durable first decision,
// including null, wins before delivery; no process-local cache owns this choice.
func (b CoordinatorOfferBuilder) freezeSessionDisplay(ctx context.Context, assignment domain.Assignment, pkg *workerproto.ExecutionPackage, workflow, task string, judge bool) error {
	store, ok := b.Store.(assignmentDisplayStore)
	if !ok {
		return errors.New("execution package builder: durable display decision store is required")
	}
	advertised, known, inventoryErr := b.advertisedCapabilities(ctx, pkg.WorkerID)
	supported := inventoryErr == nil && known && slices.Contains(advertised, workerproto.PackageCapabilitySessionDisplay)
	var proposed *workerproto.SessionDisplay
	if supported {
		proposed = &workerproto.SessionDisplay{WorkflowName: workerproto.SanitizeDisplayName(workflow), TaskName: workerproto.SanitizeDisplayName(task), ReviewJudge: judge}
	}
	display, err := store.FreezeAssignmentDisplay(ctx, b.CoordinatorEpoch, b.CoordinatorID, assignment, pkg.Identity, proposed)
	if err != nil {
		return fmt.Errorf("execution package builder: freeze display decision: %w", err)
	}
	// A known incapable worker cannot execute a previously negotiated package.
	// An unavailable inventory is uncertainty, not evidence of capability loss;
	// the worker's strict capability validation remains mandatory.
	if display != nil && inventoryErr == nil && known && !supported {
		return fmt.Errorf("execution package builder: worker %q no longer supports frozen %q", pkg.WorkerID, workerproto.PackageCapabilitySessionDisplay)
	}
	pkg.Display = display
	pkg.RequiredCapabilities = slices.DeleteFunc(pkg.RequiredCapabilities, func(c string) bool { return c == workerproto.PackageCapabilitySessionDisplay })
	if display != nil {
		pkg.RequiredCapabilities = append(pkg.RequiredCapabilities, workerproto.PackageCapabilitySessionDisplay)
	}
	return nil
}
