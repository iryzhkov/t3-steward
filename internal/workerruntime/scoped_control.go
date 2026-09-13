package workerruntime

import (
	"context"
	"errors"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ExecutionT3Provider attaches to an already prepared, identity-fenced
// execution supervisor. Attach must never launch or replace an execution.
type ExecutionT3Provider interface {
	Attach(context.Context, workerproto.ExecutionPackage) (T3Control, error)
}

// scopedDriver isolates all thread operations, including recovery and failure
// collection. A failed attachment never falls back to the shared host T3.
// This does not enable preparation: the directory backend guard stays closed
// until provider setup, verification containment and custody are integrated.
func (d *LocalDriver) scopedDriver(ctx context.Context, pkg workerproto.ExecutionPackage) (*LocalDriver, error) {
	if d.Config.DryRun || d.scoped || len(pkg.Environment.DirectoryBindings) == 0 {
		return nil, nil
	}
	if d.ScopedT3 == nil {
		return nil, errors.New("directory execution has no scoped T3 attachment")
	}
	control, err := d.ScopedT3.Attach(ctx, pkg)
	if err != nil {
		return nil, err
	}
	if control == nil {
		return nil, errors.New("directory execution scoped T3 attachment is nil")
	}
	copy := *d
	copy.T3 = control
	copy.scoped = true
	return &copy, nil
}
