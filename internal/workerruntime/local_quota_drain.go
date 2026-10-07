package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// RequestQuotaDrain sends the checkpoint notice without waiting for the turn.
// A later reconciliation observes the stop and reads checkpoint evidence.
func (d *LocalDriver) RequestQuotaDrain(ctx context.Context, pkg workerproto.ExecutionPackage, command domain.ThrottleCommand) error {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.RequestQuotaDrain(ctx, pkg, command)
	}
	if d.Config.DryRun {
		return nil
	}
	thread, err := d.requiredThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return err
	}
	text := command.Reason + "\nThis is a runtime-owned quota pause, an exception to ordinary end-of-turn task completion. Write .t3/checkpoint.md and leave the workspace consistent. The continue marker is checkpoint evidence under runtime control, not a request for extra turns; the runtime checks completion before considering an authorized resume. If the task is fully complete and all declared outputs are ready, end your final message with the exact line 'backlog status: done'. Otherwise end it with 'backlog status: continue'. End this turn."
	return d.T3.WarnThread(ctx, thread, domain.Warning{Kind: domain.ActionDrain, Text: text})
}

// ReadQuotaCheckpoint publishes optional evidence once a quota pause stopped.
// Absence of the file is a valid pause without checkpoint metadata.
func (d *LocalDriver) ReadQuotaCheckpoint(ctx context.Context, pkg workerproto.ExecutionPackage) (*domain.CheckpointMetadata, error) {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return nil, err
	} else if scoped != nil {
		return scoped.ReadQuotaCheckpoint(ctx, pkg)
	}
	if d.Config.DryRun {
		return d.Publisher.PublishCheckpoint(ctx, pkg, ".t3/checkpoint.md", []byte("no-external-effects checkpoint\n"))
	}
	path := filepath.Join(d.workspacePath(pkg), "workspace", ".t3", "checkpoint.md")
	data, err := readBoundedRegularFile(path, pkg.Limits.MaxArtifactBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	return d.Publisher.PublishCheckpoint(ctx, pkg, ".t3/checkpoint.md", data)
}
