package t3

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ArchiveSettledThread is a reversible UI action, never thread deletion.
// Recheck the settlement just before dispatch because archiving stops sessions.
func (c *Control) ArchiveSettledThread(ctx context.Context, expected domain.Thread) error {
	current, err := c.GetThread(ctx, expected.ID)
	if err != nil {
		return err
	}
	if current == nil {
		return errors.New("archive thread disappeared")
	}
	if current.ArchivedAt != nil {
		return nil
	}
	if current.SettledAt == nil || expected.SettledAt == nil || !current.SettledAt.Equal(*expected.SettledAt) ||
		!current.Settled() || IsRunning(*current) || current.SessionStatus == "running" || current.SessionStatus == "starting" ||
		current.BackgroundWork != "" || current.HasPendingApprovals || current.HasPendingUserInput ||
		(current.LatestUserMessageAt != nil && current.LatestUserMessageAt.After(*current.SettledAt)) {
		return errors.New("archive eligibility changed")
	}
	if c.DryRun {
		return errors.New("UI archive control is dry-run")
	}
	token := expected.ID + ":" + expected.SettledAt.UTC().Format(time.RFC3339Nano)
	_, dispatchErr := c.client.Dispatch(ctx, map[string]any{"type": "thread.archive", "commandId": deterministicID(token, "thread.archive"), "threadId": expected.ID})
	observed, observeErr := c.GetThread(ctx, expected.ID)
	if observeErr == nil && observed != nil && observed.ArchivedAt != nil {
		return nil
	}
	return fmt.Errorf("archive projection unproven: %w", errors.Join(dispatchErr, observeErr, errors.New("not observed archived")))
}
