// Package sessionarchive applies reversible T3 UI archiving after settlement.
// It does not export, delete or remove session data.
package sessionarchive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type State struct {
	Background bool
	Busy       string
}
type Store interface {
	GetKV(context.Context, string) (string, bool, error)
	SetKV(context.Context, string, string) error
	RecordAction(context.Context, domain.ActionRecord) error
}
type Control interface {
	GetThread(context.Context, string) (*domain.Thread, error)
	ArchiveSettledThread(context.Context, domain.Thread) error
}
type Options struct {
	BackgroundAfter time.Duration
	UserAfter       time.Duration
	MaxPerPass      int
	DryRun          bool
}
type Archiver struct {
	Options Options
	Store   Store
	Control Control
	// States combines durable backlog provenance, waits and local worker custody.
	// A failed read fences the entire pass.
	States func(context.Context) (map[string]State, error)
	Now    func() time.Time
	Logger *slog.Logger
}

func Eligible(t domain.Thread, state State, now time.Time, opts Options) bool {
	if t.ArchivedAt != nil || t.SettledAt == nil || t.SettledAt.IsZero() || !t.Settled() ||
		t.Running || t.TurnState == "running" || t.SessionStatus == "running" || t.SessionStatus == "starting" ||
		t.BackgroundWork != "" || t.HasPendingApprovals || t.HasPendingUserInput || state.Busy != "" {
		return false
	}
	// Activity since settlement invalidates the old settlement timestamp.
	if t.LatestUserMessageAt != nil && t.LatestUserMessageAt.After(*t.SettledAt) {
		return false
	}
	after := opts.UserAfter
	if state.Background {
		after = opts.BackgroundAfter
	}
	if after <= 0 {
		return false
	}
	return !now.Before(t.SettledAt.Add(after))
}

func (a *Archiver) Tick(ctx context.Context, threads []domain.Thread, _ []domain.BucketState) {
	if _, err := a.Run(ctx, threads); err != nil {
		logger := a.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("UI archive pass incomplete", "error", err)
	}
}

func (a *Archiver) Run(ctx context.Context, threads []domain.Thread) (int, error) {
	now := time.Now()
	if a.Now != nil {
		now = a.Now()
	}
	states, err := a.States(ctx)
	if err != nil {
		return 0, err
	}
	candidates := append([]domain.Thread(nil), threads...)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].SettledAt == nil {
			return false
		}
		if candidates[j].SettledAt == nil {
			return true
		}
		return candidates[i].SettledAt.Before(*candidates[j].SettledAt)
	})
	limit := a.Options.MaxPerPass
	if limit <= 0 {
		limit = 10
	}
	count, attempted := 0, 0
	var failures error
	for _, candidate := range candidates {
		if !Eligible(candidate, states[candidate.ID], now, a.Options) {
			continue
		}
		signature := candidate.SettledAt.UTC().Format(time.RFC3339Nano)
		key := "ui_archive.settlement." + candidate.ID
		previous, _, err := a.Store.GetKV(ctx, key)
		if err != nil {
			return count, err
		}
		// A manually unarchived session is not hidden again for this settlement.
		if previous == signature {
			continue
		}
		current, err := a.Control.GetThread(ctx, candidate.ID)
		if err != nil {
			return count, err
		}
		states, err = a.States(ctx)
		if err != nil {
			return count, err
		}
		if current == nil || !Eligible(*current, states[candidate.ID], now, a.Options) ||
			!current.SettledAt.Equal(*candidate.SettledAt) {
			continue
		}
		detail := fmt.Sprintf("settled=%s background=%t", signature, states[candidate.ID].Background)
		attempted++
		if !a.Options.DryRun {
			if err = a.Control.ArchiveSettledThread(ctx, *current); err != nil {
				failures = errors.Join(failures, fmt.Errorf("thread %s: %w", candidate.ID, err))
				if auditErr := a.Store.RecordAction(ctx, domain.ActionRecord{At: now, Kind: domain.ActionKind("ui-archive"), ThreadID: candidate.ID, Detail: detail, Err: err.Error()}); auditErr != nil {
					return count, errors.Join(failures, auditErr)
				}
				if attempted >= limit {
					break
				}
				continue
			}
			if err = a.Store.SetKV(ctx, key, signature); err != nil {
				return count, err
			}
		}
		if err = a.Store.RecordAction(ctx, domain.ActionRecord{At: now, Kind: domain.ActionKind("ui-archive"), ThreadID: candidate.ID, DryRun: a.Options.DryRun, Detail: detail}); err != nil {
			return count, err
		}
		count++
		if attempted >= limit {
			break
		}
	}
	return count, failures
}
