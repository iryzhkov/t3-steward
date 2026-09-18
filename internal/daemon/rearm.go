package daemon

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The operator rearm (F-1): the third writer of a bucket's phase, next to the
// engine and the load-time re-derivation. It runs from the bucket rearm verb
// against the host's state database, sets the phase to normal with
// RecoveredAt now, and records who did it and why. The worker's resume rule
// reads RecoveredAt newer than the pause, so a paused owned attempt resumes
// after the settle delay without a reading; the next reading rearms or
// re-stops the bucket honestly.

// BucketStore is the part of the state store a rearm touches.
type BucketStore interface {
	ListBuckets(context.Context) ([]domain.BucketState, error)
	SaveBucket(context.Context, domain.BucketState) error
	RecordAction(context.Context, domain.ActionRecord) error
	ClearThreadNotices(context.Context, domain.BucketKey) error
}

// RearmRequest is one operator rearm.
type RearmRequest struct {
	Key domain.BucketKey
	// Actor is who asked, as user@host.
	Actor  string
	Reason string
	// Force rearms a bucket whose stored percentage is at or above the stop
	// threshold, which is otherwise refused: the next reading would stop it
	// again at once.
	Force bool
}

// RearmResult is the state before and after and the action recorded.
type RearmResult struct {
	Before domain.BucketState  `json:"before"`
	After  domain.BucketState  `json:"after"`
	Action domain.ActionRecord `json:"action"`
	// StopPercent is the stop threshold the refusal rule used.
	StopPercent float64 `json:"stopPercent"`
}

// RearmRefusedError is a rearm the rule refused; nothing was written.
type RearmRefusedError struct {
	Key         domain.BucketKey
	UsedPercent float64
	StopPercent float64
}

func (e *RearmRefusedError) Error() string {
	return fmt.Sprintf("refusing to rearm %s: stored usage %.0f%% is at or above stop_percent %.0f%%, so the next reading would stop it again; pass --force to rearm anyway",
		e.Key, e.UsedPercent, e.StopPercent)
}

// UnknownBucketError names a key the store does not hold and the keys it does.
type UnknownBucketError struct {
	Key   string
	Known []string
}

func (e *UnknownBucketError) Error() string {
	if len(e.Known) == 0 {
		return fmt.Sprintf("unknown bucket %q: the state database holds no buckets yet (they appear once a provider reports usage)", e.Key)
	}
	return fmt.Sprintf("unknown bucket %q; known buckets: %s", e.Key, strings.Join(e.Known, ", "))
}

// RearmBucket applies one operator rearm. The action is recorded after the
// state is saved; a save failure leaves the stored phase and returns the
// error, and a failure to record the action is returned too, since a phase
// change without its record is what the contract forbids.
func RearmBucket(ctx context.Context, cfg config.Config, store BucketStore, req RearmRequest, now time.Time) (RearmResult, error) {
	if strings.TrimSpace(req.Reason) == "" {
		return RearmResult{}, errors.New("a rearm needs --reason TEXT: it is recorded with the action")
	}
	states, err := store.ListBuckets(ctx)
	if err != nil {
		return RearmResult{}, err
	}
	var before domain.BucketState
	found := false
	known := make([]string, 0, len(states))
	for _, st := range states {
		known = append(known, st.Key.String())
		if st.Key == req.Key {
			before = st
			found = true
		}
	}
	if !found {
		sort.Strings(known)
		return RearmResult{}, &UnknownBucketError{Key: req.Key.String(), Known: known}
	}
	thresholds := ThresholdsFor(cfg, req.Key, before.LimitName, before.WindowDuration)
	if before.UsedPercent >= thresholds.StopPercent && !req.Force {
		return RearmResult{}, &RearmRefusedError{Key: req.Key, UsedPercent: before.UsedPercent, StopPercent: thresholds.StopPercent}
	}
	after := before
	after.Phase = domain.PhaseNormal
	t := now
	after.RecoveredAt = &t
	after.StoppedAt = nil
	after.DrainDeadline = nil
	after.ProbedAt = nil
	after.ETAStrikes = 0
	after.RearmObservations = 0
	after.Healthy = before.UsedPercent < thresholds.WarnPercent
	after.UpdatedAt = now
	if err := store.SaveBucket(ctx, after); err != nil {
		return RearmResult{}, fmt.Errorf("save bucket %s: %w (stored phase %s kept)", req.Key, err, before.Phase)
	}
	if err := store.ClearThreadNotices(ctx, req.Key); err != nil {
		return RearmResult{}, fmt.Errorf("clear thread notices for %s: %w", req.Key, err)
	}
	actor := req.Actor
	if actor == "" {
		actor = "operator"
	}
	forced := ""
	if req.Force {
		forced = ", forced"
	}
	rec := domain.ActionRecord{
		At: now, Kind: domain.ActionRearm, Bucket: req.Key.String(),
		Detail: fmt.Sprintf("rearmed by %s: %s (phase %s -> normal at %.0f%%%s)", actor, strings.TrimSpace(req.Reason), before.Phase, before.UsedPercent, forced),
	}
	if err := store.RecordAction(ctx, rec); err != nil {
		return RearmResult{}, fmt.Errorf("record rearm of %s: %w (the phase is already normal)", req.Key, err)
	}
	return RearmResult{Before: before, After: after, Action: rec, StopPercent: thresholds.StopPercent}, nil
}
