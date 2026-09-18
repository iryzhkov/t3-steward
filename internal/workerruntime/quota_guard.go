package workerruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// QuotaPause is the bucket that requires an attempt's thread to drain or
// stop right now.
type QuotaPause struct {
	Bucket      domain.BucketKey
	Phase       domain.Phase
	UsedPercent float64
	LimitName   string
	ResetsAt    *time.Time
	ObservedAt  time.Time
}

// Summary is the short form reported upward: "claudeAgent/claude/seven_day at 97%".
func (p QuotaPause) Summary() string {
	return fmt.Sprintf("%s at %.0f%%", p.Bucket.String(), p.UsedPercent)
}

// QuotaGuard is how the worker runtime consults the host watchdog's bucket
// state for the threads it owns. The watchdog leaves those threads alone (see
// daemon.ThreadOwnership); the runtime applies the same phase evaluation and
// the same recovery rules through this seam, so that a quota stop of an
// owned thread is a pause the attempt survives rather than a failure.
//
// A nil guard means the host has no watchdog state; the runtime then never
// pauses on its own, which is today's behaviour.
type QuotaGuard interface {
	// PauseRequired reports the bucket governing the route that is in the
	// draining or stopped phase, when one is.
	PauseRequired(ctx context.Context, route domain.ProviderRoute) (QuotaPause, bool, error)
	// ResumeAllowed reports whether the bucket that paused an attempt has
	// recovered under the watchdog's resume rules, and why not otherwise.
	ResumeAllowed(ctx context.Context, pause LocalThrottleRequest, route domain.ProviderRoute) (bool, string, error)
	// Observations lists the host's current bucket states for the
	// coordinator's admission.
	Observations(ctx context.Context) ([]domain.WorkerQuotaObservation, error)
}

// BucketLister reads the watchdog's stored bucket states.
type BucketLister interface {
	ListBuckets(context.Context) ([]domain.BucketState, error)
}

// HostQuotaGuard applies the watchdog's policy from the same configuration
// and the same state database the watchdog on this host uses.
type HostQuotaGuard struct {
	Config  config.Config
	Buckets BucketLister
	Now     func() time.Time
}

func (g HostQuotaGuard) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}

func routeThread(route domain.ProviderRoute) domain.Thread {
	return domain.Thread{ProviderInstanceID: route.ProviderInstanceID, Model: route.Model}
}

// PauseRequired implements QuotaGuard.
func (g HostQuotaGuard) PauseRequired(ctx context.Context, route domain.ProviderRoute) (QuotaPause, bool, error) {
	if g.Buckets == nil || !g.Config.QuotaChecksEnabled() {
		return QuotaPause{}, false, nil
	}
	states, err := g.Buckets.ListBuckets(ctx)
	if err != nil {
		return QuotaPause{}, false, err
	}
	st, found := daemon.GoverningPause(g.Config, routeThread(route), states, g.now())
	if !found {
		return QuotaPause{}, false, nil
	}
	return QuotaPause{
		Bucket: st.Key, Phase: st.Phase, UsedPercent: st.UsedPercent, LimitName: st.LimitName,
		ResetsAt: st.ResetsAt, ObservedAt: st.ObservedAt,
	}, true, nil
}

// ResumeAllowed implements QuotaGuard with the watchdog's eligibility rules.
// A bucket whose reset passed without a confirming reading may be probed by
// resuming, as the watchdog probes with one interactive thread.
func (g HostQuotaGuard) ResumeAllowed(ctx context.Context, pause LocalThrottleRequest, route domain.ProviderRoute) (bool, string, error) {
	if g.Buckets == nil {
		return false, "no watchdog state on this host", nil
	}
	if !g.Config.QuotaChecksEnabled() {
		return true, "quota checks are disabled", nil
	}
	states, err := g.Buckets.ListBuckets(ctx)
	if err != nil {
		return false, "", err
	}
	byKey := make(map[domain.BucketKey]domain.BucketState, len(states))
	for _, st := range states {
		byKey[st.Key] = st
	}
	now := g.now()
	ok, why := daemon.BucketsRecovered(g.Config, []domain.BucketKey{pause.Bucket}, pause.RequestedAt, routeThread(route), byKey, now, func(st domain.BucketState) bool {
		return daemon.ProbeWindowOpen(g.Config, st, now)
	})
	if ok && why == "" {
		why = fmt.Sprintf("%s recovered", pause.Bucket)
	}
	return ok, why, nil
}

// Observations implements QuotaGuard.
func (g HostQuotaGuard) Observations(ctx context.Context) ([]domain.WorkerQuotaObservation, error) {
	if g.Buckets == nil {
		return nil, nil
	}
	states, err := g.Buckets.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.WorkerQuotaObservation, 0, len(states))
	for _, st := range states {
		if st.ObservedAt.IsZero() {
			continue
		}
		out = append(out, domain.WorkerQuotaObservation{
			Key: st.Key, Phase: st.Phase, UsedPercent: st.UsedPercent, Healthy: st.Healthy,
			ObservedAt: st.ObservedAt, ResetsAt: st.ResetsAt, Epoch: st.Epoch,
			LimitName: st.LimitName, ModelSelector: st.ModelSelector,
		})
	}
	return out, nil
}
