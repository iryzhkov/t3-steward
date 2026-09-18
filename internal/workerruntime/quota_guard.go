package workerruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/policy"
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

// ThreadLister is the host's T3 thread list, from the worker's control
// client; the probe rule needs it to know that nothing on the host will
// produce a reading.
type ThreadLister interface {
	ListThreads(context.Context) ([]domain.Thread, error)
}

// bucketSaver and actionRecorder are what the probe rule needs from the
// store beyond listing: the state store implements both, a bare lister
// never probes.
type bucketSaver interface {
	SaveBucket(context.Context, domain.BucketState) error
}

type actionRecorder interface {
	RecordAction(context.Context, domain.ActionRecord) error
}

// HostQuotaGuard applies the watchdog's policy from the same configuration
// and the same state database the watchdog on this host uses.
type HostQuotaGuard struct {
	Config  config.Config
	Buckets BucketLister
	// Threads lists the host's T3 threads for the probe rule. Nil means the
	// list is unknown, and an unknown list never probes.
	Threads ThreadLister
	Now     func() time.Time
}

// engine is the policy engine for a bucket under this host's configuration,
// the same thresholds the watchdog applies to it.
func (g HostQuotaGuard) engine(key domain.BucketKey, limitName string, duration time.Duration) *policy.Engine {
	return policy.New(daemon.ThresholdsFor(g.Config, key, limitName, duration))
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
	now := g.now()
	// A bucket whose probe is still out has one owned thread running on
	// purpose, to produce the reading; pausing it again before the reading
	// arrives would make the probe pointless.
	considered := make([]domain.BucketState, 0, len(states))
	for _, st := range states {
		if g.probeOutstanding(st, now) {
			continue
		}
		considered = append(considered, st)
	}
	st, found := daemon.GoverningPause(g.Config, routeThread(route), considered, now)
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
	if ok {
		return true, why, nil
	}
	if st, found := byKey[pause.Bucket]; found {
		if probed, probeWhy, err := g.probe(ctx, st, now); err != nil {
			return false, "", err
		} else if probed {
			return true, probeWhy, nil
		} else if probeWhy != "" {
			why += "; " + probeWhy
		}
	}
	return false, why, nil
}

// probeOutstanding reports whether a probe was sent for the bucket's current
// epoch and no reading has arrived since, within the time a probe is given
// to produce one (resume.probe_after_reset again).
func (g HostQuotaGuard) probeOutstanding(st domain.BucketState, now time.Time) bool {
	if st.ProbedAt == nil || st.ObservedAt.After(*st.ProbedAt) {
		return false
	}
	return now.Before(st.ProbedAt.Add(g.Config.Resume.ProbeAfterReset.D()))
}

// probe applies the stage 2 probe rule to a bucket that has not recovered:
// once per bucket epoch, when the bucket is stopped below the current
// stop_percent, the stored reading is older than resume.probe_after_reset,
// the host's thread list is known and no running thread on it matches the
// bucket, one paused attempt is resumed to obtain the reading that no thread
// would otherwise produce. The probe is recorded on the bucket (ProbedAt) and
// as a resume action; the phase does not change, and the reading that follows
// rearms or re-stops the bucket honestly. When the rule refuses it says why,
// and an empty reason means the rule does not apply at all.
func (g HostQuotaGuard) probe(ctx context.Context, st domain.BucketState, now time.Time) (bool, string, error) {
	if st.Phase != domain.PhaseStopped || g.Config.Resume.ProbeAfterReset.D() <= 0 {
		return false, "", nil
	}
	if st.ProbedAt != nil {
		return false, fmt.Sprintf("a probe was sent at %s and only one probe is made per epoch", st.ProbedAt.UTC().Format(time.RFC3339)), nil
	}
	thresholds := daemon.ThresholdsFor(g.Config, st.Key, st.LimitName, st.WindowDuration)
	if st.UsedPercent >= thresholds.StopPercent {
		return false, "", nil
	}
	age := now.Sub(st.ObservedAt)
	if age < g.Config.Resume.ProbeAfterReset.D() {
		return false, "", nil
	}
	if g.Threads == nil {
		return false, "the host's thread list is unknown, so no probe is made", nil
	}
	threads, err := g.Threads.ListThreads(ctx)
	if err != nil {
		return false, "", err
	}
	for _, t := range threads {
		if t.Running && t.MatchesBucket(st.Key, st.ModelSelector) {
			return false, fmt.Sprintf("thread %s is running on the bucket and will produce a reading", t.ID), nil
		}
	}
	saver, canSave := g.Buckets.(bucketSaver)
	if !canSave {
		return false, "the bucket store cannot record a probe", nil
	}
	probed := now
	st.ProbedAt = &probed
	st.UpdatedAt = now
	if err := saver.SaveBucket(ctx, st); err != nil {
		return false, "", fmt.Errorf("record probe on %s: %w", st.Key, err)
	}
	why := fmt.Sprintf("probe: %s is stopped at %.0f%%, below stop_percent %.0f%%, with no reading for %s and nothing running on this host to produce one; one paused attempt resumes to obtain it",
		st.Key, st.UsedPercent, thresholds.StopPercent, age.Round(time.Minute))
	if recorder, ok := g.Buckets.(actionRecorder); ok {
		if err := recorder.RecordAction(ctx, domain.ActionRecord{At: now, Kind: domain.ActionResume, Bucket: st.Key.String(), Detail: why}); err != nil {
			return false, "", fmt.Errorf("record probe action for %s: %w", st.Key, err)
		}
	}
	return true, why, nil
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
