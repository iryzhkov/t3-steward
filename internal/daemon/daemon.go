// Package daemon wires the quota source, the policy engine, the state store
// and the T3 control adapter into the long-running watchdog loop.
package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/notify"
	"github.com/iryzhkov/t3-steward/internal/policy"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Source delivers quota snapshots.
type Source interface {
	Run(ctx context.Context, output chan<- domain.QuotaSnapshot) error
}

// Controller is the subset of the T3 adapter the daemon uses. It is an
// interface so that tests can substitute a fake.
type Controller interface {
	ListThreads(ctx context.Context) ([]domain.Thread, error)
	GetThread(ctx context.Context, threadID string) (*domain.Thread, error)
	LastUserMessageAt(ctx context.Context, threadID string) (*time.Time, error)
	WarnThread(ctx context.Context, thread domain.Thread, warning domain.Warning) error
	StopThread(ctx context.Context, thread domain.Thread, mode t3control.StopMode) error
	ResumeThread(ctx context.Context, thread domain.Thread, prompt string) error
}

// BacklogRunner is ticked with the current threads and bucket states after
// every poll; the backlog package implements it.
type BacklogRunner interface {
	Tick(ctx context.Context, threads []domain.Thread, buckets []domain.BucketState)
}

// Daemon is the watchdog.
type Daemon struct {
	cfg      config.Config
	log      *slog.Logger
	store    *sqlite.Store
	control  Controller
	source   Source
	notifier notify.Notifier
	// ControlAllowed is false when the T3 version is outside the tested
	// range and no override was given; the daemon then only monitors.
	ControlAllowed bool
	ControlReason  string

	now func() time.Time

	// Usage optionally receives token samples from the source; they are
	// stored for reports.
	Usage <-chan domain.UsageSample
	// Backlog, when set, is ticked after every thread poll.
	Backlog BacklogRunner
	// Waits, when set, is ticked after every thread poll.
	Waits BacklogRunner
	// Archive, when set, is ticked after every thread poll and runs once a
	// day.
	Archive BacklogRunner

	mu      sync.Mutex
	engines map[domain.BucketKey]*policy.Engine
	// threadModels caches thread id to model id for attribution.
	threadModels map[string]string
	// lastResume is the last resume dispatch per provider instance.
	lastResume map[string]time.Time
	// probed records, per provider instance, the reset time a probe resume
	// was sent for.
	probed map[string]time.Time
}

// New builds a daemon.
func New(cfg config.Config, logger *slog.Logger, store *sqlite.Store, control Controller, source Source) *Daemon {
	if logger == nil {
		logger = slog.Default()
	}
	return &Daemon{
		cfg:            cfg,
		log:            logger.With("component", "daemon"),
		store:          store,
		control:        control,
		source:         source,
		notifier:       notify.Notifier{Enabled: cfg.Notifications.Desktop, Logger: logger},
		ControlAllowed: true,
		now:            time.Now,
		engines:        map[domain.BucketKey]*policy.Engine{},
		lastResume:     map[string]time.Time{},
		probed:         map[string]time.Time{},
		threadModels:   map[string]string{},
	}
}

// SetClock replaces the clock, for tests.
func (d *Daemon) SetClock(now func() time.Time) { d.now = now }

// Tick fires timer-driven actions (grace periods). Exposed for replay.
func (d *Daemon) Tick(ctx context.Context) { d.tickBuckets(ctx) }

// Poll reconciles thread state and resume intents. Exposed for replay.
func (d *Daemon) Poll(ctx context.Context) { d.pollThreads(ctx) }

// modelFor returns the cached model id of a thread, or empty.
func (d *Daemon) modelFor(threadID string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.threadModels[threadID]
}

// recordUsage stores a token sample, filling in the thread's model when
// the provider did not name one.
func (d *Daemon) recordUsage(ctx context.Context, u domain.UsageSample) {
	if u.Model == "" {
		u.Model = d.modelFor(u.ThreadID)
	}
	if err := d.store.RecordUsage(ctx, u); err != nil {
		d.log.Warn("record usage sample", "err", err)
	}
}

// Run executes the loop until ctx is cancelled.
func (d *Daemon) Run(ctx context.Context) error {
	snapshots := make(chan domain.QuotaSnapshot, 256)
	sourceErr := make(chan error, 1)
	go func() {
		sourceErr <- d.source.Run(ctx, snapshots)
	}()

	d.log.Info("watchdog running",
		"dry_run", d.cfg.Policy.DryRun,
		"control_allowed", d.ControlAllowed,
		"resume_enabled", d.cfg.Resume.Enabled,
		"warn", d.cfg.Policy.WarnPercent, "drain", d.cfg.Policy.DrainPercent, "stop", d.cfg.Policy.StopPercent)
	if !d.ControlAllowed {
		d.log.Warn("control actions disabled", "reason", d.ControlReason)
	}

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	poll := time.NewTicker(d.cfg.Polling.SnapshotInterval.D())
	defer poll.Stop()
	prune := time.NewTicker(time.Hour)
	defer prune.Stop()

	// Reconcile once at start so a restart during a grace period or a
	// pending resume picks up where it left off.
	d.pollThreads(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-sourceErr:
			if err != nil && ctx.Err() == nil {
				return fmt.Errorf("quota source stopped: %w", err)
			}
			return nil
		case snap := <-snapshots:
			d.HandleSnapshot(ctx, snap)
		case u := <-d.Usage:
			d.recordUsage(ctx, u)
		case <-tick.C:
			d.tickBuckets(ctx)
		case <-poll.C:
			d.pollThreads(ctx)
		case <-prune.C:
			_ = d.store.PruneEvents(ctx, d.now().Add(-7*24*time.Hour))
			_ = d.store.PruneHistory(ctx, d.now().Add(-d.cfg.Policy.HistoryRetention.D()))
		}
	}
}

// engineFor returns the policy engine for a bucket, applying the first
// matching override.
func (d *Daemon) engineFor(key domain.BucketKey, limitName string) *policy.Engine {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e, ok := d.engines[key]; ok {
		return e
	}
	t := policy.Thresholds{
		WarnPercent:       d.cfg.Policy.WarnPercent,
		DrainPercent:      d.cfg.Policy.DrainPercent,
		StopPercent:       d.cfg.Policy.StopPercent,
		RearmPercent:      d.cfg.Policy.RearmPercent,
		GracePeriod:       d.cfg.Policy.GracePeriod.D(),
		RearmObservations: d.cfg.Policy.RearmObservations,
		ResetTolerance:    5 * time.Minute,
		RateWindow:        d.cfg.Policy.RateWindow.D(),
		WarnETA:           d.cfg.Policy.WarnETA.D(),
		DrainETA:          d.cfg.Policy.DrainETA.D(),
		StopETA:           d.cfg.Policy.StopETA.D(),
		ResetExemption:    d.cfg.Policy.ResetExemption.D(),
		RunwayMargin:      d.cfg.Policy.RunwayMargin,
	}
	for _, o := range d.cfg.Overrides {
		if !overrideMatches(o, key, limitName) {
			continue
		}
		if o.WarnPercent != nil {
			t.WarnPercent = *o.WarnPercent
		}
		if o.DrainPercent != nil {
			t.DrainPercent = *o.DrainPercent
		}
		if o.StopPercent != nil {
			t.StopPercent = *o.StopPercent
		}
		if o.GracePeriod != nil {
			t.GracePeriod = o.GracePeriod.D()
		}
		d.log.Info("threshold override applied", "bucket", key.String(), "warn", t.WarnPercent, "drain", t.DrainPercent, "stop", t.StopPercent)
		break
	}
	e := policy.New(t)
	d.engines[key] = e
	return e
}

func overrideMatches(o config.Override, key domain.BucketKey, limitName string) bool {
	match := func(pattern, value string) bool {
		if pattern == "" {
			return true
		}
		ok, err := path.Match(strings.ToLower(pattern), strings.ToLower(value))
		return err == nil && ok
	}
	if !match(o.Match.Provider, key.ProviderInstanceID) {
		return false
	}
	if !match(o.Match.Window, key.Window) {
		return false
	}
	if o.Match.LimitName != "" && !match(o.Match.LimitName, limitName) && !match(o.Match.LimitName, key.LimitID) {
		return false
	}
	return true
}

func (d *Daemon) ignoredWindow(window string) bool {
	w := strings.ToLower(window)
	for _, ig := range d.cfg.Policy.IgnoreWindows {
		if ig == "" {
			continue
		}
		if ok, err := path.Match(strings.ToLower(ig), w); err == nil && ok {
			return true
		}
	}
	return false
}

// HandleSnapshot runs one observation through the policy engine and
// executes the resulting actions.
func (d *Daemon) HandleSnapshot(ctx context.Context, snap domain.QuotaSnapshot) {
	now := d.now()
	if o, ok := d.cfg.Policy.Windows[snap.Key.Window]; ok {
		if o.Label != "" {
			snap.LimitName = o.Label
		}
		if o.Model != "" {
			snap.ModelSelector = o.Model
		}
	}
	log := d.log.With("bucket", snap.Key.String(), "used", fmt.Sprintf("%.0f%%", snap.UsedPercent))
	if age := now.Sub(snap.ObservedAt); age > d.cfg.Policy.MaxSnapshotAge.D() {
		log.Debug("ignoring old snapshot", "age", age.Round(time.Second))
		return
	}
	fresh, err := d.store.MarkEventSeen(ctx, snap.SourceEventID+"#"+snap.Key.Window, now)
	if err != nil {
		log.Error("record event", "err", err)
		return
	}
	if !fresh {
		log.Debug("duplicate event")
		return
	}
	prev, err := d.store.LoadBucket(ctx, snap.Key)
	if err != nil {
		log.Error("load bucket state", "err", err)
		return
	}
	engine := d.engineFor(snap.Key, snap.LimitName)
	decision := engine.Evaluate(snap, prev, now)
	if decision.Ignored != "" {
		log.Debug("snapshot ignored", "reason", decision.Ignored)
		return
	}
	if err := d.store.SaveBucket(ctx, decision.State); err != nil {
		log.Error("save bucket state", "err", err)
		return
	}
	if err := d.store.RecordObservation(ctx, domain.Observation{
		Key: snap.Key, ObservedAt: snap.ObservedAt, UsedPercent: snap.UsedPercent, ResetsAt: snap.ResetsAt,
		EventID: snap.SourceEventID, ThreadID: snap.ThreadID, Model: d.modelFor(snap.ThreadID),
	}); err != nil {
		log.Warn("record observation", "err", err)
	}
	resets := "none"
	if snap.ResetsAt != nil {
		resets = snap.ResetsAt.Local().Format(time.RFC3339)
	}
	log.Info("quota observed", "phase", string(decision.State.Phase), "resets_at", resets, "healthy", decision.State.Healthy)
	if d.ignoredWindow(snap.Key.Window) && len(decision.Actions) > 0 {
		log.Info("window is in ignore_windows; actions suppressed", "actions", len(decision.Actions))
		return
	}
	if d.cfg.QuotaChecksEnabled() {
		for _, a := range decision.Actions {
			d.execute(ctx, a, decision.State)
		}
	}
}

// tickBuckets fires grace-period timers.
func (d *Daemon) tickBuckets(ctx context.Context) {
	if !d.cfg.QuotaChecksEnabled() {
		return
	}
	states, err := d.store.ListBuckets(ctx)
	if err != nil {
		d.log.Error("list buckets", "err", err)
		return
	}
	now := d.now()
	for _, st := range states {
		if st.Phase != domain.PhaseDraining {
			continue
		}
		decision := d.engineFor(st.Key, st.LimitName).Tick(st, now)
		if decision.Ignored != "" {
			d.log.Info("grace period expired without a stop", "bucket", st.Key.String(), "reason", decision.Ignored)
			_ = d.store.SaveBucket(ctx, decision.State)
			continue
		}
		if len(decision.Actions) == 0 {
			continue
		}
		if err := d.store.SaveBucket(ctx, decision.State); err != nil {
			d.log.Error("save bucket state", "err", err)
			continue
		}
		for _, a := range decision.Actions {
			d.execute(ctx, a, decision.State)
		}
	}
}

// pollThreads reconciles thread state: stops newly running threads in a
// stopped bucket, and advances resume intents.
func (d *Daemon) pollThreads(ctx context.Context) {
	threads, err := d.control.ListThreads(ctx)
	if err != nil {
		d.log.Warn("cannot list T3 threads", "err", err)
		return
	}
	d.mu.Lock()
	for _, t := range threads {
		if t.Model != "" {
			d.threadModels[t.ID] = t.Model
		}
	}
	d.mu.Unlock()
	states, err := d.store.ListBuckets(ctx)
	if err != nil {
		d.log.Error("list buckets", "err", err)
		return
	}
	now := d.now()
	stoppedAny := false
	for _, st := range states {
		if !d.cfg.QuotaChecksEnabled() || st.Phase == domain.PhaseNormal || d.ignoredWindow(st.Key.Window) {
			continue
		}
		if st.ResetsAt != nil && !st.ResetsAt.After(now) {
			// The window passed; wait for a fresh snapshot to rearm.
			continue
		}
		var running []domain.Thread
		for _, t := range threads {
			if t.Running && t.MatchesBucket(st.Key, st.ModelSelector) {
				running = append(running, t)
			}
		}
		if len(running) == 0 {
			continue
		}
		snap := snapshotFromState(st)
		switch st.Phase {
		case domain.PhaseWarned, domain.PhaseDraining:
			// A thread that started after the threshold was crossed still
			// gets the notice; warnThreads sends each kind once per thread
			// and window, so threads notified earlier are skipped.
			kind := domain.ActionWarn
			if st.Phase == domain.PhaseDraining {
				kind = domain.ActionDrain
			}
			d.warnThreads(ctx, running, domain.Action{
				Kind: kind, Bucket: st.Key, Snapshot: snap,
				Reason: fmt.Sprintf("thread running while %s is %s at %.0f%%", st.Key, st.Phase, st.UsedPercent),
			}, st)
		case domain.PhaseStopped:
			if !d.cfg.Policy.StopNewSessions {
				continue
			}
			d.stopThreads(ctx, running, domain.Action{
				Kind: domain.ActionStop, Bucket: st.Key, Snapshot: snap,
				Reason: fmt.Sprintf("thread started while %s is stopped at %.0f%%", st.Key, st.UsedPercent),
			}, st)
			stoppedAny = true
		}
	}
	if stoppedAny {
		// The list above predates the stops; resume bookkeeping needs the
		// post-stop state or it would read a just-stopped thread as running.
		if fresh, err := d.control.ListThreads(ctx); err == nil {
			threads = fresh
		} else {
			return
		}
	}
	if d.cfg.QuotaChecksEnabled() {
		d.advanceResumes(ctx, threads, states)
	}
	if d.Backlog != nil {
		d.Backlog.Tick(ctx, threads, states)
	}
	if d.Waits != nil {
		d.Waits.Tick(ctx, threads, states)
	}
	if d.Archive != nil {
		d.Archive.Tick(ctx, threads, states)
	}
}

func snapshotFromState(st domain.BucketState) domain.QuotaSnapshot {
	return domain.QuotaSnapshot{
		Key: st.Key, LimitName: st.LimitName, UsedPercent: st.UsedPercent, ResetsAt: st.ResetsAt,
		ModelSelector: st.ModelSelector, ObservedAt: st.ObservedAt, SourceEventID: st.LastEventID,
	}
}

func (d *Daemon) record(ctx context.Context, rec domain.ActionRecord) {
	if rec.At.IsZero() {
		rec.At = d.now()
	}
	rec.DryRun = rec.DryRun || d.cfg.Policy.DryRun || !d.ControlAllowed
	if err := d.store.RecordAction(ctx, rec); err != nil {
		d.log.Error("record action", "err", err)
	}
}
