package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"

	t3control "github.com/iryzhkov/t3-quota-watchdog/internal/control/t3"
	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// execute carries out one policy action.
func (d *Daemon) execute(ctx context.Context, a domain.Action, state domain.BucketState) {
	log := d.log.With("action", string(a.Kind), "bucket", a.Bucket.String(), "reason", a.Reason)
	switch a.Kind {
	case domain.ActionRearm:
		log.Info("bucket rearmed")
		if err := d.store.ClearThreadNotices(ctx, a.Bucket); err != nil {
			log.Error("clear thread notices", "err", err)
		}
		d.record(ctx, domain.ActionRecord{Kind: a.Kind, Bucket: a.Bucket.String(), Detail: a.Reason})
		return
	case domain.ActionWarn, domain.ActionDrain:
		affected, err := d.affectedThreads(ctx, a)
		if err != nil {
			log.Warn("cannot resolve affected threads", "err", err)
			d.record(ctx, domain.ActionRecord{Kind: a.Kind, Bucket: a.Bucket.String(), Detail: a.Reason, Err: err.Error()})
			return
		}
		log.Info("quota threshold crossed", "affected_running_threads", len(affected))
		if len(affected) == 0 {
			d.record(ctx, domain.ActionRecord{Kind: a.Kind, Bucket: a.Bucket.String(), Detail: a.Reason + " (no running threads affected)"})
			return
		}
		d.warnThreads(ctx, affected, a, state)
	case domain.ActionStop:
		affected, err := d.affectedThreads(ctx, a)
		if err != nil {
			log.Warn("cannot resolve affected threads", "err", err)
			d.record(ctx, domain.ActionRecord{Kind: a.Kind, Bucket: a.Bucket.String(), Detail: a.Reason, Err: err.Error()})
			return
		}
		log.Warn("quota stop threshold crossed", "affected_running_threads", len(affected))
		if len(affected) == 0 {
			d.record(ctx, domain.ActionRecord{Kind: a.Kind, Bucket: a.Bucket.String(), Detail: a.Reason + " (no running threads affected)"})
			return
		}
		d.stopThreads(ctx, affected, a, state)
	}
}

// affectedThreads lists running threads the bucket applies to.
func (d *Daemon) affectedThreads(ctx context.Context, a domain.Action) ([]domain.Thread, error) {
	threads, err := d.control.ListThreads(ctx)
	if err != nil {
		return nil, err
	}
	var out []domain.Thread
	for _, t := range threads {
		if t.Running && t.MatchesBucket(a.Bucket, a.Snapshot.ModelSelector) {
			out = append(out, t)
		}
	}
	return out, nil
}

// warnThreads sends the warn or drain message once per thread and epoch.
func (d *Daemon) warnThreads(ctx context.Context, threads []domain.Thread, a domain.Action, state domain.BucketState) {
	tmpl := d.cfg.Messages.Warn
	if a.Kind == domain.ActionDrain {
		tmpl = d.cfg.Messages.Drain
	}
	engine := d.engineFor(a.Bucket, a.Snapshot.LimitName)
	text, err := renderMessage(tmpl, a.Snapshot, engine.Thresholds().GracePeriod, d.now())
	if err != nil {
		d.log.Error("render message", "err", err)
		d.record(ctx, domain.ActionRecord{Kind: a.Kind, Bucket: a.Bucket.String(), Detail: a.Reason, Err: err.Error()})
		return
	}
	for _, t := range threads {
		fresh, err := d.store.MarkThreadNotice(ctx, t.ID, a.Bucket, state.Epoch, a.Kind, d.now())
		if err != nil {
			d.log.Error("record thread notice", "thread", t.ID, "err", err)
			continue
		}
		if !fresh {
			d.log.Debug("thread already notified this epoch", "thread", t.ID, "kind", string(a.Kind))
			continue
		}
		rec := domain.ActionRecord{Kind: a.Kind, Bucket: a.Bucket.String(), ThreadID: t.ID, Detail: fmt.Sprintf("%s: %s", t.Title, a.Reason)}
		if !d.ControlAllowed {
			rec.Detail += " (control disabled: " + d.ControlReason + ")"
			d.record(ctx, rec)
			continue
		}
		if err := d.control.WarnThread(ctx, t, domain.Warning{Kind: a.Kind, Text: text}); err != nil {
			d.log.Error("warn thread", "thread", t.ID, "err", err)
			rec.Err = err.Error()
		}
		d.record(ctx, rec)
		if a.Kind == domain.ActionDrain && rec.Err == "" {
			// The drain request asks the thread to checkpoint and stop on
			// its own. A thread that complies was stopped by the watchdog
			// in every sense that matters, so it gets a resume intent now;
			// the hard stop later merges into it if the thread kept going.
			d.saveIntent(ctx, t, a, state, d.now(), true)
		}
	}
}

// stopThreads dispatches stops to every thread, verifies they left the
// running state, retries with escalation, and records resume intents for
// the threads it actually stopped.
func (d *Daemon) stopThreads(ctx context.Context, threads []domain.Thread, a domain.Action, state domain.BucketState) {
	now := d.now()
	var targets []domain.Thread
	for _, t := range threads {
		fresh, err := d.store.MarkThreadNotice(ctx, t.ID, a.Bucket, state.Epoch, domain.ActionStop, now)
		if err != nil {
			d.log.Error("record thread notice", "thread", t.ID, "err", err)
			continue
		}
		if !fresh {
			d.log.Debug("thread already stopped this epoch", "thread", t.ID)
			continue
		}
		targets = append(targets, t)
	}
	if len(targets) == 0 {
		return
	}
	if !d.ControlAllowed {
		for _, t := range targets {
			d.record(ctx, domain.ActionRecord{Kind: domain.ActionStop, Bucket: a.Bucket.String(), ThreadID: t.ID,
				Detail: fmt.Sprintf("%s: %s (control disabled: %s)", t.Title, a.Reason, d.ControlReason)})
		}
		return
	}
	dry := d.cfg.Policy.DryRun
	mode := t3control.StopMode(d.cfg.Policy.StopMode)
	remaining := map[string]domain.Thread{}
	for _, t := range targets {
		remaining[t.ID] = t
		if err := d.control.StopThread(ctx, t, mode); err != nil {
			d.log.Error("stop thread", "thread", t.ID, "err", err)
		}
	}
	stopped := map[string]domain.Thread{}
	if dry {
		// Nothing was sent; treat every target as stopped for the
		// simulation so the resume path can be exercised too.
		for id, t := range remaining {
			stopped[id] = t
		}
		remaining = map[string]domain.Thread{}
	}
	attempts := 0
	for len(remaining) > 0 && attempts <= d.cfg.Policy.StopRetries {
		d.waitStopped(ctx, remaining, stopped, d.cfg.Policy.StopVerifyTimeout.D())
		if len(remaining) == 0 {
			break
		}
		attempts++
		if attempts > d.cfg.Policy.StopRetries {
			break
		}
		retryMode := mode
		if d.cfg.Policy.EscalateToSessionStop {
			retryMode = t3control.StopSession
		}
		for _, t := range remaining {
			d.log.Warn("thread still running after stop; retrying", "thread", t.ID, "attempt", attempts, "mode", string(retryMode))
			if err := d.control.StopThread(ctx, t, retryMode); err != nil {
				d.log.Error("stop thread", "thread", t.ID, "err", err)
			}
		}
	}
	for _, t := range targets {
		rec := domain.ActionRecord{Kind: domain.ActionStop, Bucket: a.Bucket.String(), ThreadID: t.ID,
			Detail: fmt.Sprintf("%s: %s", t.Title, a.Reason), DryRun: dry}
		if _, ok := stopped[t.ID]; ok {
			d.record(ctx, rec)
			d.saveIntent(ctx, t, a, state, d.now(), false)
			continue
		}
		rec.Err = "thread still running after retries"
		d.record(ctx, rec)
		d.notifier.Send(ctx, "T3 quota watchdog: stop failed",
			fmt.Sprintf("Thread %q is still running after %d attempts (%s).", t.Title, attempts, a.Bucket))
	}
	if !dry && len(stopped) > 0 {
		names := make([]string, 0, len(stopped))
		for _, t := range stopped {
			names = append(names, t.Title)
		}
		d.notifier.Send(ctx, "T3 quota watchdog: threads stopped",
			fmt.Sprintf("%s at %.0f%%. Stopped: %s", a.Snapshot.LimitName, a.Snapshot.UsedPercent, strings.Join(names, ", ")))
	}
}

// waitStopped polls the thread list until every remaining thread stopped or
// the timeout passed, moving stopped ones into the stopped map.
func (d *Daemon) waitStopped(ctx context.Context, remaining, stopped map[string]domain.Thread, timeout time.Duration) {
	// Wall-clock deadline on purpose: this waits for the real T3 server,
	// not for the policy clock.
	deadline := time.Now().Add(timeout)
	for {
		threads, err := d.control.ListThreads(ctx)
		if err == nil {
			present := map[string]domain.Thread{}
			for _, t := range threads {
				present[t.ID] = t
			}
			for id, t := range remaining {
				cur, ok := present[id]
				if !ok || !cur.Running {
					if ok {
						t = cur
					}
					stopped[id] = t
					delete(remaining, id)
				}
			}
		} else {
			d.log.Warn("cannot verify stop", "err", err)
		}
		left := time.Until(deadline)
		if len(remaining) == 0 || left <= 0 {
			return
		}
		if left > 2*time.Second {
			left = 2 * time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(left):
		}
	}
}

func (d *Daemon) saveIntent(ctx context.Context, t domain.Thread, a domain.Action, state domain.BucketState, now time.Time, drain bool) {
	existing, found, err := d.store.LoadResumeIntent(ctx, t.ID)
	if err != nil {
		d.log.Error("load resume intent", "thread", t.ID, "err", err)
		return
	}
	intent := domain.ResumeIntent{
		ThreadID:           t.ID,
		ProviderInstanceID: t.ProviderInstanceID,
		Model:              t.Model,
		StoppedByWatchdog:  true,
		StoppedAt:          now,
		StoppedTurnID:      t.TurnID,
		StopGeneration:     fmt.Sprintf("%d", now.UnixNano()),
		CheckpointExpected: drain,
		Status:             domain.ResumePending,
		Buckets:            []domain.BucketKey{a.Bucket},
		Reason:             a.Reason,
		UpdatedAt:          now,
	}
	if found && (existing.Status == domain.ResumePending || existing.Status == domain.ResumeEligible) {
		// Stopped again (a hard stop after a drain request, or another
		// bucket): union the buckets so all of them must recover, and keep
		// the earliest stop time.
		seen := map[domain.BucketKey]bool{a.Bucket: true}
		for _, b := range existing.Buckets {
			if !seen[b] {
				intent.Buckets = append(intent.Buckets, b)
				seen[b] = true
			}
		}
		intent.StoppedAt = existing.StoppedAt
		if !drain {
			intent.CheckpointExpected = false
		}
	}
	if d.cfg.Policy.DryRun {
		intent.Reason += " (dry run)"
	}
	if err := d.store.SaveResumeIntent(ctx, intent); err != nil {
		d.log.Error("save resume intent", "thread", t.ID, "err", err)
	}
}
