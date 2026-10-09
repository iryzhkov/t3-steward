package workerruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The in-session resume after a provider-side error.
//
// A provider at capacity, overloaded, rate limiting, failing on its servers or
// leaving its session not ready ends a task's turn through no fault of the
// task. Collecting that turn failed the attempt and threw away the session,
// and a turn T3 left running under a failed session was never collected at
// all. Instead the worker records the error on the attempt and, after a
// bounded backoff, sends the same session a short message that tells the agent
// to pick up from continuation.md. Only once the schedule is spent does the
// attempt fail, with failure class infrastructure, its workspace and
// continuation checkpoint kept so that a retry can start from them.
//
// Nothing else that ends a turn is resumed this way: an agent that ended its
// turn, a failed command or test, a stop, a policy refusal or a request the
// provider refused for what it asked are the task's own ending (see
// backlog.ClassifyProviderTurnEnd).

// providerResumePurpose names the resume turn in its T3 identities.
const providerResumePurpose = "provider-resume"

// maxProviderErrorDetail bounds the provider message an attempt records.
const maxProviderErrorDetail = 512

// providerResumer is the optional driver side of the in-session resume.
type providerResumer interface {
	// ProviderTurnError reads the attempt's thread and reports the
	// provider-side error its latest turn ended with, if it ended with one.
	ProviderTurnError(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.ProviderTurnError, bool, error)
	// ResumeAfterProviderError starts one turn in the same session. The token
	// identifies the resume, so a retry of the same resume is recognised.
	ResumeAfterProviderError(ctx context.Context, pkg workerproto.ExecutionPackage, token, text string) error
}

// ProviderResumeRecord is the durable state of an attempt's in-session
// resumes, so that a worker restart neither resumes an ended turn twice nor
// loses the budget.
type ProviderResumeRecord struct {
	// TurnID is the ended provider turn the current state answers.
	TurnID     string                   `json:"turnId,omitempty"`
	Kind       domain.ProviderErrorKind `json:"kind,omitempty"`
	Detail     string                   `json:"detail,omitempty"`
	ObservedAt time.Time                `json:"observedAt"`
	// Errors counts the provider errors observed, Resumes the resumes
	// claimed, sent or not, and Budget the schedule length when the latest
	// one was claimed.
	Errors   int        `json:"errors,omitempty"`
	Resumes  int        `json:"resumes,omitempty"`
	Budget   int        `json:"budget,omitempty"`
	ResumeAt *time.Time `json:"resumeAt,omitempty"`
	// WaitReason is why a due resume is not sent, typed by its prefix.
	WaitReason string                     `json:"waitReason,omitempty"`
	State      domain.ProviderResumeState `json:"state,omitempty"`
}

// report is the record as the journal excerpt carries it.
func (p ProviderResumeRecord) report() *domain.WorkerProviderError {
	report := domain.WorkerProviderError{
		Kind: p.Kind, Detail: p.Detail, TurnID: p.TurnID, ObservedAt: p.ObservedAt, State: p.State,
		Resumes: p.Resumes, Budget: p.Budget, Errors: p.Errors, WaitReason: p.WaitReason,
	}
	if p.ResumeAt != nil {
		at := *p.ResumeAt
		report.ResumeAt = &at
	}
	return &report
}

// providerResumeSchedule is the backoff before each resume: the worker's own
// schedule, or the default, cut to the coordinator's maximum count and with
// no delay longer than its maximum delay. Its length is the budget.
func (r *Runtime) providerResumeSchedule() []time.Duration {
	schedule := r.config.ProviderResumeBackoff
	if len(schedule) == 0 {
		schedule = domain.DefaultProviderResumeBackoff
	}
	limit, longest := domain.MaxProviderResumes, domain.MaxProviderResumeDelay
	if policy := r.providerResume.Load(); policy != nil {
		limit = min(limit, policy.MaxResumes)
		if policy.MaxDelaySeconds > 0 {
			longest = min(longest, time.Duration(policy.MaxDelaySeconds)*time.Second)
		}
	}
	limit = min(limit, len(schedule))
	out := make([]time.Duration, 0, limit)
	for _, delay := range schedule[:limit] {
		out = append(out, min(max(delay, 0), longest))
	}
	return out
}

// holdForProviderError decides, for a turn that ended with nothing parking
// the attempt, whether the turn ended on a provider-side error the worker
// answers by resuming the same session. It reports true when the caller must
// not collect: a resume is scheduled, waiting for quota, or sent and its turn
// awaited, or the attempt has been failed and its failure published.
//
// A driver without the check, or a turn without an identity to bind a resume
// to, is collected as before.
func (r *Runtime) holdForProviderError(ctx context.Context, id string, record AttemptRecord, turnID string) (bool, error) {
	resumer, ok := r.driver.(providerResumer)
	if !ok || turnID == "" {
		return false, nil
	}
	pkg := record.Package.Package
	var current ProviderResumeRecord
	if record.ProviderResume != nil {
		current = *record.ProviderResume
	}
	if current.TurnID == turnID {
		switch current.State {
		case domain.ProviderResumeScheduled, domain.ProviderResumeQuotaWait:
			return true, r.resumeWhenDue(ctx, id, resumer, pkg, current)
		case domain.ProviderResumeSent:
			// The resume is in T3 and the turn it starts has not been
			// observed yet.
			return true, nil
		case domain.ProviderResumeExhausted:
			// The budget was spent but the failure did not become durable;
			// the attempt is not failed yet, so it is failed now.
			return true, r.failProviderError(ctx, id, record, current)
		case domain.ProviderResumeRecovered:
			return false, nil
		}
	}
	failure, provider, err := resumer.ProviderTurnError(ctx, pkg)
	if err != nil {
		// Collecting now could fail an attempt a resume would save; the next
		// pass reads the thread again.
		r.log.Warn("provider error check unavailable; collection deferred", "assignment", id, "error", r.loggedError(ctx, id, err))
		return true, nil
	}
	if !provider {
		if record.ProviderResume == nil || current.TurnID == turnID {
			return false, nil
		}
		// A resumed turn ended as the task's own ending: the resume worked.
		r.log.Info("resumed session ended its turn without a provider error; collected as usual",
			"assignment", id, "thread", pkg.Identity.ThreadID, "turn", turnID, "resumes", current.Resumes)
		return false, r.updateProviderResume(id, func(state *ProviderResumeRecord) {
			state.TurnID, state.State, state.ResumeAt, state.WaitReason = turnID, domain.ProviderResumeRecovered, nil, ""
		})
	}
	schedule := r.providerResumeSchedule()
	now := r.now()
	next := current
	next.TurnID, next.Kind, next.ObservedAt = turnID, failure.Kind, now
	next.Detail = truncateText(r.checkedText(ctx, id, pkg, failure.Detail), maxProviderErrorDetail)
	next.Errors++
	next.Budget, next.WaitReason = len(schedule), ""
	if current.Resumes >= len(schedule) {
		next.State, next.ResumeAt = domain.ProviderResumeExhausted, nil
		if err := r.updateProviderResume(id, func(state *ProviderResumeRecord) { *state = next }); err != nil {
			return true, err
		}
		return true, r.failProviderError(ctx, id, record, next)
	}
	resumeAt := now.Add(schedule[current.Resumes])
	next.Resumes = current.Resumes + 1
	next.State, next.ResumeAt = domain.ProviderResumeScheduled, &resumeAt
	// The claim is durable before the effect, like a turn-end nudge: a
	// worker that dies after sending resends under the same identity, which
	// T3 recognises, rather than sending a second resume.
	if err := r.updateProviderResume(id, func(state *ProviderResumeRecord) { *state = next }); err != nil {
		return true, err
	}
	r.log.Warn("provider turn ended on a provider-side error; the same session is resumed after a backoff",
		"assignment", id, "thread", pkg.Identity.ThreadID, "turn", turnID, "kind", string(failure.Kind),
		"resume", next.Resumes, "of", next.Budget, "at", resumeAt.Format(time.RFC3339))
	return true, r.resumeWhenDue(ctx, id, resumer, pkg, next)
}

// resumeWhenDue sends a scheduled resume once its backoff has passed and the
// route's quota admits a turn. A resume that cannot be sent yet is retried on
// a later pass under the same identity.
func (r *Runtime) resumeWhenDue(ctx context.Context, id string, resumer providerResumer, pkg workerproto.ExecutionPackage, current ProviderResumeRecord) error {
	// Resumes includes the pending claim, so it is admissible only when its
	// ordinal still fits the current budget. Recheck even during backoff:
	// max_resumes=0 disables a pending send at the next reconcile.
	budget := len(r.providerResumeSchedule())
	if current.Budget != budget || current.Resumes > budget {
		current.Budget = budget
		if current.Resumes > budget {
			current.State, current.ResumeAt, current.WaitReason = domain.ProviderResumeExhausted, nil, ""
		}
		if err := r.updateProviderResume(id, func(state *ProviderResumeRecord) { *state = current }); err != nil {
			return err
		}
	}
	if current.State == domain.ProviderResumeExhausted {
		record, exists, err := r.currentRecord(id)
		if err != nil || !exists {
			return err
		}
		return r.failProviderError(ctx, id, record, current)
	}
	if current.ResumeAt != nil && r.now().Before(*current.ResumeAt) {
		return nil
	}
	if reason := r.providerResumeQuotaWait(ctx, id, pkg); reason != "" {
		if current.State == domain.ProviderResumeQuotaWait && current.WaitReason == reason {
			return nil
		}
		r.log.Info("provider-error resume is due but quota admits no turn; it waits", "assignment", id, "reason", reason)
		return r.updateProviderResume(id, func(state *ProviderResumeRecord) {
			if state.TurnID == current.TurnID {
				state.State, state.WaitReason = domain.ProviderResumeQuotaWait, reason
			}
		})
	}
	token := pkg.Identity.DispatchToken + "/" + providerResumePurpose + "/" + current.TurnID
	if err := resumer.ResumeAfterProviderError(ctx, pkg, token, providerResumeText(current)); err != nil {
		r.log.Warn("provider-error resume is not delivered yet; it is sent again on a later pass", "assignment", id, "error", r.loggedError(ctx, id, err))
		return nil
	}
	r.log.Info("resumed the same session after a provider error", "assignment", id, "thread", pkg.Identity.ThreadID,
		"turn", current.TurnID, "resume", current.Resumes, "of", current.Budget)
	return r.updateProviderResume(id, func(state *ProviderResumeRecord) {
		if state.TurnID == current.TurnID {
			state.State, state.WaitReason = domain.ProviderResumeSent, ""
		}
	})
}

// providerResumeQuotaWait is the typed reason a due resume waits for quota,
// or empty when the route admits a turn: the coordinator's statement that
// the route's pool is closed or draining, or this host's watchdog bucket for
// the route in its drain or stop phase. A host whose quota state cannot be
// read resumes, as a running attempt keeps running then (see pauseForQuota).
func (r *Runtime) providerResumeQuotaWait(ctx context.Context, id string, pkg workerproto.ExecutionPackage) string {
	if policy := r.providerResume.Load(); policy != nil && pkg.Route.QuotaPoolID != "" {
		for _, pool := range policy.ClosedPools {
			if pool.PoolID != pkg.Route.QuotaPoolID {
				continue
			}
			reason := fmt.Sprintf("%s: pool %s admission is %s", ProviderResumeQuotaClosed, pool.PoolID, pool.Admission)
			if pool.Reason != "" {
				reason += ": " + pool.Reason
			}
			return truncateText(reason, maxProviderErrorDetail)
		}
	}
	if r.config.Quota == nil {
		return ""
	}
	pause, required, err := r.config.Quota.PauseRequired(ctx, pkg.Route)
	if err != nil {
		r.log.Warn("host quota state unavailable; provider-error resume proceeds", "assignment", id, "error", r.loggedError(ctx, id, err))
		return ""
	}
	if !required {
		return ""
	}
	return truncateText(ProviderResumeQuotaClosed+": "+pause.Summary(), maxProviderErrorDetail)
}

// ProviderResumeQuotaClosed is the type prefix of the wait reason of a
// resume that is due while the route's quota admits no turn.
const ProviderResumeQuotaClosed = "quota-closed"

// ProviderErrorFailure is the reason prefix of an attempt failed after its
// provider-error resumes were spent; the failure class comes first.
const ProviderErrorFailure = domain.FailureClassInfrastructure + ": provider error"

// failProviderError fails an attempt whose turn ended on a provider error
// once its resumes are spent. Uncommitted work is snapshotted first where the
// driver can, and the workspace and continuation checkpoint stay on the
// worker for the attempt's retention, so a retry can start from them.
func (r *Runtime) failProviderError(ctx context.Context, id string, record AttemptRecord, state ProviderResumeRecord) error {
	pkg := record.Package.Package
	evidence := []string{declaredOutputEvidence(pkg, record.WorkspacePath)}
	if inspector, ok := r.driver.(turnEndInspector); ok {
		retained, err := inspector.SnapshotWorkInProgress(ctx, pkg, record.WorkspacePath)
		if err != nil {
			retained = "wip.bundle not retained: " + r.checkedText(ctx, id, pkg, err.Error())
		}
		if retained != "" {
			evidence = append(evidence, truncateText(retained, 512))
		}
	}
	// The checkpoint of the ended turn is recorded after the caller read the
	// record, so the journal is read again.
	if current, exists, err := r.currentRecord(id); err == nil && exists && current.Continuation != nil {
		evidence = append(evidence, "continuation.md checkpoint kept")
	}
	detail := state.Detail
	if detail == "" {
		detail = "T3 recorded no detail"
	}
	reason := truncateText(fmt.Sprintf("%s (%s) after %d of %d in-session resumes: %s; %s",
		ProviderErrorFailure, state.Kind, state.Resumes, state.Budget, detail, strings.Join(evidence, "; ")), 2048)
	r.log.Warn("provider errors outlasted every in-session resume; the attempt fails as infrastructure",
		"assignment", id, "thread", pkg.Identity.ThreadID, "kind", string(state.Kind), "resumes", state.Resumes)
	if err := r.markFailed(ctx, id, reason); err != nil {
		return err
	}
	return r.collect(ctx, id)
}

func (r *Runtime) updateProviderResume(id string, change func(*ProviderResumeRecord)) error {
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok {
			return nil
		}
		var record ProviderResumeRecord
		if current.ProviderResume != nil {
			record = *current.ProviderResume
		}
		change(&record)
		current.ProviderResume = &record
		current.UpdatedAt = r.now()
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}

// providerResumeText is the message the resumed turn starts with.
func providerResumeText(state ProviderResumeRecord) string {
	return fmt.Sprintf("Your previous turn was ended by the model provider (%s), not by anything you did. "+
		"This is the same session and the same workspace. Read continuation.md, check the state of the workspace, "+
		"and continue the task from where it stopped. Keep continuation.md current as you work. "+
		"This is automatic resume %d of %d.", state.Kind, state.Resumes, state.Budget)
}

// cloneProviderResumePolicy owns the statement and its closed-pool slice;
// request storage may be reused after the exchange returns.
func cloneProviderResumePolicy(policy *workerproto.ProviderResumePolicy) *workerproto.ProviderResumePolicy {
	if policy == nil {
		return nil
	}
	copy := *policy
	copy.ClosedPools = append([]workerproto.ClosedQuotaPool(nil), policy.ClosedPools...)
	return &copy
}
