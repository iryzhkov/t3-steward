package backlogadmin

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// PlanningSnapshotHolder publishes only the last successful planning pass.
// Published slices and maps are private immutable copies, shared by all readers.
type PlanningSnapshotHolder struct {
	mu       sync.RWMutex
	interval time.Duration
	snapshot planningSnapshot
}
type planningSnapshot struct {
	at        time.Time
	interval  time.Duration
	decisions []backlog.TaskPlanningDecision
	byAttempt map[string]int
	revisions map[string]int64
}

func NewPlanningSnapshotHolder(interval time.Duration) *PlanningSnapshotHolder {
	if interval <= 0 {
		interval = time.Minute
	}
	return &PlanningSnapshotHolder{interval: interval}
}
func (h *PlanningSnapshotHolder) Record(plan backlog.Plan, attempts []domain.Attempt, at time.Time) {
	if h == nil {
		return
	}
	next := planningSnapshot{at: at, interval: h.interval, byAttempt: make(map[string]int), revisions: make(map[string]int64)}
	for _, a := range attempts {
		next.revisions[a.ID] = a.Revision
	}
	for _, d := range plan.Decisions {
		copyDecision := backlog.TaskPlanningDecision{WorkflowRunID: d.WorkflowRunID, TaskID: d.TaskID, AttemptID: d.AttemptID, Progress: d.Progress, Proposed: d.Proposed}
		copyDecision.Blockers = clonePlanningBlockers(d.Blockers)
		for _, c := range d.Candidates {
			copyDecision.Candidates = append(copyDecision.Candidates, backlog.CandidateEvaluation{WorkerID: c.WorkerID, Blockers: clonePlanningBlockers(c.Blockers)})
		}
		next.byAttempt[d.AttemptID] = len(next.decisions)
		next.decisions = append(next.decisions, copyDecision)
	}
	h.mu.Lock()
	h.snapshot = next
	h.mu.Unlock()
}
func clonePlanningBlockers(in []backlog.PlanningBlocker) []backlog.PlanningBlocker {
	out := append([]backlog.PlanningBlocker(nil), in...)
	for i := range out {
		if in[i].EarliestAt != nil {
			at := *in[i].EarliestAt
			out[i].EarliestAt = &at
		}
	}
	return out
}
func (h *PlanningSnapshotHolder) load() planningSnapshot {
	if h == nil {
		return planningSnapshot{}
	}
	h.mu.RLock()
	snapshot := h.snapshot
	h.mu.RUnlock()
	return snapshot
}

// SetPlanningSnapshotHolder is configured before the service starts serving.
func (s *Service) SetPlanningSnapshotHolder(holder *PlanningSnapshotHolder) { s.planning = holder }

func (v view) addPlanningExplanation(e *Explanation, attempt *domain.Attempt) {
	// Assigned attempts retain their existing execution/control explanation.
	if attempt != nil && attempt.Control != domain.ControlUnassigned {
		return
	}
	if v.planning.at.IsZero() {
		e.Eligible = false
		if len(e.Blockers) == 0 {
			e.Summary = "no planning pass yet since coordinator start; eligibility is computed from records only"
		} else {
			e.Summary += "; no planning pass yet since coordinator start"
		}
		return
	}
	age := v.now.Sub(v.planning.at)
	if age < 0 {
		age = 0
	}
	index, found := v.planning.byAttempt[e.AttemptID]
	if !found || attempt == nil {
		e.Eligible = false
		e.Summary = fmt.Sprintf("waiting: task was not evaluated in the last planning pass (%ds ago)", int(age.Seconds()))
		return
	}
	decision := v.planning.decisions[index]
	revision, hasRevision := v.planning.revisions[attempt.ID]
	// Saturate the bound: a valid scheduling interval can exceed MaxDuration/3.
	// Every representable age is fresh when the mathematical bound exceeds it.
	maxAge := time.Duration(1<<63 - 1)
	if v.planning.interval <= maxAge/3 {
		maxAge = 3 * v.planning.interval
	}
	stale := age > maxAge || !hasRevision || revision != attempt.Revision ||
		decision.WorkflowRunID != e.WorkflowRunID || decision.TaskID != e.TaskID
	if stale {
		e.Eligible = false
		e.Summary = fmt.Sprintf("waiting: last planning decision is stale (%ds old)", int(age.Seconds()))
	}
	// Candidate refusals are relevant only when no candidate won.
	if !decision.Proposed {
		mapped := map[string]Blocker{}
		add := func(b backlog.PlanningBlocker, worker string) {
			if b.WorkerID != "" {
				worker = b.WorkerID
			}
			key := worker + "\x00" + b.Code + "\x00" + b.QuotaPoolID
			at := b.EarliestAt
			if at != nil {
				copyAt := *at
				at = &copyAt
			}
			converted := Blocker{Code: b.Code, Detail: b.Detail, WorkerID: worker, QuotaPoolID: b.QuotaPoolID, EarliestAt: at, DependsOn: b.DependsOn, Resource: b.Resource, OwnerID: b.OwnerID, GateID: b.GateID, HoldID: b.HoldID, SupervisionCode: b.SupervisionCode}
			if prev, ok := mapped[key]; !ok || planningBlockerLess(converted, prev) {
				mapped[key] = converted
			}
		}
		for _, b := range decision.Blockers {
			add(b, "")
		}
		for _, c := range decision.Candidates {
			for _, b := range c.Blockers {
				add(b, c.WorkerID)
			}
		}
		keys := make([]string, 0, len(mapped))
		for key := range mapped {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		const limit = 10
		for i, key := range keys {
			if i >= limit {
				break
			}
			b := mapped[key]
			duplicate := false
			for _, old := range e.Blockers {
				if old.Code == b.Code && old.WorkerID == b.WorkerID && old.QuotaPoolID == b.QuotaPoolID && old.Detail == b.Detail {
					duplicate = true
					break
				}
			}
			if !duplicate {
				e.Blockers = append(e.Blockers, b)
			}
		}
		if len(keys) > limit {
			e.Details = append(e.Details, fmt.Sprintf("and %d more planning blockers", len(keys)-limit))
		}
	}
	if !stale {
		e.Eligible = e.Eligible && decision.Proposed
		if !decision.Proposed {
			e.Summary = fmt.Sprintf("waiting: not assigned in the last planning pass (%ds ago)", int(age.Seconds()))
		}
	}
	if detail := v.planningQueueDetail(e.AttemptID); detail != "" {
		e.Details = append(e.Details, detail)
	}
	sort.SliceStable(e.Blockers, func(i, j int) bool {
		a, b := e.Blockers[i], e.Blockers[j]
		return a.Code+"\x00"+a.WorkerID+"\x00"+a.QuotaPoolID+"\x00"+a.Detail < b.Code+"\x00"+b.WorkerID+"\x00"+b.QuotaPoolID+"\x00"+b.Detail
	})
}

// runCapacityDeadlocks reads the capacity-deadlock blockers the last planning
// pass recorded for a run's tasks. A stale pass reports none: the cycle it saw
// may already be broken.
func (v view) runCapacityDeadlocks(runID string) []CapacityDeadlock {
	if v.planning.at.IsZero() {
		return nil
	}
	maxAge := time.Duration(1<<63 - 1)
	if v.planning.interval <= maxAge/3 {
		maxAge = 3 * v.planning.interval
	}
	if v.now.Sub(v.planning.at) > maxAge {
		return nil
	}
	var deadlocks []CapacityDeadlock
	for _, decision := range v.planning.decisions {
		if decision.WorkflowRunID != runID {
			continue
		}
		for _, blocker := range decision.Blockers {
			if blocker.Code != backlog.PlanningBlockerCapacityDeadlock {
				continue
			}
			deadlocks = append(deadlocks, CapacityDeadlock{
				TaskID: decision.TaskID, TaskName: v.tasks[decision.TaskID].Name, AttemptID: decision.AttemptID,
				HolderID: blocker.OwnerID, Detail: blocker.Detail,
			})
		}
	}
	return deadlocks
}

// Choose one stable representative when routes repeat the same worker/code/pool.
// Prefer the earliest known retry time when their verdict text is identical.
func planningBlockerLess(a, b Blocker) bool {
	if a.Detail != b.Detail {
		return a.Detail < b.Detail
	}
	if a.EarliestAt == nil || b.EarliestAt == nil {
		if a.EarliestAt != b.EarliestAt {
			return a.EarliestAt != nil
		}
	} else if !a.EarliestAt.Equal(*b.EarliestAt) {
		return a.EarliestAt.Before(*b.EarliestAt)
	}
	return strings.Join([]string{a.DependsOn, a.Resource, a.OwnerID, a.GateID, a.HoldID, string(a.SupervisionCode)}, "\x00") <
		strings.Join([]string{b.DependsOn, b.Resource, b.OwnerID, b.GateID, b.HoldID, string(b.SupervisionCode)}, "\x00")
}

func (v view) planningQueueDetail(attemptID string) string {
	position, total := 0, 0
	counts := map[domain.TaskClass]int{}
	var runs []string
	seenRuns := map[string]bool{}
	for _, d := range v.planning.decisions {
		if d.Progress != domain.ProgressReady {
			continue
		}
		current := latestAttempt(v.attempts[d.WorkflowRunID+"\x00"+d.TaskID])
		if current == nil || current.ID != d.AttemptID || current.Progress.Terminal() {
			continue
		}
		total++
		if d.AttemptID == attemptID {
			position = total
		}
		if position == 0 && d.AttemptID != attemptID {
			class := v.tasks[d.TaskID].Class
			if class == "" {
				class = v.workflows[v.runs[d.WorkflowRunID].WorkflowID].Class
			}
			if class == "" {
				class = domain.TaskClassRequired
			}
			counts[class]++
			if !seenRuns[d.WorkflowRunID] && len(runs) < 3 {
				runs = append(runs, d.WorkflowRunID)
				seenRuns[d.WorkflowRunID] = true
			}
		}
	}
	if position == 0 {
		return ""
	}
	var ahead []string
	for _, class := range []domain.TaskClass{domain.TaskClassRequired, domain.TaskClassSurplus} {
		if counts[class] > 0 {
			ahead = append(ahead, fmt.Sprintf("%d %s tasks", counts[class], class))
		}
	}
	if len(ahead) == 0 {
		ahead = append(ahead, "none")
	}
	detail := fmt.Sprintf("queue position %d of %d ready tasks; ahead: %s", position, total, strings.Join(ahead, ", "))
	if len(runs) > 0 {
		detail += " (" + strings.Join(runs, ", ") + ")"
	}
	return detail
}
