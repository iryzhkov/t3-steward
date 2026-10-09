package domain

import (
	"errors"
	"strings"
	"time"
)

// RunLineage records the attempt a workflow run was submitted from. A run
// submitted by `task run` or `campaign submit` from inside a running or parked
// Steward task carries it; a run submitted by an operator or a schedule has
// none. The coordinator fills it from the submitting task's identity after
// checking that the attempt exists and still has a live turn, so a client can
// name its parent but never invent one.
//
// Lineage is what lets the planner order nested work ahead of root work that
// arrived after the parent started: a parent that parks on its child holds
// nothing, but a child that queues behind newer campaigns keeps the parent
// parked, and the parent's workspace with it.
type RunLineage struct {
	ParentRunID     string `json:"parentRunId"`
	ParentTaskID    string `json:"parentTaskId"`
	ParentAttemptID string `json:"parentAttemptId"`
	// ParentStartedAt is when the parent attempt was first assigned. Planning
	// treats the child as ready no later than this, so it is ordered ahead of
	// root work that became ready after its parent started.
	ParentStartedAt time.Time `json:"parentStartedAt"`
	RecordedAt      time.Time `json:"recordedAt"`
}

// Validate checks that the lineage names one attempt completely.
func (l RunLineage) Validate() error {
	for _, value := range []string{l.ParentRunID, l.ParentTaskID, l.ParentAttemptID} {
		if value == "" || strings.TrimSpace(value) != value {
			return errors.New("run lineage must name the parent run, task and attempt")
		}
	}
	if l.ParentStartedAt.IsZero() || l.RecordedAt.IsZero() {
		return errors.New("run lineage needs the parent start and the recording time")
	}
	return nil
}

// SubmissionParent is what a client inside a Steward task says about the task
// it is running in when it submits work. It is a claim: the coordinator checks
// it against its own attempt records before it records any lineage.
type SubmissionParent struct {
	RunID     string `json:"runId"`
	TaskID    string `json:"taskId"`
	AttemptID string `json:"attemptId"`
}

// Validate checks that the claim names one attempt completely.
func (p SubmissionParent) Validate() error {
	for _, value := range []string{p.RunID, p.TaskID, p.AttemptID} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
			return errors.New("submission parent must name a run, task and attempt")
		}
	}
	return nil
}

// CloneRunLineage returns an independent copy of an optional lineage.
func CloneRunLineage(lineage *RunLineage) *RunLineage {
	if lineage == nil {
		return nil
	}
	copied := *lineage
	return &copied
}

// Wake deferral codes say why a settled task-bound wait has not resumed its
// attempt yet. A deferred wake is never failed: it is retried on every
// coordinator boundary, ahead of ordinary work that became ready after it
// settled, and the code is replaced or cleared as the cause changes.
const (
	// WakeDeferredExecutorCapacity: the parked attempt's own worker has no
	// executor slot, CPU, memory or scratch left for the resumed turn.
	WakeDeferredExecutorCapacity = "wake-executor-capacity"
	// WakeDeferredPoolConcurrency: the attempt's quota pool is at its
	// concurrency limit.
	WakeDeferredPoolConcurrency = "wake-pool-concurrency"
	// WakeDeferredQuotaAdmission: the attempt's quota pool is not open, or its
	// admission evidence is stale.
	WakeDeferredQuotaAdmission = "wake-quota-admission"
	// WakeDeferredWorkerUnavailable: the worker holding the parked workspace
	// has no current authorized snapshot (it is disconnected, stale, or no
	// longer offers the route or project). The wake waits for the same worker
	// to return; a worker that returns under a new epoch has lost the
	// execution, and the wake is then abandoned and the attempt failed through
	// the ordinary abandoned-execution path.
	WakeDeferredWorkerUnavailable = "wake-worker-unavailable"
	// WakeDeferredOlderWork: older ordinary work contends for the same worker
	// or pool and is served first.
	WakeDeferredOlderWork = "wake-yields-to-older-work"
)

// TaskWaitWakeDeferral is the coordinator's statement of why a settled wait's
// wake has not been applied. It is written only when the cause changes.
type TaskWaitWakeDeferral struct {
	Code       string    `json:"code"`
	Detail     string    `json:"detail"`
	WorkerID   string    `json:"workerId,omitempty"`
	PoolID     string    `json:"poolId,omitempty"`
	ObservedAt time.Time `json:"observedAt"`
}

// RouteReresolution records that the planner moved an accepted, never-started
// role task from its resolved route to another eligible candidate of the same
// role because the resolved route's quota pool was at its concurrency limit.
// It is written into the placement decision of the assignment it produced.
// Explicit pins are never re-resolved.
type RouteReresolution struct {
	Role      string    `json:"role"`
	FromRoute string    `json:"fromRoute"`
	FromPool  string    `json:"fromPool"`
	ToRoute   string    `json:"toRoute"`
	ToPool    string    `json:"toPool"`
	Effort    string    `json:"effort"`
	Ranking   string    `json:"ranking,omitempty"`
	Reason    string    `json:"reason"`
	DecidedAt time.Time `json:"decidedAt"`
}

// CloneRouteReresolution returns an independent copy of an optional receipt.
func CloneRouteReresolution(value *RouteReresolution) *RouteReresolution {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
