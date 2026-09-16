package domain

import (
	"sort"
	"strings"
)

// SupervisionBlockerCode is a stable machine-readable reason a supervised task
// is not admitted. The codes are part of the frozen interface: the planner
// renders them in explain output, the store returns them in refusals and the
// CLI distinguishes them, so they are never reworded in place.
type SupervisionBlockerCode string

const (
	// SupervisionBlockerGatePending means a gate protecting the task has not
	// collected its evidence yet.
	SupervisionBlockerGatePending SupervisionBlockerCode = "supervision-gate-pending"
	// SupervisionBlockerGateAwaitingReview means the evidence is ready and a
	// decision has not been made.
	SupervisionBlockerGateAwaitingReview SupervisionBlockerCode = "supervision-gate-awaiting-review"
	// SupervisionBlockerGateHeld means a gate was rejected with corrections.
	SupervisionBlockerGateHeld SupervisionBlockerCode = "supervision-gate-held"
	// SupervisionBlockerGateEscalated means a gate is waiting for a human.
	SupervisionBlockerGateEscalated SupervisionBlockerCode = "supervision-gate-escalated"
	// SupervisionBlockerGateCancelled means a gate was cancelled with its run.
	SupervisionBlockerGateCancelled SupervisionBlockerCode = "supervision-gate-cancelled"
	// SupervisionBlockerBranchHold means an active branch hold covers the task.
	SupervisionBlockerBranchHold SupervisionBlockerCode = "supervision-branch-hold"
	// SupervisionBlockerRunHold means an active run-wide hold covers the task.
	SupervisionBlockerRunHold SupervisionBlockerCode = "supervision-run-hold"
	// SupervisionBlockerRouteUnavailable means the supervisor route cannot run,
	// so a gate the task waits on cannot be decided. It is reported only for a
	// task actually waiting on a decision: an unavailable supervisor route does
	// not block unrelated permitted branches.
	SupervisionBlockerRouteUnavailable SupervisionBlockerCode = "supervision-route-unavailable"
	// SupervisionBlockerRunCancelled means the run was cancelled, which revokes
	// supervision authority and prevents new offers.
	SupervisionBlockerRunCancelled SupervisionBlockerCode = "supervision-run-cancelled"
	// SupervisionBlockerRunTerminal means the run settled; no further worker
	// task is admitted.
	SupervisionBlockerRunTerminal SupervisionBlockerCode = "supervision-run-terminal"
	// SupervisionBlockerStaleSnapshot means the caller's expected graph
	// revision is not the one this snapshot describes, so no answer given from
	// it would be about the graph the caller has.
	SupervisionBlockerStaleSnapshot SupervisionBlockerCode = "supervision-stale-snapshot"
)

// SupervisionBlocker is one reason with the identity behind it, so an operator
// reading explain output learns which gate or which hold, not merely that
// supervision said no.
type SupervisionBlocker struct {
	Code   SupervisionBlockerCode `json:"code"`
	TaskID string                 `json:"taskId,omitempty"`
	GateID string                 `json:"gateId,omitempty"`
	HoldID string                 `json:"holdId,omitempty"`
	Reason string                 `json:"reason,omitempty"`
}

// SupervisionSnapshot is the compact supervision state the predicate consumes.
// It is plain values and carries no store handle, so the planner can build it
// from its plan input and the store can build it inside the transaction it is
// about to commit.
type SupervisionSnapshot struct {
	RunID string `json:"runId"`
	// GraphRevision is the revision the gates and holds below were resolved
	// against.
	GraphRevision int64 `json:"graphRevision"`
	// Supervised is false for every run without a supervision record, which is
	// every run that exists today. A false value admits everything.
	Supervised   bool `json:"supervised"`
	RunTerminal  bool `json:"runTerminal,omitempty"`
	RunCancelled bool `json:"runCancelled,omitempty"`
	// RouteAvailable reports that the supervisor route can currently be
	// admitted. An unavailable route leaves gates closed rather than switching
	// to a weaker model.
	RouteAvailable bool   `json:"routeAvailable"`
	Gates          []Gate `json:"gates,omitempty"`
	Holds          []Hold `json:"holds,omitempty"`
}

// SupervisionQuery is the task the predicate is asked about.
type SupervisionQuery struct {
	TaskID string `json:"taskId"`
	// ExpectedGraphRevision fences the answer against the snapshot. Zero means
	// the caller is not fencing, which is correct for a planner reading and
	// deciding within one pass, and wrong for a store committing an offer.
	ExpectedGraphRevision int64 `json:"expectedGraphRevision,omitempty"`
}

// SupervisionVerdict is admit-or-blockers. Blockers are ordered
// deterministically so two callers rendering the same state render it the same
// way.
type SupervisionVerdict struct {
	Admitted bool                 `json:"admitted"`
	Blockers []SupervisionBlocker `json:"blockers,omitempty"`
}

// SupervisionAdmits reports whether supervision permits this task to be
// dispatched, and why not when it does not.
//
// This is the single predicate used at candidate selection, at offer creation
// and at claim and start authorization. The planner calls it to explain, the
// store calls it inside the offer transaction and inside the claim transaction
// to refuse, and admin start must call it too: a manual start bypasses quota by
// design, and it must not thereby bypass a gate.
//
// It is pure. It performs no I/O, reads no clock and mutates nothing, so the
// three callers get the same answer from the same state, and the store's copy
// and the planner's copy cannot drift apart the way dependency reasoning did.
//
// It answers about supervision only. Ordinary dependencies, resource locks,
// worker eligibility, effect safety and quota admission are separate
// conjuncts that each caller still evaluates; an admitted verdict here is
// permission from supervision, not readiness.
func SupervisionAdmits(snapshot SupervisionSnapshot, query SupervisionQuery) SupervisionVerdict {
	if !snapshot.Supervised {
		return SupervisionVerdict{Admitted: true}
	}
	var blockers []SupervisionBlocker
	add := func(blocker SupervisionBlocker) {
		blocker.TaskID = query.TaskID
		blockers = append(blockers, blocker)
	}

	if query.ExpectedGraphRevision != 0 && query.ExpectedGraphRevision != snapshot.GraphRevision {
		add(SupervisionBlocker{
			Code:   SupervisionBlockerStaleSnapshot,
			Reason: "the supervision snapshot is not at the expected graph revision",
		})
		// A stale snapshot cannot be used to report anything else honestly.
		return SupervisionVerdict{Blockers: blockers}
	}
	if snapshot.RunCancelled {
		add(SupervisionBlocker{
			Code:   SupervisionBlockerRunCancelled,
			Reason: "the run is cancelled; supervision authority is revoked",
		})
	}
	if snapshot.RunTerminal {
		add(SupervisionBlocker{
			Code:   SupervisionBlockerRunTerminal,
			Reason: "the run has settled",
		})
	}

	for _, hold := range snapshot.Holds {
		if !hold.Covers(query.TaskID) {
			continue
		}
		code := SupervisionBlockerBranchHold
		if hold.Scope.Kind == HoldScopeRun {
			code = SupervisionBlockerRunHold
		}
		add(SupervisionBlocker{Code: code, HoldID: hold.ID, Reason: hold.Reason})
	}

	awaitingDecision := false
	for _, gate := range snapshot.Gates {
		if !gate.Definition.Protects(query.TaskID) {
			continue
		}
		var code SupervisionBlockerCode
		switch gate.State {
		case GateAccepted:
			continue
		case GateReadyForReview:
			code = SupervisionBlockerGateAwaitingReview
			awaitingDecision = true
		case GateHeld:
			code = SupervisionBlockerGateHeld
		case GateEscalated:
			code = SupervisionBlockerGateEscalated
		case GateCancelled:
			code = SupervisionBlockerGateCancelled
		default:
			// An empty state is pending-evidence: a gate is closed before any
			// producer runs.
			code = SupervisionBlockerGatePending
			awaitingDecision = true
		}
		add(SupervisionBlocker{Code: code, GateID: gate.Definition.ID, Reason: string(gate.State)})
	}
	if awaitingDecision && !snapshot.RouteAvailable {
		add(SupervisionBlocker{
			Code:   SupervisionBlockerRouteUnavailable,
			Reason: "the supervisor route cannot be admitted, so the gate stays closed",
		})
	}

	if len(blockers) == 0 {
		return SupervisionVerdict{Admitted: true}
	}
	sortSupervisionBlockers(blockers)
	return SupervisionVerdict{Blockers: blockers}
}

func sortSupervisionBlockers(blockers []SupervisionBlocker) {
	sort.SliceStable(blockers, func(i, j int) bool {
		if blockers[i].Code != blockers[j].Code {
			return blockers[i].Code < blockers[j].Code
		}
		if blockers[i].GateID != blockers[j].GateID {
			return blockers[i].GateID < blockers[j].GateID
		}
		return blockers[i].HoldID < blockers[j].HoldID
	})
}

// SupervisedRunStatus is a derived label, never a stored state. Nothing in the
// scheduler branches on it: Terminal, RunExecutionsQuiescent, the planner and
// the sink projection are unchanged by supervision, and this label exists so a
// person reading status is told which of several true things matters most.
type SupervisedRunStatus string

const (
	SupervisedRunActive          SupervisedRunStatus = "active"
	SupervisedRunWaitingExternal SupervisedRunStatus = "waiting-external"
	SupervisedRunRunnable        SupervisedRunStatus = "runnable"
	SupervisedRunEscalated       SupervisedRunStatus = "escalated"
	SupervisedRunHeld            SupervisedRunStatus = "held"
	SupervisedRunBlocked         SupervisedRunStatus = "blocked"
	SupervisedRunTerminal        SupervisedRunStatus = "terminal"
)

// SupervisedRunStatusInput is the state the precedence function reads.
type SupervisedRunStatusInput struct {
	Snapshot SupervisionSnapshot
	// Attempts are the run's current attempts.
	Attempts []Attempt
	// ReadyTaskIDs are the tasks whose ordinary dependencies are satisfied.
	// Supervision is applied to them here rather than by the caller, so "a
	// runnable task" means one this predicate would admit.
	ReadyTaskIDs []string
	Incidents    []ReviewIncident
	// TasksRemaining reports that the run still has non-terminal tasks.
	TasksRemaining bool
	// SinkSettled reports terminal sink settlement.
	SinkSettled bool
}

// SupervisedRunStatusLabel applies the status precedence of the seam review,
// section 3.5. Highest rank wins:
//
//  1. active: some attempt has a live turn that is not parked
//  2. waiting-external: some attempt is parked on a task-bound wait
//  3. runnable: some ready task is admitted by SupervisionAdmits
//  4. escalated: an open escalated incident, or an attempt in needs-input
//  5. held: a gate awaiting review or held, or an active hold over a ready task
//  6. blocked: tasks remain and nothing above applies
//  7. terminal: the sink settled
//
// Ranks 4 and 5 are ordered so an escalation outranks a hold: an escalation
// needs a human, a hold needs only a decision.
func SupervisedRunStatusLabel(in SupervisedRunStatusInput) SupervisedRunStatus {
	parked := false
	needsInput := false
	for _, attempt := range in.Attempts {
		if !attempt.TurnLive() {
			if attempt.Progress == ProgressNeedsInput {
				needsInput = true
			}
			continue
		}
		if attempt.Progress == ProgressWaitingExternal || attempt.Control == ControlWaitingExternal {
			parked = true
			continue
		}
		return SupervisedRunActive
	}
	if parked {
		return SupervisedRunWaitingExternal
	}
	for _, taskID := range in.ReadyTaskIDs {
		if SupervisionAdmits(in.Snapshot, SupervisionQuery{TaskID: taskID}).Admitted {
			return SupervisedRunRunnable
		}
	}
	for _, incident := range in.Incidents {
		if incident.State == IncidentEscalated {
			return SupervisedRunEscalated
		}
	}
	if needsInput {
		return SupervisedRunEscalated
	}
	for _, gate := range in.Snapshot.Gates {
		if gate.State == GateReadyForReview || gate.State == GateHeld {
			return SupervisedRunHeld
		}
	}
	for _, taskID := range in.ReadyTaskIDs {
		for _, hold := range in.Snapshot.Holds {
			if hold.Covers(taskID) {
				return SupervisedRunHeld
			}
		}
	}
	if in.TasksRemaining {
		return SupervisedRunBlocked
	}
	if in.SinkSettled {
		return SupervisedRunTerminal
	}
	return SupervisedRunBlocked
}

// GateChangeKind is the kind of change an acceptance is tested against.
type GateChangeKind string

const (
	// GateChangeNewAttempt is a new attempt on a task: a retry or a
	// replacement of a producer.
	GateChangeNewAttempt GateChangeKind = "new-attempt"
	// GateChangeAmendment is a graph amendment naming the tasks it touched.
	GateChangeAmendment GateChangeKind = "amendment"
)

// GateChange is what happened after an acceptance was recorded.
type GateChange struct {
	Kind GateChangeKind `json:"kind"`
	// TaskID is the task a new attempt was created on.
	TaskID string `json:"taskId,omitempty"`
	// AmendedTaskIDs are the tasks an amendment touched.
	AmendedTaskIDs []string `json:"amendedTaskIds,omitempty"`
	// RevalidationReceiptID is the audit receipt written by an atomic
	// revalidation. Without it, an amendment does not carry an acceptance
	// forward.
	RevalidationReceiptID string `json:"revalidationReceiptId,omitempty"`
}

// AcceptanceInvalidation is why an acceptance does or does not stand.
type AcceptanceInvalidation string

const (
	AcceptanceStandsUnaffected       AcceptanceInvalidation = "stands-unaffected"
	AcceptanceStandsRevalidated      AcceptanceInvalidation = "stands-revalidated"
	AcceptanceInvalidNotAnAcceptance AcceptanceInvalidation = "not-an-acceptance"
	AcceptanceInvalidObservedRetried AcceptanceInvalidation = "observed-task-retried"
	AcceptanceInvalidScopeAmended    AcceptanceInvalidation = "amendment-touched-gate-scope"
	AcceptanceInvalidNoRevalidation  AcceptanceInvalidation = "amendment-without-revalidation-receipt"
)

// AcceptanceVerdict is whether the acceptance stands, and the reason either
// way.
type AcceptanceVerdict struct {
	Stands bool                   `json:"stands"`
	Reason AcceptanceInvalidation `json:"reason"`
}

// GateAcceptanceStands reports whether a recorded gate acceptance survives a
// change.
//
// Conservative invalidation is the default, deliberately. A false invalidation
// costs one review; a false carry-forward is an unreviewed dispatch. So a new
// attempt on any observed task invalidates, an amendment touching the gate's
// observed or protected sets invalidates unconditionally, and an amendment
// touching neither carries the acceptance forward only when an atomic
// revalidation wrote an audit receipt.
//
// This function says whether the acceptance stands. It does not say whether the
// invalidation may be applied: an invalidation whose protected task has already
// been offered or started is refused by GateTransition, because v1 does not
// pretend a started effect can be undone.
func GateAcceptanceStands(gate GateDefinition, acceptance GateDecision, change GateChange) AcceptanceVerdict {
	if acceptance.Outcome != GateDecisionAccept {
		return AcceptanceVerdict{Reason: AcceptanceInvalidNotAnAcceptance}
	}
	switch change.Kind {
	case GateChangeNewAttempt:
		if gate.Observes(change.TaskID) {
			return AcceptanceVerdict{Reason: AcceptanceInvalidObservedRetried}
		}
		return AcceptanceVerdict{Stands: true, Reason: AcceptanceStandsUnaffected}
	case GateChangeAmendment:
		for _, taskID := range change.AmendedTaskIDs {
			if gate.Observes(taskID) || gate.Protects(taskID) {
				return AcceptanceVerdict{Reason: AcceptanceInvalidScopeAmended}
			}
		}
		if strings.TrimSpace(change.RevalidationReceiptID) == "" {
			return AcceptanceVerdict{Reason: AcceptanceInvalidNoRevalidation}
		}
		return AcceptanceVerdict{Stands: true, Reason: AcceptanceStandsRevalidated}
	}
	return AcceptanceVerdict{Reason: AcceptanceInvalidNoRevalidation}
}
