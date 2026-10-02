package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A whole-run cancel closes the run's supervision in the transaction that
// cancels its tasks. Before it did, a run whose tasks were all terminal but
// whose review incident was escalated could not be cancelled at all: the cancel
// found no task to fence on, the incident kept the sink open, and the operator
// had to find the incident, its revision and the resolve verb by hand. What the
// cancel now does, in order, inside ApplyAdminCommand's transaction:
//
//  1. refuse while an overseer activation is live (liveRunActivationTx), since
//     resolving its incidents underneath it would race its decisions;
//  2. resolve every open or escalated incident as cancelled, cancel every gate
//     that is not accepted, and release every active hold
//     (closeRunSupervisionTx), each through the pure state machine's own
//     run-cancelled or release row;
//  3. settle the sink if the run is quiescent (settleCancelledRunSinkTx). A run
//     with a worker still stopping settles at the next projection instead,
//     which nothing in supervision can now hold back.

// liveRunActivationTx returns the run's live overseer activation, if it has
// one. Live is the rule every other lane uses: pending dispatch or active, at
// or above the record's current epoch. A row below the epoch was superseded.
func liveRunActivationTx(ctx context.Context, tx *sql.Tx, runID string) (domain.Activation, bool, error) {
	record, _, err := loadSupervisionRecordTx(ctx, tx, runID)
	if err != nil {
		return domain.Activation{}, false, err
	}
	activations, err := loadSupervisionActivationsTx(ctx, tx, runID)
	if err != nil {
		return domain.Activation{}, false, err
	}
	for _, activation := range activations {
		if otherActivationValid(activation, record.ActivationEpoch) {
			return activation, true, nil
		}
	}
	return domain.Activation{}, false, nil
}

// RunCancelActivationRefusal is the failure a whole-run cancel records while
// an overseer activation is live. It names the activation and the two commands
// the operator needs: one to watch it end, and the cancel to send again.
func RunCancelActivationRefusal(runID string, activation domain.Activation) string {
	lease := ""
	if activation.LeaseExpiresAt != nil {
		lease = ", lease until " + activation.LeaseExpiresAt.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("run %s has a live overseer activation %s (epoch %d, %s%s); "+
		"a whole-run cancel would resolve its incidents while it is deciding them, so it was not applied. "+
		"Wait until the activation is no longer pending-dispatch or active: t3-steward campaign supervision show %s; "+
		"then send the cancel again: t3-steward campaign cancel %s --reason TEXT",
		runID, activation.ID, activation.Epoch, activation.State, lease, runID, runID)
}

// runSupervisionClosure is what closing a run's supervision changed, recorded
// as the detail of its audit event.
type runSupervisionClosure struct {
	CommandID         string   `json:"commandId"`
	ResolvedIncidents []string `json:"resolvedIncidents,omitempty"`
	CancelledGates    []string `json:"cancelledGates,omitempty"`
	ReleasedHolds     []string `json:"releasedHolds,omitempty"`
	SupervisionRev    int64    `json:"supervisionRevision,omitempty"`
	SinkSettled       bool     `json:"sinkSettled"`
}

func (c runSupervisionClosure) changed() bool {
	return len(c.ResolvedIncidents) != 0 || len(c.CancelledGates) != 0 || len(c.ReleasedHolds) != 0 || c.SinkSettled
}

// closeRunSupervisionTx resolves, cancels and releases everything open in the
// run's supervision on behalf of the operator who cancelled the run, and bumps
// the supervision revision so a decision read before the cancel is fenced out.
// An unsupervised run is left untouched.
func closeRunSupervisionTx(ctx context.Context, tx *sql.Tx, runID string, actor domain.Actor, commandID, reason string, now time.Time) (runSupervisionClosure, error) {
	closure := runSupervisionClosure{CommandID: commandID}
	state, supervised, err := supervisionContextTx(ctx, tx, runID, false)
	if err != nil || !supervised {
		return closure, err
	}
	incidents, err := loadSupervisionIncidentsTx(ctx, tx, runID)
	if err != nil {
		return closure, err
	}
	for _, incident := range incidents {
		if incident.State != domain.IncidentOpen && incident.State != domain.IncidentEscalated {
			continue
		}
		next, err := domain.IncidentTransition(domain.IncidentTransitionInput{
			Incident: incident, Event: domain.IncidentEventRunCancelled, Actor: actor, ExpectedRevision: incident.Revision,
		})
		if err != nil {
			return closure, fmt.Errorf("resolve incident %s with the run's cancel: %w", incident.ID, err)
		}
		incident.Resolution = &domain.ResolutionReceipt{
			Actor: actor, Outcome: domain.IncidentOutcomeCancelled, ExpectedRevision: incident.Revision,
			RequestID: commandID, Reason: reason, ResolvedAt: now,
		}
		incident.State = next
		incident.Revision++
		if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
			return closure, err
		}
		closure.ResolvedIncidents = append(closure.ResolvedIncidents, incident.ID)
	}
	// A gate that was not accepted is cancelled with its run. Left as it was, a
	// final gate would keep the sink of a run whose tasks all succeeded open
	// forever, and an advancing gate would raise a fresh review incident on a
	// run the operator has just closed.
	for _, gate := range state.Snapshot.Gates {
		if gate.State == domain.GateAccepted || gate.State == domain.GateCancelled {
			continue
		}
		next, err := domain.GateTransition(domain.GateTransitionInput{Gate: gate, Event: domain.GateEventRunCancelled, Actor: actor})
		if err != nil {
			return closure, fmt.Errorf("cancel gate %s with the run: %w", gate.Definition.ID, err)
		}
		gate.State = next
		gate.Revision++
		gate.UpdatedAt = now
		if err := saveSupervisionGateTx(ctx, tx, gate); err != nil {
			return closure, err
		}
		closure.CancelledGates = append(closure.CancelledGates, gate.Definition.ID)
	}
	for _, hold := range state.Snapshot.Holds {
		next, err := domain.HoldTransition(domain.HoldTransitionInput{Hold: hold, Event: domain.HoldEventRelease, Actor: actor})
		if err != nil {
			return closure, fmt.Errorf("release hold %s with the run's cancel: %w", hold.ID, err)
		}
		released := now
		hold.State, hold.ReleasedAt = next, &released
		if err := saveSupervisionHoldTx(ctx, tx, hold); err != nil {
			return closure, err
		}
		closure.ReleasedHolds = append(closure.ReleasedHolds, hold.ID)
	}
	record := state.Record
	record.RunID = runID
	record.Revision++
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	if err := saveSupervisionRecordTx(ctx, tx, record); err != nil {
		return closure, err
	}
	closure.SupervisionRev = record.Revision
	return closure, nil
}

// settleCancelledRunSinkTx settles the run's sink in the cancel's own
// transaction when nothing is left to wait for: every predecessor terminal,
// every execution quiescent and supervision no longer holding it. It reports
// false, and changes nothing, when one of those is not yet true; the ordinary
// projection settles the run once it is.
func settleCancelledRunSinkTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time) (bool, error) {
	snapshot, err := loadWorkflowProjectionTx(ctx, tx, runID)
	if err != nil {
		return false, err
	}
	if snapshot.Run.Sink == nil || snapshot.Run.Sink.Progress.Terminal() {
		return false, nil
	}
	barrier := domain.SupervisionBarrier{}
	if snapshot.Supervision != nil {
		barrier = domain.SupervisionBarrier{
			Supervised: true, Gates: snapshot.Supervision.Gates, Incidents: snapshot.Supervision.Incidents,
		}
	}
	settled, err := domain.ProjectSupervisedRunSink(snapshot.Run, snapshot.Tasks, snapshot.Attempts, snapshot.Assignments, barrier, now)
	if err != nil {
		return false, err
	}
	if settled.Sink == nil || !settled.Sink.Progress.Terminal() {
		return false, nil
	}
	settled.Revision = snapshot.Run.Revision + 1
	settled.UpdatedAt = now
	if err := updateAdminWorkflowRunTx(ctx, tx, settled); err != nil {
		return false, err
	}
	detail, err := json.Marshal(settled.Sink)
	if err != nil {
		return false, err
	}
	if _, err := insertAuditEventTx(ctx, tx, domain.AuditEvent{
		ID: "sink-settled:" + runID, WorkflowRunID: runID, TaskID: settled.Sink.ID,
		Kind: "sink-settled", Actor: "coordinator",
		Reason: "settled by a whole-run cancel: all sink predecessors are terminal and executions are quiescent",
		Detail: detail, CreatedAt: now,
	}); err != nil {
		return false, fmt.Errorf("record sink settlement: %w", err)
	}
	return true, nil
}

// releaseSettledRunHoldsTx releases every hold still active when a run's sink
// settles, through the hold machine's run-settled row. Settlement revokes every
// supervision capability, release included, so a hold left active here could
// never be released by anyone and was listed by triage forever as "still
// recorded active on a settled run". Holds of runs that settled before this
// existed are not rewritten; show and triage treat them as closed.
func releaseSettledRunHoldsTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time) error {
	holds, err := loadSupervisionHoldsTx(ctx, tx, runID, domain.HoldActive)
	if err != nil {
		return err
	}
	for _, hold := range holds {
		next, err := domain.HoldTransition(domain.HoldTransitionInput{Hold: hold, Event: domain.HoldEventRunSettled})
		if err != nil {
			return fmt.Errorf("release hold %s at settlement: %w", hold.ID, err)
		}
		released := now
		hold.State, hold.ReleasedAt = next, &released
		if err := saveSupervisionHoldTx(ctx, tx, hold); err != nil {
			return err
		}
	}
	return nil
}

// closeCancelledRunTx is steps 2 and 3 above plus the audit event that records
// them, called once the cancel itself has been written.
func closeCancelledRunTx(ctx context.Context, tx *sql.Tx, command domain.AdminCommand, runID string, now time.Time) error {
	actor := domain.Actor{Kind: domain.ActorOperator, Principal: command.RequestedBy}
	closure, err := closeRunSupervisionTx(ctx, tx, runID, actor, command.ID, command.Reason, now)
	if err != nil {
		return err
	}
	if closure.SinkSettled, err = settleCancelledRunSinkTx(ctx, tx, runID, now); err != nil {
		return err
	}
	if !closure.changed() {
		return nil
	}
	detail, err := json.Marshal(closure)
	if err != nil {
		return err
	}
	_, err = insertAuditEventTx(ctx, tx, domain.AuditEvent{
		ID: "run-supervision-closed:" + command.ID, Kind: "run-supervision-closed",
		WorkflowRunID: runID, TargetType: domain.AdminTargetWorkflowRun, TargetID: runID,
		Actor: command.RequestedBy, Reason: command.Reason, Detail: detail, CreatedAt: now,
	})
	if err != nil {
		return fmt.Errorf("record the supervision the cancel closed: %w", err)
	}
	return nil
}
