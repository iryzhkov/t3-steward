package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RetargetNodeWakes moves only run-backed completion and attention recipients.
// Destination validation belongs to the service. No transport exposes this yet.
func (s *Store) RetargetNodeWakes(ctx context.Context, request domain.NodeWakeRetargetRequest, now time.Time) (domain.NodeWakeRetargetResult, error) {
	var result domain.NodeWakeRetargetResult
	selectors := 0
	for _, present := range []bool{len(request.RunIDs) > 0, request.Project != "", request.SourceThreadID != ""} {
		if present {
			selectors++
		}
	}
	if selectors != 1 || (request.SourceThreadID != "") != (request.SourceHost != "") || now.IsZero() {
		return result, errors.New("retarget needs exactly one selector; thread selection needs its host")
	}
	for _, value := range []string{request.ThreadID, request.Host, request.Actor, request.Reason, request.Key} {
		if strings.TrimSpace(value) == "" {
			return result, errors.New("retarget needs destination, actor, reason and idempotency key")
		}
	}
	runs := map[string]bool{}
	for _, id := range request.RunIDs {
		if id == "" {
			return result, errors.New("empty retarget run ID")
		}
		runs[id] = true
	}
	request.RunIDs = slices.Sorted(maps.Keys(runs))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	receiptID := fmt.Sprintf("node-wake-retarget:%x", sha256.Sum256([]byte(request.Key)))
	event, found, err := loadAuditEventTx(ctx, tx, receiptID)
	if err != nil {
		return result, err
	}
	type receipt struct {
		Request domain.NodeWakeRetargetRequest `json:"request"`
		Result  domain.NodeWakeRetargetResult  `json:"result"`
	}
	if found {
		var prior receipt
		if err := json.Unmarshal(event.Detail, &prior); err != nil {
			return result, err
		}
		if !reflect.DeepEqual(prior.Request, request) {
			return result, errors.New("retarget key replay changed request")
		}
		return prior.Result, tx.Commit()
	}
	if request.Project != "" {
		workflows, err := loadJSON[domain.Workflow](ctx, tx, "coordinator_workflows")
		if err != nil {
			return result, err
		}
		runRows, err := loadJSON[domain.WorkflowRun](ctx, tx, "coordinator_workflow_runs")
		if err != nil {
			return result, err
		}
		for _, workflow := range workflows {
			if workflow.Project != request.Project {
				continue
			}
			for _, run := range runRows {
				if run.WorkflowID == workflow.ID {
					runs[run.ID] = true
				}
			}
		}
	}
	waits, err := loadJSON[domain.NodeWait](ctx, tx, "coordinator_node_waits")
	if err != nil {
		return result, err
	}
	changes := map[string][]domain.NodeWait{}
	for _, w := range waits {
		r := w.Request
		if r.Target.RunID == "" || r.Quota != nil || r.State == domain.NodeStateActive {
			continue
		}
		if request.SourceThreadID != "" {
			if r.ThreadID != request.SourceThreadID || w.Host != request.SourceHost {
				continue
			}
		} else if !runs[r.Target.RunID] {
			continue
		}
		switch w.Delivery {
		case "delivered", "rejected", "cancelled":
			result.Skipped = append(result.Skipped, r.ID)
			continue
		}
		if r.ThreadID == request.ThreadID && w.Host == request.Host {
			result.Unchanged = append(result.Unchanged, r.ID)
			continue
		}
		switch w.Delivery {
		case "pending", "held", "offline", "busy":
		default:
			return domain.NodeWakeRetargetResult{}, fmt.Errorf("retarget wait %q: delivery %s is uncertain", r.ID, w.Delivery)
		}
		if w.DeliveryPayload != "" || w.DeliveryPayloadDigest != "" || len(w.DeliveryGroupMembers) != 0 {
			return domain.NodeWakeRetargetResult{}, fmt.Errorf("retarget wait %q: frozen delivery", r.ID)
		}
		changes[r.Target.RunID] = append(changes[r.Target.RunID], w)
		result.Moved = append(result.Moved, r.ID)
	}
	slices.Sort(result.Moved)
	slices.Sort(result.Skipped)
	slices.Sort(result.Unchanged)
	for _, runID := range slices.Sorted(maps.Keys(changes)) {
		type move struct {
			WaitID, OldThread, OldHost, NewThread, NewHost string
			Generation                                     uint64
		}
		var moves []move
		for _, w := range changes[runID] {
			moves = append(moves, move{w.Request.ID, w.Request.ThreadID, w.Host, request.ThreadID, request.Host, w.RecipientGeneration + 1})
			if w.RegistrationHost == "" {
				w.RegistrationHost = w.Host
			}
			w.Request.ThreadID, w.Host = request.ThreadID, request.Host
			w.RecipientGeneration++
			if w.RecipientGeneration == 0 {
				return domain.NodeWakeRetargetResult{}, errors.New("recipient generation exhausted")
			}
			if err := saveNodeWaitTx(ctx, tx, w); err != nil {
				return domain.NodeWakeRetargetResult{}, err
			}
		}
		detail, err := json.Marshal(struct {
			Key   string
			Moves []move
		}{request.Key, moves})
		if err != nil {
			return domain.NodeWakeRetargetResult{}, err
		}
		if _, err := insertAuditEventTx(ctx, tx, domain.AuditEvent{ID: fmt.Sprintf("%s:run:%d:%s", receiptID, len(runID), runID), Kind: "node-wake-retarget-run", WorkflowRunID: runID, Actor: request.Actor, Reason: request.Reason, Detail: detail, CreatedAt: now.UTC()}); err != nil {
			return domain.NodeWakeRetargetResult{}, err
		}
	}
	detail, err := json.Marshal(receipt{request, result})
	if err != nil {
		return domain.NodeWakeRetargetResult{}, err
	}
	if _, err := insertAuditEventTx(ctx, tx, domain.AuditEvent{ID: receiptID, Kind: "node-wake-retarget-receipt", Actor: request.Actor, Reason: request.Reason, Detail: detail, CreatedAt: now.UTC()}); err != nil {
		return domain.NodeWakeRetargetResult{}, err
	}
	return result, tx.Commit()
}
