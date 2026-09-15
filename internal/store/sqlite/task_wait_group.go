package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A claim proves that the sender committed to a payload containing this exact
// group. Message identity alone cannot prove that after an older per-wait
// sender has touched a shared ID.
func taskWakeGroupClaim(members []domain.TaskWait, now time.Time) domain.TaskWaitReconciliation {
	ids := make([]string, 0, len(members))
	for _, member := range members {
		ids = append(ids, member.ID)
	}
	sort.Strings(ids)
	raw, _ := json.Marshal(ids)
	first := members[0]
	return domain.TaskWaitReconciliation{
		ID: "group-delivery-claim:" + first.DeliveryID, Kind: "group-delivery-claim",
		AttemptID: first.AttemptID, ThreadID: first.ThreadID, WaitID: ids[0],
		Detail: fmt.Sprintf("%d:%s", first.WakeRevision, raw), ObservedAt: now.UTC(),
	}
}

func taskWakeGroupClaimedTx(ctx context.Context, tx *sql.Tx, members []domain.TaskWait) (bool, error) {
	expected := taskWakeGroupClaim(members, time.Time{})
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_task_wait_events WHERE id=?", expected.ID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var event domain.TaskWaitReconciliation
	if err := json.Unmarshal(raw, &event); err != nil {
		return false, err
	}
	return event.Kind == expected.Kind && event.AttemptID == expected.AttemptID &&
		event.ThreadID == expected.ThreadID && event.Detail == expected.Detail, nil
}

// Older binaries transition shared-ID rows individually and render only one
// result. Without a group claim, even uniform delivered rows may only prove
// that one outcome reached the thread. Preserve that ambiguity for an operator;
// never resend or resolve it using an existence-only message observation.
func reconcileTaskWakeGroupsTx(ctx context.Context, tx *sql.Tx, waits []domain.TaskWait, now time.Time) error {
	groups := map[string][]int{}
	for i, w := range waits {
		if w.Woken() && w.DeliveryID != "" {
			key := fmt.Sprintf("%q/%q/%d/%q", w.AttemptID, w.ThreadID, w.WakeRevision, w.DeliveryID)
			groups[key] = append(groups[key], i)
		}
	}
	for _, indexes := range groups {
		if len(indexes) < 2 {
			continue
		}
		var members []domain.TaskWait
		states := map[string]bool{}
		for _, i := range indexes {
			members = append(members, waits[i])
			states[waits[i].Delivery] = true
		}
		claimed, err := taskWakeGroupClaimedTx(ctx, tx, members)
		if err != nil {
			return err
		}
		target := ""
		switch {
		case states["manual-recovery-required"] || (!claimed && (states["sending"] || states["recovery-required"] || states["delivered"])):
			target = "manual-recovery-required"
		case len(states) == 1:
			continue
		case states["delivered"]:
			target = "delivered"
		case states["sending"] || states["recovery-required"]:
			target = "recovery-required"
		case states["abandoned"]:
			target = "abandoned"
		case states["pending"] && states["held"]:
			target = "held"
		default:
			target = "manual-recovery-required"
		}
		if target == "manual-recovery-required" {
			first := members[0]
			if err := recordTaskWaitEventTx(ctx, tx, domain.TaskWaitReconciliation{
				ID: "group-delivery-ambiguous:" + first.DeliveryID, Kind: "group-delivery-ambiguous",
				AttemptID: first.AttemptID, ThreadID: first.ThreadID, WaitID: first.ID,
				Detail:     "manual recovery required: message existence cannot prove every grouped outcome was delivered",
				ObservedAt: now.UTC(),
			}); err != nil {
				return err
			}
		}
		for _, i := range indexes {
			if waits[i].Delivery == target {
				continue
			}
			waits[i].Delivery = target
			if target == "delivered" {
				delivered := now.UTC()
				waits[i].DeliveredAt = &delivered
			}
			if err := saveTaskWaitTx(ctx, tx, waits[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
