package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Older code may have transitioned just one member of a shared delivery during
// rollback. Preserve its strongest durable evidence: never send again when any
// member says sending, and settle the group when any member proves delivery.
func reconcileTaskWakeGroupsTx(ctx context.Context, tx *sql.Tx, waits []domain.TaskWait, now time.Time) error {
	groups := map[string][]int{}
	for index, wait := range waits {
		if wait.DeliveryID == "" || !wait.Woken() {
			continue
		}
		key := fmt.Sprintf("%q/%q/%d/%q", wait.AttemptID, wait.ThreadID, wait.WakeRevision, wait.DeliveryID)
		groups[key] = append(groups[key], index)
	}
	for _, members := range groups {
		if len(members) < 2 {
			continue
		}
		uniform := true
		for _, index := range members[1:] {
			uniform = uniform && waits[index].Delivery == waits[members[0]].Delivery
		}
		if uniform {
			continue
		}
		delivery := ""
		var deliveredAt *time.Time
		uncertain, abandoned := false, false
		for _, index := range members {
			wait := waits[index]
			switch wait.Delivery {
			case "delivered":
				delivery, deliveredAt = "delivered", wait.DeliveredAt
			case "sending", "recovery-required":
				uncertain = true
			case "abandoned":
				abandoned = true
			}
		}
		if delivery == "" && uncertain {
			delivery = "recovery-required"
		}
		if delivery == "" && abandoned {
			delivery = "abandoned"
		}
		if delivery == "" {
			continue
		}
		if delivery == "delivered" && deliveredAt == nil {
			at := now.UTC()
			deliveredAt = &at
		}
		for _, index := range members {
			if waits[index].Delivery == delivery {
				continue
			}
			waits[index].Delivery = delivery
			if delivery == "delivered" {
				waits[index].DeliveredAt = deliveredAt
			}
			if err := saveTaskWaitTx(ctx, tx, waits[index]); err != nil {
				return err
			}
		}
	}
	return nil
}
