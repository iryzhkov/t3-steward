package t3

import (
	"context"

	"github.com/iryzhkov/t3-steward/internal/domain"
	waitdelivery "github.com/iryzhkov/t3-steward/internal/wait"
)

func nodeWakeID(token, purpose string) string {
	return deterministicID(token, "node-wake-"+purpose)
}
func (c *Control) ObserveNodeWake(ctx context.Context, threadID, token string) (bool, error) {
	detail, err := c.client.ThreadDetail(ctx, threadID, 100)
	if err != nil {
		return false, err
	}
	id := nodeWakeID(token, "message")
	for _, message := range detail.Messages {
		if message.ID == id && message.Role == "user" {
			return true, nil
		}
	}
	return false, nil
}

// ReconcileNodeWake explicitly reports that a bounded message listing can
// prove delivery but can never prove non-delivery.
func (c *Control) ReconcileNodeWake(ctx context.Context, threadID, token string) (waitdelivery.WakeReceiptStatus, error) {
	found, err := c.ObserveNodeWake(ctx, threadID, token)
	if err != nil {
		return waitdelivery.WakeReceiptUnknown, err
	}
	if found {
		return waitdelivery.WakeReceiptDelivered, nil
	}
	return waitdelivery.WakeReceiptUnknown, nil
}

func (c *Control) SendNodeWake(ctx context.Context, thread domain.Thread, token, text string) error {
	// Node delivery has its own policy; callers durably claim before this effect.
	live := *c
	live.DryRun = false
	return live.sendMessageIDs(ctx, thread, text, "node-wake", nodeWakeID(token, "command"), nodeWakeID(token, "message"))
}
