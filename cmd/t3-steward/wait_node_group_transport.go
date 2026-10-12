package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// remoteNodeWaitGroupStore opts into atomic claims without changing the
// default remote store's interfaces or delivery behavior.
type remoteNodeWaitGroupStore struct {
	*remoteNodeWaitStore
	unsupported atomic.Bool
}

var _ wait.NodeGroupClaimer = (*remoteNodeWaitGroupStore)(nil)

func (s *remoteNodeWaitGroupStore) ClaimNodeWakeGroup(ctx context.Context, id, from, deliveryID, payload string, members []string, _ time.Time) (bool, error) {
	if s.unsupported.Load() {
		return false, wait.ErrNodeGroupClaimUnsupported
	}
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{
		Action: backlogadmin.NodeWaitClaimGroupAction, ID: id, From: from,
		DeliveryID: deliveryID, Payload: payload, Members: members,
	})
	// Unknown actions have the same refusal as run collection on either carrier.
	if refusedForAnUnknownField(err) || backlogadmin.RunCollectionUnsupported(err) {
		if s.unsupported.CompareAndSwap(false, true) && s.log != nil {
			s.log.Warn("this coordinator cannot claim node wake groups; using individual transitions until this steward is restarted", "err", err)
		}
		return false, wait.ErrNodeGroupClaimUnsupported
	}
	return response.Changed, err
}
