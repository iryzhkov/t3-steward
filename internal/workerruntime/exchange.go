package workerruntime

import (
	"context"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type Exchange struct {
	Runtime *Runtime
	Server  *workerproto.Server
	Custody *CustodyStore
}

func (e Exchange) Handle(ctx context.Context, envelope workerproto.Envelope) (workerproto.Envelope, error) {
	if e.Runtime == nil || e.Server == nil {
		return workerproto.Envelope{}, fmt.Errorf("worker exchange: runtime and protocol server are required")
	}
	return e.Server.Handle(ctx, envelope, e.handle)
}

func (e Exchange) handle(ctx context.Context, envelope workerproto.Envelope) (workerproto.MessageType, any, error) {
	switch envelope.Type {
	case workerproto.MessageCapabilities:
		var request workerproto.CapabilityNegotiation
		if err := workerproto.DecodePayload(envelope, workerproto.MessageCapabilities, &request); err != nil {
			return "", nil, err
		}
		return workerproto.MessageCapabilities, request, nil
	case workerproto.MessageSnapshot:
		var request workerproto.SnapshotRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageSnapshot, &request); err != nil {
			return "", nil, err
		}
		snapshot, err := e.Runtime.Snapshot(ctx)
		return workerproto.MessageObservations, workerproto.Observations{Snapshot: snapshot}, err
	case workerproto.MessageOffers:
		var offers workerproto.AssignmentOffers
		if err := workerproto.DecodePayload(envelope, workerproto.MessageOffers, &offers); err != nil {
			return "", nil, err
		}
		claims, err := e.Runtime.AcceptOffers(ctx, offers)
		return workerproto.MessageClaims, claims, err
	case workerproto.MessageLeaseRenewals:
		var renewals workerproto.LeaseRenewals
		if err := workerproto.DecodePayload(envelope, workerproto.MessageLeaseRenewals, &renewals); err != nil {
			return "", nil, err
		}
		if err := e.Runtime.ApplyLeaseRenewals(renewals); err != nil {
			return "", nil, err
		}
		snapshot, err := e.Runtime.Snapshot(ctx)
		return workerproto.MessageObservations, workerproto.Observations{Snapshot: snapshot}, err
	case workerproto.MessageCommands:
		var delivery workerproto.CommandDelivery
		if err := workerproto.DecodePayload(envelope, workerproto.MessageCommands, &delivery); err != nil {
			return "", nil, err
		}
		acknowledgements, err := e.Runtime.DeliverCommands(ctx, delivery)
		return workerproto.MessageAcknowledgements, acknowledgements, err
	case workerproto.MessageThrottleCommands:
		var delivery workerproto.ThrottleDelivery
		if err := workerproto.DecodePayload(envelope, workerproto.MessageThrottleCommands, &delivery); err != nil {
			return "", nil, err
		}
		acknowledgements, err := e.Runtime.DeliverThrottle(ctx, delivery.Commands)
		return workerproto.MessageThrottleAcknowledgements, workerproto.ThrottleAcknowledgements{Acknowledgements: acknowledgements}, err
	default:
		return "", nil, &workerproto.ProtocolError{Code: workerproto.ErrorAuthorization, Message: "message kind is not a worker request", RequestID: envelope.RequestID}
	}
}
