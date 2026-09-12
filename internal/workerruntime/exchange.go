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
	case workerproto.MessageArtifactPoll:
		if e.Custody == nil {
			return "", nil, fmt.Errorf("worker exchange: custody is required")
		}
		var request workerproto.ArtifactPollRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageArtifactPoll, &request); err != nil {
			return "", nil, err
		}
		pending, err := e.Custody.PendingUploadByPurpose(request.Purpose, request.Exclude...)
		if err != nil {
			return "", nil, err
		}
		announcement := workerproto.ArtifactAnnouncement{}
		if pending != nil {
			announcement.Upload = &workerproto.ArtifactUploadResponse{Manifest: pending.Manifest, Custody: pending.Custody}
		}
		return workerproto.MessageArtifactAnnouncement, announcement, nil
	case workerproto.MessageArtifactAcknowledge:
		if e.Custody == nil {
			return "", nil, fmt.Errorf("worker exchange: custody is required")
		}
		var request workerproto.ArtifactAcknowledgeRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageArtifactAcknowledge, &request); err != nil {
			return "", nil, err
		}
		if err := e.Custody.AcknowledgeUpload(request.ManifestID); err != nil {
			return "", nil, err
		}
		return workerproto.MessageArtifactAcknowledged, workerproto.ArtifactAcknowledgement(request), nil
	default:
		return "", nil, &workerproto.ProtocolError{Code: workerproto.ErrorAuthorization, Message: "message kind is not a worker request", RequestID: envelope.RequestID}
	}
}
