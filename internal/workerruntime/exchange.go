package workerruntime

import (
	"context"
	"fmt"
	"os"

	"github.com/iryzhkov/t3-steward/internal/resourcetelemetry"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type UsageDeliveryStore interface {
	WorkerUsageBatch(context.Context, []string, int) ([]domain.UsageSample, error)
}

type Exchange struct {
	Runtime *Runtime
	Server  *workerproto.Server
	Custody *CustodyStore
	Usage   UsageDeliveryStore
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
		// The coordinator's statement about parked assignments is applied before
		// the reconcile that Snapshot performs, so this exchange already acts on
		// it. That is what lets a worker which restarted mid-wait learn on its
		// very next exchange that it must not collect.
		if err := e.Runtime.ApplyParkedAssignments(request); err != nil {
			return "", nil, err
		}
		// The campaign keep list is applied in the same exchange. A worker on
		// another host has no other way to learn that a campaign is over, and
		// without it its ref store grows for as long as the worker exists.
		if err := e.Runtime.ReleaseUnretainedCampaignRuns(ctx, request); err != nil {
			return "", nil, err
		}
		snapshot, err := e.Runtime.Snapshot(ctx)
		if err != nil {
			return "", nil, err
		}
		observations := workerproto.Observations{Snapshot: snapshot}
		if request.ReportResourceTelemetry {
			collect := e.Runtime.config.CollectResourceTelemetry
			if collect == nil {
				collect = resourcetelemetry.New().Collect
			}
			telemetry := collect(e.Runtime.config.WorkspaceRoot, os.TempDir(), activeAttempts(snapshot.Assignments))
			if !request.ZramSwapWanted {
				// A coordinator that did not ask decodes telemetry strictly.
				telemetry.ZramSwapUsedMB = nil
			}
			observations.Telemetry = &telemetry
		}
		if e.Usage != nil {
			observations.Usage, err = e.Usage.WorkerUsageBatch(ctx, request.UsageAcknowledgements, workerproto.MaxUsageDelivery)
			observations.AcknowledgedUsageEventIDs = append([]string(nil), request.UsageAcknowledgements...)
		}
		return workerproto.MessageObservations, observations, err
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
	case workerproto.MessageRepositoryProbe:
		var request workerproto.RepositoryProbeRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageRepositoryProbe, &request); err != nil {
			return "", nil, err
		}
		observation, err := e.Runtime.ObserveRepository(ctx, request)
		return workerproto.MessageRepositoryObservation, observation, err
	case workerproto.MessageRepositoryRefResolve:
		var request workerproto.RepositoryRefRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageRepositoryRefResolve, &request); err != nil {
			return "", nil, err
		}
		resolution, err := e.Runtime.ResolveRepositoryRef(ctx, request)
		return workerproto.MessageRepositoryRefResolution, resolution, err
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

// activeAttempts counts attempts that are using, or about to use, the host:
// preparing and resuming attempts load it as much as running ones do.
func activeAttempts(assignments []domain.WorkerAssignmentObservation) int {
	active := 0
	for _, assignment := range assignments {
		switch assignment.Control {
		case domain.ControlPreparing, domain.ControlRunning, domain.ControlResuming:
			active++
		}
	}
	return active
}
