package backlogadmin

import (
	"context"
	"io"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// adminDispatch runs one decoded coordinator-admin operation. Both carriers
// share it, so the local socket and the restricted SSH command speak exactly
// one operation vocabulary and cannot drift apart in what they accept.
//
// The principal is the one the carrier's own authentication produced. Whatever
// principal the request claimed is discarded here, on every carrier.
type adminDispatch struct {
	service            LocalService
	maxArtifactBytes   int64
	maxSubmissionBytes int64
}

// handle answers one request. When the operation is an artifact read that
// succeeded it also returns the opened content: the carrier writes the response
// frame and then streams exactly ArtifactSize raw bytes after it. body is the
// carrier's stream positioned immediately after the request frame, which only
// the submission operation reads.
type recoveryRetryService interface {
	RetryRecovery(context.Context, Principal, domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error)
}

func (d adminDispatch) retryRecovery(ctx context.Context, principal Principal, request localRequest, response *localResponse) {
	handler, ok := d.service.(recoveryRetryService)
	if !ok {
		response.Error = "recovery retry is unavailable"
		return
	}
	if request.RecoveryRetry == nil || request.Supervision != nil || request.Query != nil || request.Mutation != nil ||
		request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 {
		response.Error = "malformed recovery retry request"
		return
	}
	value, err := handler.RetryRecovery(ctx, principal, *request.RecoveryRetry)
	if err != nil {
		response.Error = err.Error()
		return
	}
	response.RecoveryRetryResponse = &value
}

func (d adminDispatch) handle(
	ctx context.Context,
	principal Principal,
	request localRequest,
	body io.Reader,
) (response localResponse, artifact *ArtifactContent) {
	response = localResponse{Version: LocalTransportVersion}
	defer func() {
		if err := projectRC119UnversionedAnswer(&response); err != nil {
			response = localResponse{Version: LocalTransportVersion, Error: "project the rc.119 answer: " + err.Error()}
		}
	}()
	if request.CommitExport != nil && request.Operation != localOperationCommitExport {
		response.Error = "unexpected commit export request"
		return response, nil
	}
	if request.Lease != nil && request.Operation != localOperationLease {
		response.Error = "unexpected lease request"
		return response, nil
	}
	if request.RecoveryRetry != nil && request.Operation != localOperationRecoveryRetry {
		response.Error = "unexpected recovery retry request"
		return response, nil
	}
	if request.NodeWait != nil && request.Operation != localOperationNodeWait {
		response.Error = "unexpected native wait"
		return response, nil
	}
	if request.Operation != localOperationGraphAmendment && request.GraphAmendment != nil {
		response.Error = "unexpected graph amendment"
		return response, nil
	}
	if request.WorkerEnrollment != nil && request.Operation != localOperationWorkerEnrollment {
		response.Error = "unexpected worker enrollment"
		return response, nil
	}
	if request.Supervision != nil && !supervisionOperation(request.Operation) {
		response.Error = "unexpected supervision request"
		return response, nil
	}
	switch request.Operation {
	case localOperationCommitExport:
		handler, ok := d.service.(interface {
			ExportCommit(context.Context, Principal, CommitExportRequest) (ArtifactContent, error)
		})
		if !ok || request.CommitExport == nil || request.Query != nil || request.Mutation != nil || request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil || request.UnknownRecovery != nil || request.QuarantineRelease != nil || request.GraphAmendment != nil || request.NodeWait != nil || request.WorkerEnrollment != nil || request.Supervision != nil || request.RecoveryRetry != nil {
			response.Error = "malformed commit export request"
			break
		}
		value, err := handler.ExportCommit(ctx, principal, *request.CommitExport)
		if err != nil {
			response.Error = err.Error()
			break
		}
		if value.Content == nil || value.Provenance == nil || value.Metadata.Size < 0 || value.Metadata.Size > d.maxArtifactBytes {
			if value.Content != nil {
				_ = value.Content.Close()
			}
			response.Error = "invalid or oversized commit export"
			break
		}
		response.ArtifactMetadata = &value.Metadata
		response.ArtifactSize = value.Metadata.Size
		response.CommitProvenance = value.Provenance
		return response, &value
	case localOperationLease:
		d.lease(ctx, principal, request, &response)
	case localOperationRecoveryRetry:
		d.retryRecovery(ctx, principal, request, &response)
	case localOperationSupervisionShow, localOperationSupervisionDecision:
		d.supervise(ctx, principal, request, &response)
	case localOperationWorkerEnrollment:
		d.enrollWorker(ctx, principal, request, &response)
	case localOperationGraphAmendment:
		d.amendGraph(ctx, principal, request, &response)
	case localOperationNodeWait:
		d.nodeWait(ctx, principal, request, &response)
	case localOperationQuery:
		if request.Query == nil || request.Mutation != nil || request.ArtifactID != "" ||
			request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil ||
			request.UnknownRecovery != nil || request.QuarantineRelease != nil {
			response.Error = "malformed local admin query"
			break
		}
		request.Query.Principal = principal
		value, queryErr := d.service.Query(ctx, *request.Query)
		if queryErr != nil {
			response.Error = queryErr.Error()
		} else {
			response.Response = &value
		}
	case localOperationMutation:
		if request.Mutation == nil || request.Query != nil || request.ArtifactID != "" ||
			request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil ||
			request.UnknownRecovery != nil || request.QuarantineRelease != nil {
			response.Error = "malformed local admin mutation"
			break
		}
		request.Mutation.Principal = principal
		value, mutationErr := d.service.Mutate(ctx, *request.Mutation)
		if mutationErr != nil {
			response.Error = mutationErr.Error()
		} else {
			response.MutationResponse = &value
		}
	case localOperationArtifact:
		if request.ArtifactID == "" || request.Query != nil || request.Mutation != nil ||
			request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil ||
			request.UnknownRecovery != nil || request.QuarantineRelease != nil {
			response.Error = "malformed local admin artifact request"
			break
		}
		value, artifactErr := d.service.OpenArtifact(ctx, principal, request.ArtifactID)
		if artifactErr != nil {
			response.Error = artifactErr.Error()
			break
		}
		if value.Metadata.Size < 0 || value.Metadata.Size > d.maxArtifactBytes {
			_ = value.Content.Close()
			response.Error = "artifact exceeds local transport limit"
			break
		}
		response.ArtifactMetadata = &value.Metadata
		response.ArtifactSize = value.Metadata.Size
		return response, &value
	case localOperationSubmission:
		if request.Submission == nil || request.Query != nil || request.Mutation != nil ||
			request.ArtifactID != "" || request.SubmissionSize <= 0 ||
			request.SubmissionSize > d.maxSubmissionBytes || request.ScheduleDefinition != nil ||
			request.UnknownRecovery != nil || request.QuarantineRelease != nil {
			response.Error = "malformed or oversized local submission request"
			break
		}
		archive := &io.LimitedReader{R: body, N: request.SubmissionSize}
		value, submissionErr := d.service.SubmitArchive(ctx, principal, *request.Submission, archive)
		if submissionErr != nil {
			response.Error = submissionErr.Error()
		} else if archive.N != 0 {
			response.Error = "submission archive ended before its declared size"
		} else {
			response.SubmissionResponse = &value
		}
	case localOperationScheduleDefinition:
		if request.ScheduleDefinition == nil || request.Query != nil || request.Mutation != nil ||
			request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 ||
			request.UnknownRecovery != nil || request.QuarantineRelease != nil {
			response.Error = "malformed local schedule definition request"
			break
		}
		value, definitionErr := d.service.PutSchedule(ctx, principal, *request.ScheduleDefinition)
		if definitionErr != nil {
			response.Error = definitionErr.Error()
		} else {
			response.ScheduleDefinitionResponse = &value
		}
	case localOperationUnknownRecovery:
		if request.UnknownRecovery == nil || request.Query != nil || request.Mutation != nil ||
			request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 ||
			request.ScheduleDefinition != nil || request.QuarantineRelease != nil {
			response.Error = "malformed local unknown recovery request"
			break
		}
		value, recoveryErr := d.service.RecoverUnknown(ctx, principal, *request.UnknownRecovery)
		if recoveryErr != nil {
			response.Error = recoveryErr.Error()
			break
		}
		response.UnknownRecoveryResponse = &value
		if err := projectV1Answers(&response); err != nil {
			response.UnknownRecoveryResponse = nil
			response.Error = "project the unknown recovery answer: " + err.Error()
		}
	case localOperationQuarantineRelease:
		if request.QuarantineRelease == nil || request.Query != nil || request.Mutation != nil ||
			request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 ||
			request.ScheduleDefinition != nil || request.UnknownRecovery != nil {
			response.Error = "malformed local quarantine release request"
			break
		}
		value, releaseErr := d.service.ReleaseQuarantine(ctx, principal, *request.QuarantineRelease)
		if releaseErr != nil {
			response.Error = releaseErr.Error()
		} else {
			response.QuarantineReleaseResponse = &value
		}
	default:
		response.Error = "unknown local admin operation"
	}
	return response, nil
}

// The three operations below reach the service through optional interfaces
// because not every LocalService implementation offers them.

func (d adminDispatch) enrollWorker(ctx context.Context, principal Principal, request localRequest, response *localResponse) {
	handler, ok := d.service.(interface {
		EnrollWorker(context.Context, Principal, domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error)
	})
	if !ok || request.WorkerEnrollment == nil || request.GraphAmendment != nil || request.NodeWait != nil ||
		request.Query != nil || request.Mutation != nil || request.ArtifactID != "" || request.Submission != nil ||
		request.SubmissionSize != 0 || request.ScheduleDefinition != nil || request.UnknownRecovery != nil ||
		request.QuarantineRelease != nil {
		response.Error = "malformed worker enrollment request"
		return
	}
	value, err := handler.EnrollWorker(ctx, principal, *request.WorkerEnrollment)
	if err != nil {
		response.Error = err.Error()
		return
	}
	response.WorkerEnrollment = &value
}

func (d adminDispatch) amendGraph(ctx context.Context, principal Principal, request localRequest, response *localResponse) {
	handler, ok := d.service.(interface {
		AmendGraph(context.Context, Principal, domain.GraphAmendment) (domain.GraphAmendmentResult, error)
	})
	if !ok || request.GraphAmendment == nil || request.NodeWait != nil || request.Query != nil ||
		request.Mutation != nil || request.ArtifactID != "" || request.Submission != nil ||
		request.SubmissionSize != 0 || request.ScheduleDefinition != nil || request.UnknownRecovery != nil ||
		request.QuarantineRelease != nil {
		response.Error = "malformed graph amendment request"
		return
	}
	value, err := handler.AmendGraph(ctx, principal, *request.GraphAmendment)
	if err != nil {
		response.Error = err.Error()
		return
	}
	response.GraphAmendment = &value
	// Only a client of rc.118 or later sends useCommit, so it reads every
	// field of this release, including the reused commits its rerun receipt
	// prints. Every other request keeps the rc.115 shape.
	if request.GraphAmendment.UseCommit {
		return
	}
	if err := projectV1Answers(response); err != nil {
		response.GraphAmendment = nil
		response.Error = "project the graph amendment answer: " + err.Error()
	}
}

func (d adminDispatch) nodeWait(ctx context.Context, principal Principal, request localRequest, response *localResponse) {
	handler, ok := d.service.(interface {
		NodeWait(context.Context, Principal, NodeWaitOperation) (NodeWaitResponse, error)
	})
	if !ok || request.NodeWait == nil || request.Query != nil || request.Mutation != nil ||
		request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 ||
		request.ScheduleDefinition != nil || request.UnknownRecovery != nil ||
		request.QuarantineRelease != nil {
		response.Error = "malformed native wait request"
		return
	}
	value, err := handler.NodeWait(ctx, principal, *request.NodeWait)
	if err != nil {
		response.Error = err.Error()
		return
	}
	response.NodeWait = &value
}
