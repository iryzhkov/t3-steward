package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"io"
)

type commitBundleClient interface {
	FetchCommitBundle(context.Context, workerproto.CommitBundleRequest) (io.ReadCloser, error)
}

func coordinatorCommitBundleOpener(reader backlogadmin.Reader, observer *coordinatorRepositoryObserver) backlogadmin.CommitBundleOpenFunc {
	return func(ctx context.Context, p backlog.CommitProvenance, a domain.Artifact) (io.ReadCloser, error) {
		records, err := reader.LoadCoordinatorRecords(ctx)
		if err != nil {
			return nil, err
		}
		assignmentID := ""
		for _, attempt := range records.Attempts {
			if attempt.ID == a.AttemptID && attempt.TaskID == p.TaskID && attempt.WorkflowRunID == p.WorkflowRunID {
				assignmentID = attempt.AssignmentID
				break
			}
		}
		workerID := ""
		for _, assignment := range records.Assignments {
			if assignment.ID == assignmentID && assignment.AttemptID == a.AttemptID {
				workerID = assignment.WorkerID
				break
			}
		}
		if workerID == "" {
			return nil, errors.New("commit export: producing worker assignment unavailable")
		}
		client, closer, err := observer.dialWorkerOperation(ctx, workerID, coordinatorWorkerArtifactSendOperation)
		if err != nil {
			return nil, err
		}
		if closer != nil {
			defer closer()
		}
		fetcher, ok := client.(commitBundleClient)
		if !ok {
			return nil, errors.New("commit export: producing worker does not support bundle export")
		}
		raw, err := backlog.MarshalCommitProvenance(p)
		if err != nil {
			return nil, err
		}
		return fetcher.FetchCommitBundle(ctx, workerproto.CommitBundleRequest{Provenance: raw, MaxBytes: workerproto.MaxCommitBundleBytes})
	}
}
