package main

import (
	"bytes"
	"context"
	"os"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// submitArchiveClaimingParent submits an archive with the caller's parent claim
// attached. A coordinator from before lineage existed decodes requests
// strictly and refuses the unknown parent field before it records anything, so
// the same request is sent once more without the claim: the work is accepted
// as root work instead of failing a task that merely wanted to submit it.
func submitArchiveClaimingParent(ctx context.Context, client adminSubmissionService, request backlogadmin.LocalSubmissionRequest, archive []byte) (backlogadmin.LocalSubmissionResponse, error) {
	response, err := client.SubmitArchive(ctx, request, bytes.NewReader(archive), int64(len(archive)))
	if err != nil && request.Parent != nil && strings.Contains(err.Error(), `json: unknown field "parent"`) {
		request.Parent = nil
		return client.SubmitArchive(ctx, request, bytes.NewReader(archive), int64(len(archive)))
	}
	return response, err
}

// submissionParentLookup names the Steward task this process runs inside, for
// the submissions it makes: `campaign submit`, `task run` and `review`. A run
// submitted from inside a task records that task's attempt as its parent, and
// the planner orders it ahead of root work that arrived after the parent
// started, so a parent parked on its own child cannot be starved by newer
// campaigns. The coordinator checks the claim; it is only ever a claim here.
//
// It is a variable so that CLI tests, which run inside whatever workspace the
// suite was started from, never inherit that workspace's task identity.
var submissionParentLookup = func() *domain.SubmissionParent {
	return submissionParentFrom(os.Getenv, "")
}

// submissionParentFrom resolves the task identity the way `--task current`
// does and returns nil outside a task or when the identity is unusable: an
// unreadable identity makes the submission root work, never a failure.
func submissionParentFrom(getenv func(string) string, directory string) *domain.SubmissionParent {
	identity, err := resolveTaskIdentityFrom(getenv, directory)
	if err != nil {
		return nil
	}
	parent := domain.SubmissionParent{
		RunID: identity.WorkflowRunID, TaskID: identity.TaskID, AttemptID: identity.AttemptID,
	}
	if parent.Validate() != nil {
		return nil
	}
	return &parent
}
