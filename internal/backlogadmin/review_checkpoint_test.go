package backlogadmin

import (
	"context"
	"errors"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type recordingCheckpointOpener struct {
	requests []domain.ReviewCheckpointRequest
}

func (r *recordingCheckpointOpener) OpenReviewCheckpoint(_ context.Context, request domain.ReviewCheckpointRequest) domain.ReviewCheckpointResult {
	r.requests = append(r.requests, request)
	return domain.ReviewCheckpointResult{Round: &domain.ReviewCheckpointRound{RoundID: "round", CheckpointID: request.CheckpointID}}
}

func TestReviewCheckpointActionIsScopedAndStructured(t *testing.T) {
	ctx := context.Background()
	store := openAdminTestStore(t)
	auth := &allowAuthorizer{}
	service, err := New(store, auth)
	if err != nil {
		t.Fatal(err)
	}
	request := domain.ReviewCheckpointRequest{WorkflowRunID: "run-1", TaskID: "implement", AttemptID: "attempt-1", IssuedRevision: 1, AssignmentID: "assignment-1", ThreadID: "thread-1", CheckpointID: "cp-1"}
	principal := Principal{ID: "local:test", Roles: []string{LocalAdminRole}}

	// A coordinator without the operation refuses with a structured answer.
	response, err := service.NodeWait(ctx, principal, NodeWaitOperation{Action: ReviewCheckpointAction, Checkpoint: &request})
	if err != nil || response.Checkpoint == nil {
		t.Fatalf("unwired operation: %+v %v", response, err)
	}
	var refusal *domain.ReviewCheckpointRefusal
	if _, err = response.Checkpoint.Outcome(); !errors.As(err, &refusal) || refusal.Code != domain.ReviewCheckpointUnavailable {
		t.Fatalf("unwired operation answered %v", err)
	}
	if len(auth.actions) != 1 || auth.actions[0].WorkflowRunID != "run-1" || auth.actions[0].TaskID != "implement" {
		t.Fatalf("authorization was not scoped to the checkpoint's task: %+v", auth.actions)
	}

	opener := &recordingCheckpointOpener{}
	service.SetReviewCheckpoint(opener)
	response, err = service.NodeWait(ctx, principal, NodeWaitOperation{Action: ReviewCheckpointAction})
	if err != nil || response.Checkpoint == nil || response.Checkpoint.Refusal == nil || response.Checkpoint.Refusal.Code != domain.ReviewCheckpointInvalidRequest {
		t.Fatalf("missing request: %+v %v", response, err)
	}
	response, err = service.NodeWait(ctx, principal, NodeWaitOperation{Action: ReviewCheckpointAction, Checkpoint: &request, Task: &domain.TaskWaitRegistration{}})
	if err != nil || response.Checkpoint == nil || response.Checkpoint.Refusal == nil || response.Checkpoint.Refusal.Code != domain.ReviewCheckpointInvalidRequest {
		t.Fatalf("mixed request: %+v %v", response, err)
	}
	if len(opener.requests) != 0 {
		t.Fatal("malformed operations reached the opener")
	}
	response, err = service.NodeWait(ctx, principal, NodeWaitOperation{Action: ReviewCheckpointAction, Checkpoint: &request})
	if err != nil {
		t.Fatal(err)
	}
	round, err := response.Checkpoint.Outcome()
	if err != nil || round.CheckpointID != "cp-1" || len(opener.requests) != 1 || opener.requests[0] != request {
		t.Fatalf("operation did not reach the opener intact: %+v %v", round, err)
	}

	auth.err = errors.New("denied")
	if _, err = service.NodeWait(ctx, principal, NodeWaitOperation{Action: ReviewCheckpointAction, Checkpoint: &request}); err == nil || len(opener.requests) != 1 {
		t.Fatal("an unauthorized checkpoint reached the opener")
	}
}

func TestReviewCheckpointRequestValidation(t *testing.T) {
	valid := domain.ReviewCheckpointRequest{WorkflowRunID: "run-1", TaskID: "implement", AttemptID: "attempt-1", IssuedRevision: 1, AssignmentID: "assignment-1", ThreadID: "thread-1", CheckpointID: "cp-1"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*domain.ReviewCheckpointRequest){
		"no attempt":       func(r *domain.ReviewCheckpointRequest) { r.AttemptID = "" },
		"no thread":        func(r *domain.ReviewCheckpointRequest) { r.ThreadID = " " },
		"no checkpoint":    func(r *domain.ReviewCheckpointRequest) { r.CheckpointID = "" },
		"zero revision":    func(r *domain.ReviewCheckpointRequest) { r.IssuedRevision = 0 },
		"short head":       func(r *domain.ReviewCheckpointRequest) { r.HeadCommit = "abc123" },
		"uppercase head":   func(r *domain.ReviewCheckpointRequest) { r.HeadCommit = "ABCDEF0123456789ABCDEF0123456789ABCDEF01" },
		"symbolic head":    func(r *domain.ReviewCheckpointRequest) { r.HeadCommit = "HEAD" },
		"padded run":       func(r *domain.ReviewCheckpointRequest) { r.WorkflowRunID = "run-1 " },
		"no assignment ID": func(r *domain.ReviewCheckpointRequest) { r.AssignmentID = "" },
	} {
		request := valid
		mutate(&request)
		if err := request.Validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, request)
		}
	}
}
