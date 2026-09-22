package backlogadmin

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func signedApprovalRequest(t *testing.T, now time.Time) (LocalServer, localRequest) {
	t.Helper()
	credentials := AdminCredentials{
		ClientPrincipal: "human", ClientKeyID: "human-key", ClientSecret: []byte("human-secret-1234"),
		CoordinatorPrincipal: "coord", CoordinatorKeyID: "coord-key", CoordinatorSecret: []byte("coord-secret-1234"),
	}
	decision := domain.AttentionDecision{
		ID: "decision-1", WaitID: "tw-request-1", RequestID: "request-1",
		WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
		AssignmentID: "assignment-1", ThreadID: "thread-1", RegisteredRevision: 7,
		Kind: domain.AttentionApprove, Reason: "operator approved",
	}
	original := localRequest{
		Version: LocalTransportVersion, Operation: localOperationNodeWait,
		NodeWait: &NodeWaitOperation{Action: "decide-attention", Decision: &decision},
	}
	frame, err := newRemoteFrame(localOperationNodeWait, "session-1", "frame-request-1", "human", "coord", 1, now, now.Add(time.Minute), original)
	if err != nil {
		t.Fatal(err)
	}
	if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
		t.Fatal(err)
	}
	relayed := original
	relayed.ApprovalFrame = &frame
	relayed.RemoteAdmin = &RemoteAdminAssertion{
		Principal: credentials.ClientPrincipal, Coordinator: "coord", RequestID: frame.RequestID,
	}
	server := LocalServer{
		CoordinatorID: "coord", Approvers: map[string]AdminCredentials{"human": credentials},
		Now: func() time.Time { return now.Add(time.Second) },
	}
	return server, relayed
}

func TestApprovalFrameIsReverifiedAtFinalBoundary(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	server, relayed := signedApprovalRequest(t, now)
	principal, request, err := server.verifyApprovalFrame(relayed)
	if err != nil {
		t.Fatal(err)
	}
	if principal.ID != RelayedPrincipalID("human") || len(principal.Roles) != 1 || principal.Roles[0] != ApproverRole {
		t.Fatalf("wrong derived principal: %+v", principal)
	}
	if request.ApprovalFrame != nil || request.RemoteAdmin != nil || request.NodeWait.Decision.ID != "decision-1" {
		t.Fatalf("wrong verified request: %+v", request)
	}
}

func TestApprovalFrameRefusesForgeryAndRelayMutation(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	t.Run("forged signature", func(t *testing.T) {
		server, relayed := signedApprovalRequest(t, now)
		relayed.ApprovalFrame.Authentication.Signature = "00"
		if _, _, err := server.verifyApprovalFrame(relayed); err == nil {
			t.Fatal("forged approval proof was accepted")
		}
	})
	t.Run("payload changed after authentication", func(t *testing.T) {
		server, relayed := signedApprovalRequest(t, now)
		relayed.NodeWait.Decision.Reason = "relay changed it"
		if _, _, err := server.verifyApprovalFrame(relayed); err == nil {
			t.Fatal("mutated relayed approval was accepted")
		}
	})
	t.Run("expired proof", func(t *testing.T) {
		server, relayed := signedApprovalRequest(t, now)
		server.Now = func() time.Time { return now.Add(2 * time.Minute) }
		if _, _, err := server.verifyApprovalFrame(relayed); err == nil {
			t.Fatal("expired approval proof was accepted")
		}
	})
}

func TestApproverCredentialIsCapabilityOnlyAndRevocable(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	credentials := AdminCredentials{
		ClientPrincipal: "human", ClientKeyID: "human-key", ClientSecret: []byte("human-secret-1234"),
		CoordinatorPrincipal: "coord", CoordinatorKeyID: "coord-key", CoordinatorSecret: []byte("coord-secret-1234"),
	}
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID: "coord", Clients: map[string]AdminCredentials{"human": credentials},
		Approvers: map[string]bool{"human": true}, MaxRequestBytes: 1 << 20,
		MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	validate := func(request localRequest) *workerproto.ProtocolError {
		t.Helper()
		frame, err := newRemoteFrame(request.Operation, "session", "request", "human", "coord", 1, now, now.Add(time.Minute), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
			t.Fatal(err)
		}
		_, _, _, protocolErr := server.validate("", frame)
		return protocolErr
	}
	for name, request := range map[string]localRequest{
		"ordinary mutation": {
			Version: LocalTransportVersion, Operation: localOperationMutation, Mutation: &Mutation{},
		},
		"submission": {
			Version: LocalTransportVersion, Operation: localOperationSubmission,
			SubmissionSize: 1, Submission: &LocalSubmissionRequest{ArchiveSHA256: "digest"},
		},
		"wait settlement": {
			Version: LocalTransportVersion, Operation: localOperationNodeWait,
			NodeWait: &NodeWaitOperation{Action: "settle-task"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if protocolErr := validate(request); protocolErr == nil || protocolErr.Code != workerproto.ErrorAuthorization {
				t.Fatalf("capability escape accepted: %+v", protocolErr)
			}
		})
	}
	for _, action := range []string{"inspect-attention", "decide-attention"} {
		request := localRequest{Version: LocalTransportVersion, Operation: localOperationNodeWait, NodeWait: &NodeWaitOperation{Action: action}}
		if action == "decide-attention" {
			request.NodeWait.Decision = &domain.AttentionDecision{}
		}
		frame, err := newRemoteFrame(request.Operation, "session", "allowed-"+action, "human", "coord", 1, now, now.Add(time.Minute), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
			t.Fatal(err)
		}
		relayed, _, _, protocolErr := server.validate("", frame)
		if protocolErr != nil || relayed.ApprovalFrame == nil {
			t.Fatalf("%s was not relayed with original proof: request=%+v err=%v", action, relayed, protocolErr)
		}
	}
	delete(server.config.Clients, "human")
	request := localRequest{Version: LocalTransportVersion, Operation: localOperationNodeWait, NodeWait: &NodeWaitOperation{Action: "inspect-attention"}}
	if protocolErr := validate(request); protocolErr == nil || protocolErr.Code != workerproto.ErrorAuthentication {
		t.Fatalf("revoked approver authenticated: %+v", protocolErr)
	}
}
