package backlogadmin

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A CLI ask answer signed by a configured approver passes the remote relay and
// is reverified at the final boundary as the approver role; a T3-sourced
// answer in an approver frame is not an approval proof at all.
func TestAskAnswerSurvivesTheApproverPathOnlyFromTheCLI(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	credentials := AdminCredentials{
		ClientPrincipal: "human", ClientKeyID: "human-key", ClientSecret: []byte("human-secret-1234"),
		CoordinatorPrincipal: "coord", CoordinatorKeyID: "coord-key", CoordinatorSecret: []byte("coord-secret-1234"),
	}
	remote, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID: "coord", Clients: map[string]AdminCredentials{"human": credentials},
		Approvers: map[string]bool{"human": true}, MaxRequestBytes: 1 << 20,
		MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	local := LocalServer{
		CoordinatorID: "coord", Approvers: map[string]AdminCredentials{"human": credentials},
		Now: func() time.Time { return now.Add(time.Second) },
	}
	for _, source := range []domain.AskAnswerSource{domain.AskSourceCLI, domain.AskSourceT3} {
		original := localRequest{
			Version: LocalTransportVersion, Operation: localOperationNodeWait,
			NodeWait: &NodeWaitOperation{Action: AskAnswerAction, Answer: &domain.AskAnswer{
				AskID: "tw-ask-1", Options: []string{"beta"}, Source: source,
			}},
		}
		frame, err := newRemoteFrame(localOperationNodeWait, "session-ask", "request-ask-"+string(source),
			"human", "coord", 1, now, now.Add(time.Minute), original)
		if err != nil {
			t.Fatal(err)
		}
		if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
			t.Fatal(err)
		}
		relayed, _, verified, protocolErr := remote.validate("", frame)
		if protocolErr != nil || !verified || relayed.ApprovalFrame == nil {
			t.Fatalf("%s: remote validation: verified=%v err=%v", source, verified, protocolErr)
		}
		principal, request, err := local.verifyApprovalFrame(relayed)
		if source == domain.AskSourceT3 {
			if err == nil {
				t.Fatal("a T3-sourced answer was accepted as an approval proof")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(principal.Roles) != 1 || principal.Roles[0] != ApproverRole || request.NodeWait.Answer == nil ||
			request.NodeWait.Answer.AskID != "tw-ask-1" {
			t.Fatalf("principal=%+v request=%+v", principal, request.NodeWait)
		}
	}
}
