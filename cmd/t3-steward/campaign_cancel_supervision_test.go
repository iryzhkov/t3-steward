package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// terminalRunDetail is run-3ae2f87b's shape: every task terminal, the sink
// still waiting, the run itself at revision 9.
func terminalRunDetail() backlogadmin.WorkflowDetail {
	detail := cancelRunDetail()
	detail.Summary.Run.Revision = 9
	detail.Summary.Run.Sink = &domain.SinkTask{ID: "sink:run-1", Progress: domain.ProgressBlocked}
	for i := range detail.Tasks {
		detail.Tasks[i].Attempt.Progress = domain.ProgressSucceeded
	}
	detail.Tasks[1].Attempt.Progress = domain.ProgressCancelled
	return detail
}

func supervisionShowing(state backlogadmin.SupervisionState) *fakeSupervisionTransport {
	return &fakeSupervisionTransport{response: backlogadmin.SupervisionResponse{
		Operation: backlogadmin.SupervisionShow, RunID: "run-1", State: &state,
	}}
}

// A run with no live task but an unsettled sink is closed by one cancel,
// fenced on the run's own revision, instead of being refused locally with
// "every task of it is already terminal".
func TestCampaignCancelOfAnAllTerminalRunClosesItFencedOnTheRun(t *testing.T) {
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, terminalRunDetail(), &sent)
	if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].ExpectedRevision != 9 || sent[0].WorkflowRunID != "run-1" {
		t.Fatalf("mutations = %+v, want one fenced on run revision 9", sent)
	}
	if text := out.String(); !strings.Contains(text, "will close run run-1") || !strings.Contains(text, "supervision") {
		t.Fatalf("output does not say what the command will do:\n%s", text)
	}
}

// The note that the cancel also closes the run's supervision is a claim about
// what the coordinator will do, so it is printed only for a coordinator that
// does it.
func TestCampaignCancelSaysItClosesSupervisionOnlyOnACoordinatorThatDoes(t *testing.T) {
	state := supervisionTestState()
	state.Activation.State = domain.ActivationSpent
	for release, want := range map[string]bool{"v0.11.0-rc.99": false, campaignRunCloseRelease: true} {
		var out bytes.Buffer
		var sent []backlogadmin.Mutation
		cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
		cli.release = func(context.Context) (string, error) { return release, nil }
		cli.superviseAs = supervisionShowing(state).supervise
		if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"}); err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(out.String(), "resolves the run's open supervision incidents"); got != want {
			t.Fatalf("release %s: note printed = %v, want %v:\n%s", release, got, want, out.String())
		}
	}
}

// A settled run is refused locally and says where to look.
func TestCampaignCancelOfASettledRunIsRefused(t *testing.T) {
	detail := terminalRunDetail()
	detail.Summary.Run.Progress = domain.ProgressCancelled
	detail.Summary.Run.Sink.Progress = domain.ProgressSucceeded
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	err := cancelRunCLI(&out, detail, &sent).run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"})
	if err == nil || !strings.Contains(err.Error(), "already settled as cancelled") ||
		!strings.Contains(err.Error(), "t3-steward campaign show run-1") || len(sent) != 0 {
		t.Fatalf("err = %v, sent = %+v", err, sent)
	}
}

// While an overseer activation is live the whole-run cancel is refused before
// anything is sent, naming the activation and the commands to wait and retry.
func TestCampaignCancelIsRefusedWhileAnOverseerIsLive(t *testing.T) {
	state := supervisionTestState()
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
	cli.superviseAs = supervisionShowing(state).supervise
	err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"})
	if err == nil || len(sent) != 0 {
		t.Fatalf("err = %v, sent = %+v, want a refusal before anything is sent", err, sent)
	}
	for _, want := range []string{"activation-1", "epoch 3", "t3-steward campaign supervision show run-1", "t3-steward campaign cancel run-1 --reason"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
}

// An older coordinator still refuses an all-terminal run. The refusal then
// carries the exact supervision commands that close the run by hand, with the
// incident ids and revisions filled in, instead of leaving the operator to dig
// them out of --json.
func TestCampaignCancelRefusedByAnOlderCoordinatorNamesTheResolveCommands(t *testing.T) {
	state := supervisionTestState()
	state.Activation.State = domain.ActivationSpent
	state.Incidents = []backlogadmin.SupervisionIncidentView{
		{Incident: domain.ReviewIncident{ID: "inc-escalated", State: domain.IncidentEscalated, Revision: 3}},
		{Incident: domain.ReviewIncident{ID: "inc-open", State: domain.IncidentOpen, Revision: 1}},
		{Incident: domain.ReviewIncident{ID: "inc-done", State: domain.IncidentResolved, Revision: 5}},
	}
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, terminalRunDetail(), &sent)
	cli.superviseAs = supervisionShowing(state).supervise
	cli.mutate = func(context.Context, backlogadmin.Mutation) (backlogadmin.MutationResponse, error) {
		return backlogadmin.MutationResponse{}, errors.New("invalid backlog admin query: run run-1 has no task to cancel; every task of it is already terminal")
	}
	err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"})
	if err == nil {
		t.Fatal("the older coordinator's refusal was lost")
	}
	for _, want := range []string{
		"t3-steward campaign supervision resolve run-1 --incident inc-escalated --expected-revision 3 --outcome cancelled --request-id cancel-inc-escalated-r3 --reason",
		"t3-steward campaign supervision escalate run-1 --incident inc-open --expected-revision 1 --request-id cancel-escalate-inc-open-r1 --reason",
		"t3-steward campaign supervision resolve run-1 --incident inc-open --expected-revision 2 --outcome cancelled --request-id cancel-inc-open-r2 --reason",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal lacks %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "inc-done") {
		t.Fatalf("a resolved incident was offered:\n%s", err)
	}
}
