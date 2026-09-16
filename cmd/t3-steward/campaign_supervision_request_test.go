package main

// The requests the supervision command actually builds, checked against the
// validator that actually refuses them.
//
// This closes the loop the live qualification found open on the other side.
// "campaign supervision show" was refused on both carriers with "malformed
// supervision request", and the request was not malformed at all: the
// coordinator's local service did not forward Supervise, and the type assertion
// that looks for it shared a branch with the malformed answer. A test that
// builds each verb the way the command does and validates it says which half of
// that sentence is true.

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// supervisionCommandLines is one accepted command line per verb, spelled as the
// help text spells it.
func supervisionCommandLines() map[backlogadmin.SupervisionOperation][]string {
	return map[backlogadmin.SupervisionOperation][]string{
		backlogadmin.SupervisionShow: {"run-1"},
		backlogadmin.SupervisionDecide: {
			"run-1", "--gate", "gate-1", "--accept", "--evidence", "snapshot-1",
			"--expected-revision", "3", "--graph-revision", "12", "--incident", "incident-1",
			"--request-id", "key-1", "--reason", "the rubric is satisfied", "--activation", "4",
		},
		backlogadmin.SupervisionHold: {
			"run-1", "--scope", "branch:task-publish", "--expected-revision", "7",
			"--request-id", "key-2", "--reason", "waiting for an operator",
		},
		backlogadmin.SupervisionRelease: {
			"run-1", "--hold", "hold-1", "--expected-revision", "8",
			"--request-id", "key-3", "--reason", "the concern is resolved",
		},
		backlogadmin.SupervisionEscalate: {
			"run-1", "--incident", "incident-1", "--expected-revision", "2",
			"--request-id", "key-4", "--reason", "this needs a person",
		},
		backlogadmin.SupervisionResolve: {
			"run-1", "--incident", "incident-1", "--expected-revision", "2",
			"--outcome", "remediated",
			"--request-id", "key-5", "--reason", "the correction landed",
		},
	}
}

// Every verb the CLI offers builds a request the coordinator's own validator
// accepts, show included.
func TestEverySupervisionCommandBuildsAValidRequest(t *testing.T) {
	lines := supervisionCommandLines()
	for _, operation := range backlogadmin.SupervisionOperations() {
		arguments, declared := lines[operation]
		if !declared {
			t.Fatalf("no command line is exercised for verb %q", operation)
		}
		parsed, err := parseCampaignSupervisionArgs(operation, arguments)
		if err != nil {
			t.Fatalf("%s: parse: %v", operation, err)
		}
		request, err := campaignSupervisionRequest(operation, parsed)
		if err != nil {
			t.Fatalf("%s: build: %v", operation, err)
		}
		if request.Version != backlogadmin.SupervisionVersion {
			t.Fatalf("%s: version = %q", operation, request.Version)
		}
		if request.Operation != operation {
			t.Fatalf("%s: operation = %q", operation, request.Operation)
		}
		if err := request.Validate(); err != nil {
			t.Fatalf("%s: the request the command builds is refused as malformed: %v", operation, err)
		}
	}
}

// A show carries no decision, which is the rule most likely to be tripped by a
// field the builder sets for every verb.
func TestSupervisionShowCarriesNoDecision(t *testing.T) {
	parsed, err := parseCampaignSupervisionArgs(backlogadmin.SupervisionShow, []string{"run-1", "--json"})
	if err != nil {
		t.Fatal(err)
	}
	request, err := campaignSupervisionRequest(backlogadmin.SupervisionShow, parsed)
	if err != nil {
		t.Fatal(err)
	}
	if request.RequestKey != "" || request.ExpectedRevision != 0 || request.Reason != "" {
		t.Fatalf("show carries decision fields: %+v", request)
	}
	if request.Gate != nil || request.Hold != nil || request.Release != nil || request.Incident != nil {
		t.Fatalf("show carries a payload: %+v", request)
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("show is refused: %v", err)
	}
}

// A show is the command an operator runs when a gate is not moving, so its
// human output names the reason no overseer can be dispatched. That reason
// appears in no gate, hold or incident the run has.
func TestSupervisionShowPrintsWhyNoOverseerCanBeDispatched(t *testing.T) {
	state := supervisionTestState()
	state.RouteAvailable = false
	state.RouteBlockReason = "no supervisor client configured; operator decision required"
	out := &bytes.Buffer{}
	transport := &fakeSupervisionTransport{
		response: backlogadmin.SupervisionResponse{
			Operation: backlogadmin.SupervisionShow, RunID: "run-1", State: &state,
		},
	}
	cli := supervisionTestCLI(out, transport)
	if err := cli.runSupervision(context.Background(), []string{"show", "run-1"}); err != nil {
		t.Fatal(err)
	}
	printed := out.String()
	if !strings.Contains(printed, "no overseer can be dispatched") ||
		!strings.Contains(printed, "no supervisor client configured") {
		t.Fatalf("show does not report the route block:\n%s", printed)
	}
}

// The coordinator's local service must forward Supervise. It reaches the
// dispatch through an optional interface, so omitting the method is not a
// compile error on its own: it is every supervision request being refused at
// run time on both carriers.
func TestCoordinatorLocalServiceCarriesSupervision(t *testing.T) {
	var service any = coordinatorLocalService{}
	if _, ok := service.(interface {
		Supervise(context.Context, backlogadmin.Principal, backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error)
	}); !ok {
		t.Fatal("coordinatorLocalService does not forward Supervise, so every supervision request is refused")
	}
}

// The supervisor credential override is accepted by the supervision verbs and
// by no other command, which is what keeps the strongest credential on a worker
// host out of operations that start or amend work.
func TestSupervisorCredentialFlagBelongsToSupervisionOnly(t *testing.T) {
	for _, operation := range backlogadmin.SupervisionOperations() {
		if !campaignSupervisionFlags(operation)[supervisorCredentialFlag] {
			t.Fatalf("verb %q does not accept %s", operation, supervisorCredentialFlag)
		}
	}
	parsed, err := parseCampaignSupervisionArgs(backlogadmin.SupervisionShow,
		[]string{"run-1", supervisorCredentialFlag, "secretref:f03-admin/supervisor"})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.supervisorCredential != "secretref:f03-admin/supervisor" {
		t.Fatalf("credential = %q", parsed.supervisorCredential)
	}
	// The structural half of the same rule: exactly one seam of the CLI takes a
	// supervisor identity, so no other command family can reach a transport
	// built from one. A seam added with that parameter fails here.
	cli := reflect.TypeOf(campaignCLI{})
	identity := reflect.TypeOf(supervisorIdentity{})
	var carriers []string
	for index := 0; index < cli.NumField(); index++ {
		field := cli.Field(index)
		if field.Type.Kind() != reflect.Func {
			continue
		}
		for argument := 0; argument < field.Type.NumIn(); argument++ {
			if field.Type.In(argument) == identity {
				carriers = append(carriers, field.Name)
			}
		}
	}
	if len(carriers) != 1 || carriers[0] != "superviseAs" {
		t.Fatalf("seams carrying a supervisor identity = %v, want only superviseAs", carriers)
	}
}
