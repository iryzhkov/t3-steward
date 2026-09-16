package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// fakeSupervisionTransport records what the command sent and answers with what
// the test wants back. It is the whole coordinator as far as these tests are
// concerned: what matters is the structured request that left the command, not
// what any store would have done with it.
type fakeSupervisionTransport struct {
	requests []backlogadmin.SupervisionRequest
	response backlogadmin.SupervisionResponse
	err      error
}

func (f *fakeSupervisionTransport) supervise(_ context.Context, request backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error) {
	f.requests = append(f.requests, request)
	if f.err != nil {
		return backlogadmin.SupervisionResponse{}, f.err
	}
	response := f.response
	if response.Version == "" {
		response.Version = backlogadmin.SupervisionVersion
	}
	if response.Operation == "" {
		response.Operation = request.Operation
	}
	if response.RunID == "" {
		response.RunID = request.RunID
	}
	return response, nil
}

func supervisionTestCLI(out *bytes.Buffer, transport *fakeSupervisionTransport) campaignCLI {
	return campaignCLI{stdout: out, stderr: out, supervise: transport.supervise}
}

func supervisionTestState() backlogadmin.SupervisionState {
	return backlogadmin.SupervisionState{
		Record: domain.SupervisionRecord{
			RunID: "run-1",
			Config: domain.SupervisionConfig{
				Route: domain.ProviderRoute{ProviderInstanceID: "instance-a", Model: "model-b"},
			},
			ActivationEpoch:          3,
			ActivationsUsed:          2,
			BudgetGrantedActivations: 5,
			Revision:                 11,
		},
		Activation: domain.Activation{
			ID: "activation-1", RunID: "run-1", Epoch: 3,
			State: domain.ActivationActive, TurnsUsed: 4,
		},
		Gates: []backlogadmin.SupervisionGateView{{
			Gate: domain.Gate{
				Definition: domain.GateDefinition{ID: "gate-1", Name: "review", Final: true},
				RunID:      "run-1", State: domain.GateReadyForReview,
				GraphRevision: 12, EvidenceSnapshotID: "snapshot-9", Revision: 7,
			},
		}},
		Holds: []domain.Hold{{
			ID: "hold-1", RunID: "run-1",
			Scope: domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: "build"},
			Owner: domain.Actor{Kind: domain.ActorOperator, Principal: "operator-1"},
			State: domain.HoldActive, Reason: "waiting for a rubric",
		}},
		Incidents: []backlogadmin.SupervisionIncidentView{{
			Incident: domain.ReviewIncident{
				ID: "incident-1", RunID: "run-1", GateID: "gate-1",
				RequiredDisposition: domain.DispositionGateDecision,
				State:               domain.IncidentOpen, Reason: "producer failed twice",
				Revision: 4,
			},
		}},
	}
}

// The supervision surface is what an overseer agent parses, so its JSON answer
// carries the document version rather than being a bare projection an agent has
// to recognize by shape.
func TestCampaignSupervisionShowJSONIsVersioned(t *testing.T) {
	state := supervisionTestState()
	transport := &fakeSupervisionTransport{response: backlogadmin.SupervisionResponse{
		Version:     backlogadmin.SupervisionVersion,
		Operation:   backlogadmin.SupervisionShow,
		RunID:       "run-1",
		GeneratedAt: time.Unix(1700000000, 0).UTC(),
		Actor:       domain.Actor{Kind: domain.ActorOperator, Principal: "operator-1"},
		State:       &state,
	}}
	var out bytes.Buffer
	cli := supervisionTestCLI(&out, transport)
	if err := cli.run(context.Background(), []string{"supervision", "show", "run-1", "--json"}); err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatalf("show --json is not one JSON document: %v", err)
	}
	if document["version"] != backlogadmin.SupervisionVersion {
		t.Fatalf("version = %v, want %q", document["version"], backlogadmin.SupervisionVersion)
	}
	if document["operation"] != string(backlogadmin.SupervisionShow) {
		t.Fatalf("operation = %v", document["operation"])
	}
	if _, present := document["state"]; !present {
		t.Fatal("show --json carried no state")
	}
	if len(transport.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(transport.requests))
	}
	sent := transport.requests[0]
	if sent.Version != backlogadmin.SupervisionVersion || sent.Operation != backlogadmin.SupervisionShow {
		t.Fatalf("request = %+v", sent)
	}
	if sent.RequestKey != "" || sent.ExpectedRevision != 0 || sent.Gate != nil {
		t.Fatalf("a read carried a decision: %+v", sent)
	}

	// The human rendering names the record, the activation and every gate,
	// hold and incident, because an operator who cannot see the revisions
	// cannot fence a decision against them.
	out.Reset()
	if err := cli.run(context.Background(), []string{"supervision", "show", "run-1"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"supervision of run run-1", "instance-a", "model-b", "epoch        3",
		"2 used of 5 granted", "activation-1", "active",
		"gate gate-1 \"review\" ready-for-review", "revision 7", "graph revision 12",
		"evidence snapshot-9", "final",
		"hold hold-1 branch:build", "operator:operator-1", "waiting for a rubric",
		"incident incident-1 open", "gate gate-1", "requires gate-decision",
		"producer failed twice",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("show does not render %q in:\n%s", want, out.String())
		}
	}
}

// A decision is structured. The outcome comes from --accept or --reject and
// from nowhere else: the reason is recorded as evidence and must never be read
// for a verdict, which is what would happen the first time someone typed
// "--reason rejected, the tests fail".
func TestCampaignSupervisionDecideSendsStructuredDecision(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		flag    string
		reason  string
		outcome domain.GateDecisionOutcome
	}{
		{name: "accept", flag: "--accept", reason: "reject everything, the rubric is met",
			outcome: domain.GateDecisionAccept},
		{name: "reject", flag: "--reject", reason: "accept this, the rubric is not met",
			outcome: domain.GateDecisionReject},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &fakeSupervisionTransport{response: backlogadmin.SupervisionResponse{
				GateID:    "gate-1",
				GateState: domain.GateAccepted,
				Decision:  &domain.GateDecision{Outcome: testCase.outcome},
			}}
			var out bytes.Buffer
			cli := supervisionTestCLI(&out, transport)
			err := cli.run(context.Background(), []string{
				"supervision", "decide", "run-1", "--gate", "gate-1", testCase.flag,
				"--evidence", "snapshot-9", "--expected-revision", "7",
				"--graph-revision", "12", "--activation", "3",
				"--incident", "incident-1",
				"--request-id", "key-1", "--reason", testCase.reason,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(transport.requests) != 1 {
				t.Fatalf("requests = %d, want 1", len(transport.requests))
			}
			sent := transport.requests[0]
			if sent.Operation != backlogadmin.SupervisionDecide || sent.RunID != "run-1" {
				t.Fatalf("request = %+v", sent)
			}
			if sent.RequestKey != "key-1" || sent.Reason != testCase.reason {
				t.Fatalf("audit fields = %+v", sent)
			}
			if sent.ExpectedRevision != 7 || sent.ActivationEpoch != 3 {
				t.Fatalf("fences = %+v", sent)
			}
			if sent.Gate == nil {
				t.Fatal("the decision carried no gate")
			}
			// The prose says the opposite of the flag in both directions, so a
			// parse of the reason could not produce this outcome by accident.
			if sent.Gate.Outcome != testCase.outcome {
				t.Fatalf("outcome = %q, want %q", sent.Gate.Outcome, testCase.outcome)
			}
			if sent.Gate.GateID != "gate-1" || sent.Gate.EvidenceSnapshotID != "snapshot-9" ||
				sent.Gate.ExpectedGraphRevision != 12 || sent.Gate.IncidentID != "incident-1" {
				t.Fatalf("gate decision = %+v", *sent.Gate)
			}
			if err := sent.Validate(); err != nil {
				t.Fatalf("the command sent a request the coordinator would refuse: %v", err)
			}
			if !strings.Contains(out.String(), "gate gate-1") {
				t.Fatalf("decide printed %q", out.String())
			}
		})
	}

	// Naming neither outcome, or both, is refused rather than defaulted.
	for _, flags := range [][]string{{}, {"--accept", "--reject"}} {
		transport := &fakeSupervisionTransport{}
		var out bytes.Buffer
		cli := supervisionTestCLI(&out, transport)
		args := append([]string{
			"supervision", "decide", "run-1", "--gate", "gate-1", "--evidence", "snapshot-9",
			"--expected-revision", "7", "--graph-revision", "12",
			"--request-id", "key-1", "--reason", "because",
		}, flags...)
		err := cli.run(context.Background(), args)
		if err == nil || !strings.Contains(err.Error(), "--accept") {
			t.Fatalf("error = %v", err)
		}
		if len(transport.requests) != 0 {
			t.Fatal("an undecided decision reached the coordinator")
		}
	}
}

// Every mutating verb is audited, and an audit record without a key or a reason
// is a record nobody can act on. Both are refused here rather than after a
// round trip.
func TestCampaignSupervisionRefusesMutationWithoutRequestID(t *testing.T) {
	complete := map[backlogadmin.SupervisionOperation][]string{
		backlogadmin.SupervisionDecide: {
			"--gate", "gate-1", "--accept", "--evidence", "snapshot-9", "--graph-revision", "12",
		},
		backlogadmin.SupervisionHold:     {"--scope", "run"},
		backlogadmin.SupervisionRelease:  {"--hold", "hold-1"},
		backlogadmin.SupervisionEscalate: {"--incident", "incident-1"},
		backlogadmin.SupervisionResolve:  {"--incident", "incident-1", "--outcome", "remediated"},
	}
	for operation, extra := range complete {
		for _, missing := range []string{"--request-id", "--reason"} {
			t.Run(string(operation)+missing, func(t *testing.T) {
				args := []string{"supervision", string(operation), "run-1", "--expected-revision", "7"}
				args = append(args, extra...)
				if missing != "--request-id" {
					args = append(args, "--request-id", "key-1")
				}
				if missing != "--reason" {
					args = append(args, "--reason", "because")
				}
				transport := &fakeSupervisionTransport{}
				var out bytes.Buffer
				cli := supervisionTestCLI(&out, transport)
				err := cli.run(context.Background(), args)
				if err == nil || !strings.Contains(err.Error(), missing) {
					t.Fatalf("error = %v, want one naming %s", err, missing)
				}
				if len(transport.requests) != 0 {
					t.Fatal("an unaudited mutation reached the coordinator")
				}
			})
		}
	}

	// show is not a mutation and needs neither.
	transport := &fakeSupervisionTransport{response: backlogadmin.SupervisionResponse{}}
	var out bytes.Buffer
	cli := supervisionTestCLI(&out, transport)
	if err := cli.run(context.Background(), []string{"supervision", "show", "run-1"}); err != nil {
		t.Fatal(err)
	}
}

// The five refusal classes are what a caller branches on, so each one has to
// arrive as its own named outcome with the exit code its transport class owns.
// stale-evidence and unmet-prerequisite are both refusals on the merits and
// share exit 8; the class word in the message is what separates them.
func TestCampaignSupervisionDistinguishesErrorClasses(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		err       error
		class     backlogadmin.SupervisionErrorClass
		transport backlogadmin.TransportClass
		exitCode  int
	}{
		{
			name:      "stale evidence",
			err:       fmt.Errorf("%w: the gate is at 9", domain.ErrSupervisionStaleRevision),
			class:     backlogadmin.SupervisionErrorStaleEvidence,
			transport: backlogadmin.ClassRejected, exitCode: 8,
		},
		{
			name:      "unauthorized scope",
			err:       fmt.Errorf("%w: not this run", domain.ErrSupervisionUnauthorizedActor),
			class:     backlogadmin.SupervisionErrorUnauthorizedScope,
			transport: backlogadmin.ClassAuthentication, exitCode: 4,
		},
		{
			name:      "unmet prerequisite",
			err:       fmt.Errorf("%w: the producers are not verified", domain.ErrSupervisionPrerequisite),
			class:     backlogadmin.SupervisionErrorPrerequisite,
			transport: backlogadmin.ClassRejected, exitCode: 8,
		},
		{
			name:      "temporarily unavailable",
			err:       fmt.Errorf("%w: no store is bound", backlogadmin.ErrSupervisionUnavailable),
			class:     backlogadmin.SupervisionErrorUnavailable,
			transport: backlogadmin.ClassUnavailable, exitCode: 5,
		},
		{
			name:      "malformed request",
			err:       fmt.Errorf("%w: unknown supervision operation", backlogadmin.ErrInvalidQuery),
			class:     backlogadmin.SupervisionErrorMalformed,
			transport: backlogadmin.ClassProtocol, exitCode: 7,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &fakeSupervisionTransport{err: testCase.err}
			var out bytes.Buffer
			cli := supervisionTestCLI(&out, transport)
			err := cli.run(context.Background(), []string{"supervision", "show", "run-1"})
			if err == nil {
				t.Fatal("a refused supervision read was reported as success")
			}
			if class := backlogadmin.ClassOf(err); class != testCase.transport {
				t.Fatalf("transport class = %q, want %q", class, testCase.transport)
			}
			if code := backlogadmin.ExitCodeFor(err); code != testCase.exitCode {
				t.Fatalf("exit code = %d, want %d", code, testCase.exitCode)
			}
			if !strings.Contains(err.Error(), string(testCase.class)) {
				t.Fatalf("error %q does not name the class %q", err, testCase.class)
			}
			if !errors.Is(err, testCase.err) {
				t.Fatalf("the refusal lost its cause: %v", err)
			}
		})
	}

	// Every class word is distinct, so a caller reading the message can always
	// tell one refusal from another even where two share an exit code.
	seen := map[backlogadmin.SupervisionErrorClass]bool{}
	for _, class := range []backlogadmin.SupervisionErrorClass{
		backlogadmin.SupervisionErrorStaleEvidence,
		backlogadmin.SupervisionErrorUnauthorizedScope,
		backlogadmin.SupervisionErrorPrerequisite,
		backlogadmin.SupervisionErrorUnavailable,
		backlogadmin.SupervisionErrorMalformed,
	} {
		if seen[class] {
			t.Fatalf("class %q is not distinct", class)
		}
		seen[class] = true
	}

	// A CLI with no supervision seam reports the operation as unavailable
	// rather than panicking on a carrier that never promised to supervise.
	var out bytes.Buffer
	cli := campaignCLI{stdout: &out, stderr: &out}
	err := cli.run(context.Background(), []string{"supervision", "show", "run-1"})
	if backlogadmin.ExitCodeFor(err) != 5 {
		t.Fatalf("missing seam exit code = %d, want 5", backlogadmin.ExitCodeFor(err))
	}
	if !strings.Contains(err.Error(), string(backlogadmin.SupervisionErrorUnavailable)) {
		t.Fatalf("missing seam error = %v", err)
	}
}

// A hold scope has exactly two spellings. Anything else is refused here, where
// the caller can still fix the command line, rather than sent for the
// coordinator to reject.
func TestCampaignSupervisionHoldScopeParsing(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		scope string
		want  domain.HoldScope
	}{
		{name: "run", scope: "run", want: domain.HoldScope{Kind: domain.HoldScopeRun}},
		{
			name: "branch", scope: "branch:build",
			want: domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: "build"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &fakeSupervisionTransport{response: backlogadmin.SupervisionResponse{
				Hold: &domain.Hold{ID: "hold-1", Scope: testCase.want, State: domain.HoldActive},
			}}
			var out bytes.Buffer
			cli := supervisionTestCLI(&out, transport)
			err := cli.run(context.Background(), []string{
				"supervision", "hold", "run-1", "--scope", testCase.scope,
				"--expected-revision", "11", "--request-id", "key-1", "--reason", "pausing",
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(transport.requests) != 1 {
				t.Fatalf("requests = %d, want 1", len(transport.requests))
			}
			sent := transport.requests[0]
			if sent.Hold == nil || sent.Hold.Scope != testCase.want {
				t.Fatalf("hold = %+v", sent.Hold)
			}
			if err := sent.Validate(); err != nil {
				t.Fatalf("the command sent a hold the coordinator would refuse: %v", err)
			}
			if !strings.Contains(out.String(), "hold hold-1") {
				t.Fatalf("hold printed %q", out.String())
			}
		})
	}

	for _, scope := range []string{"branch", "branch:", "everything", "run:build"} {
		t.Run("refuses "+scope, func(t *testing.T) {
			transport := &fakeSupervisionTransport{}
			var out bytes.Buffer
			cli := supervisionTestCLI(&out, transport)
			err := cli.run(context.Background(), []string{
				"supervision", "hold", "run-1", "--scope", scope,
				"--expected-revision", "11", "--request-id", "key-1", "--reason", "pausing",
			})
			if err == nil || !strings.Contains(err.Error(), "--scope") {
				t.Fatalf("error = %v", err)
			}
			if len(transport.requests) != 0 {
				t.Fatal("a malformed scope reached the coordinator")
			}
		})
	}
}
