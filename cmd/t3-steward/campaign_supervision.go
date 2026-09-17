package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignSupervisionUsage is the detail the campaign usage block has no room
// for. The campaign namespace help is capped so that it can enter an agent's
// context whole, so the supervision family names itself there and carries its
// own flag contract here, reachable as "campaign supervision --help".
const campaignSupervisionUsage = `Usage: t3-steward campaign supervision <verb> <run> [flags]

Supervision is a structured decision surface. Every decision names the record
and the revision it acts on. Prose travels in --reason, which is recorded as
evidence and is never parsed into an outcome.

  show     <run> [--json]
  decide   <run> --gate ID (--accept|--reject) --evidence SNAPSHOT
           --expected-revision N --graph-revision N [--incident ID]
  hold     <run> --scope (run|branch:TASK) --expected-revision N
  release  <run> --hold ID --expected-revision N
  escalate <run> --incident ID --expected-revision N
  resolve  <run> --incident ID --expected-revision N
           --outcome (conclude-failure|remediated|cancelled)
  reassess <run> --expected-revision N

Every mutating verb requires --request-id KEY and --reason TEXT, and accepts
--activation EPOCH. --request-id is the idempotency key: repeating a verb with
the same key and the same payload returns the first answer, and the same key
carrying a different payload is refused rather than answered from the receipt.
--activation names the epoch a supervisor acts under; an operator may omit it.

--expected-revision is the revision of the record the verb targets: the gate's
revision for decide, the supervision record's revision for hold, release and
reassess, and the incident's revision for escalate and resolve. Read all of
them with "t3-steward campaign supervision show <run> --json".

reassess is the operator's re-arming verb, and only an operator may use it. An
overseer that ends its activation without deciding leaves the run waiting: the
coordinator escalates the open review incident and starts no replacement, since
repeating the same review automatically is a spin rather than a decision.
reassess records an operator-reassessment trigger, and the next coordinator
boundary wakes a fresh activation at the next epoch, spending one of the run's
declared activations. It is refused once that budget is exhausted.

--supervisor-credential REFERENCE authenticates as the fleet's supervisor admin
client instead of this host's own coordinator client, which is what a decision
from a worker host needs: the coordinator authorizes supervision by principal
against the activation it recorded, and it knows exactly one supervisor
principal. An overseer thread receives the same value in its environment and
needs no flag. It is accepted on these verbs and on no other command.

--json prints the versioned backlog.admin.supervision/v1 response document.
Read its version field first.

A refusal names its supervision class in the message and exits with the frozen
transport code for that class:
  stale-evidence           8  the revision or evidence snapshot moved first
  unmet-prerequisite       8  the record is not in a state that permits this
  unauthorized-scope       4  this principal may not act on this run or epoch
  temporarily-unavailable  5  the coordinator cannot answer now; retry
  malformed-request        7  the request itself is wrong; repeating it fails
stale-evidence and unmet-prerequisite share exit 8 because both are refusals on
the merits, which is the code that class already owns; the class word in the
message, and in the --json error document, tells them apart.
`

// campaignSupervisionArgs is one parsed supervision command line.
type campaignSupervisionArgs struct {
	run    string
	asJSON bool
	// accept and reject are separate booleans rather than one string, so that
	// naming neither and naming both are both refused here instead of one of
	// them silently becoming a default.
	accept    bool
	reject    bool
	gate      string
	evidence  string
	incident  string
	scope     string
	hold      string
	outcome   string
	requestID string
	reason    string
	// supervisorCredential is the admin credential reference this command
	// authenticates with, overriding this host's own coordinator client. It is
	// accepted on the supervision verbs and on no other command, which is what
	// keeps the strongest credential on a worker host out of "backlog start".
	supervisorCredential string
	expectedRevision     int64
	graphRevision        int64
	activation           int64
}

// campaignSupervisionFlags is the flag vocabulary of one verb. A flag that
// belongs to another verb is refused by name, so that a caller who reaches for
// --outcome on a hold is told which verb owns it rather than told it is
// unknown.
func campaignSupervisionFlags(operation backlogadmin.SupervisionOperation) map[string]bool {
	// Every supervision verb accepts the supervisor credential override,
	// including show: an overseer reads the revisions it is about to decide
	// against under the same identity it decides under.
	allowed := map[string]bool{"--json": true, supervisorCredentialFlag: true}
	if operation.Mutating() {
		for _, flag := range []string{"--expected-revision", "--activation", "--request-id", "--reason"} {
			allowed[flag] = true
		}
	} else {
		allowed["--activation"] = true
	}
	switch operation {
	case backlogadmin.SupervisionDecide:
		for _, flag := range []string{"--gate", "--accept", "--reject", "--evidence", "--graph-revision", "--incident"} {
			allowed[flag] = true
		}
	case backlogadmin.SupervisionHold:
		allowed["--scope"] = true
	case backlogadmin.SupervisionRelease:
		allowed["--hold"] = true
	case backlogadmin.SupervisionEscalate:
		allowed["--incident"] = true
	case backlogadmin.SupervisionResolve:
		allowed["--incident"] = true
		allowed["--outcome"] = true
	case backlogadmin.SupervisionReassess:
		// Reassessment names no record of its own, so it adds no flag beyond the
		// audit fields every mutating verb carries.
	}
	return allowed
}

// campaignSupervisionKnownFlag reports whether a flag belongs to some
// supervision verb.
func campaignSupervisionKnownFlag(flag string) bool {
	for _, operation := range backlogadmin.SupervisionOperations() {
		if campaignSupervisionFlags(operation)[flag] {
			return true
		}
	}
	return false
}

func parseCampaignSupervisionArgs(operation backlogadmin.SupervisionOperation, args []string) (campaignSupervisionArgs, error) {
	var parsed campaignSupervisionArgs
	allowed := campaignSupervisionFlags(operation)
	index := 0
	text := func(flag string, into *string) error {
		if *into != "" {
			return fmt.Errorf("%s may be supplied only once", flag)
		}
		if index+1 >= len(args) || args[index+1] == "" {
			return fmt.Errorf("%s needs one nonempty value", flag)
		}
		index++
		*into = args[index]
		return nil
	}
	number := func(flag string, into *int64) error {
		if *into != 0 {
			return fmt.Errorf("%s may be supplied only once", flag)
		}
		if index+1 >= len(args) {
			return fmt.Errorf("%s needs one whole number above zero", flag)
		}
		index++
		value, err := strconv.ParseInt(args[index], 10, 64)
		if err != nil || value <= 0 {
			return fmt.Errorf("%s needs one whole number above zero, not %q", flag, args[index])
		}
		*into = value
		return nil
	}
	for ; index < len(args); index++ {
		argument := args[index]
		if strings.HasPrefix(argument, "-") && !allowed[argument] {
			if campaignSupervisionKnownFlag(argument) {
				return campaignSupervisionArgs{}, fmt.Errorf(
					"campaign supervision %s does not accept %s", operation, argument)
			}
			return campaignSupervisionArgs{}, fmt.Errorf(
				"unknown campaign supervision %s option %q", operation, argument)
		}
		var err error
		switch argument {
		case "--json":
			if parsed.asJSON {
				err = errors.New("--json may be supplied only once")
			}
			parsed.asJSON = true
		case "--accept":
			if parsed.accept {
				err = errors.New("--accept may be supplied only once")
			}
			parsed.accept = true
		case "--reject":
			if parsed.reject {
				err = errors.New("--reject may be supplied only once")
			}
			parsed.reject = true
		case "--gate":
			err = text(argument, &parsed.gate)
		case "--evidence":
			err = text(argument, &parsed.evidence)
		case "--incident":
			err = text(argument, &parsed.incident)
		case "--scope":
			err = text(argument, &parsed.scope)
		case "--hold":
			err = text(argument, &parsed.hold)
		case "--outcome":
			err = text(argument, &parsed.outcome)
		case "--request-id":
			err = text(argument, &parsed.requestID)
		case "--reason":
			err = text(argument, &parsed.reason)
		case supervisorCredentialFlag:
			err = text(argument, &parsed.supervisorCredential)
		case "--expected-revision":
			err = number(argument, &parsed.expectedRevision)
		case "--graph-revision":
			err = number(argument, &parsed.graphRevision)
		case "--activation":
			err = number(argument, &parsed.activation)
		default:
			if parsed.run != "" {
				err = fmt.Errorf("campaign supervision %s names exactly one run", operation)
			}
			parsed.run = argument
		}
		if err != nil {
			return campaignSupervisionArgs{}, err
		}
	}
	if parsed.run == "" {
		return campaignSupervisionArgs{}, fmt.Errorf("campaign supervision %s needs a run", operation)
	}
	if !operation.Mutating() {
		return parsed, nil
	}
	// The two audit fields are required here rather than left to the
	// coordinator: a decision that reaches the coordinator without them has
	// already cost a round trip to learn something the command line knew.
	if parsed.requestID == "" {
		return campaignSupervisionArgs{}, fmt.Errorf(
			"campaign supervision %s requires --request-id KEY, the idempotency key of the decision", operation)
	}
	if parsed.reason == "" {
		return campaignSupervisionArgs{}, fmt.Errorf(
			"campaign supervision %s requires --reason TEXT, which is recorded on the decision", operation)
	}
	if parsed.expectedRevision == 0 {
		return campaignSupervisionArgs{}, fmt.Errorf(
			"campaign supervision %s requires --expected-revision N, the revision it acts on", operation)
	}
	return parsed, nil
}

// campaignSupervisionScope parses a hold scope. "run" and "branch:TASK" are the
// only two spellings, and anything else is refused here rather than sent for
// the coordinator to reject.
func campaignSupervisionScope(scope string) (domain.HoldScope, error) {
	switch {
	case scope == "run":
		return domain.HoldScope{Kind: domain.HoldScopeRun}, nil
	case strings.HasPrefix(scope, "branch:"):
		root := strings.TrimPrefix(scope, "branch:")
		if strings.TrimSpace(root) != root || root == "" {
			return domain.HoldScope{}, errors.New("--scope branch:TASK needs one root task name after the colon")
		}
		return domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: root}, nil
	default:
		return domain.HoldScope{}, fmt.Errorf(
			"--scope is run or branch:TASK, not %q", scope)
	}
}

// campaignSupervisionRequest turns a parsed command line into the structured
// request. Nothing here reads the reason: an outcome comes from a flag or it
// does not exist.
func campaignSupervisionRequest(operation backlogadmin.SupervisionOperation, parsed campaignSupervisionArgs) (backlogadmin.SupervisionRequest, error) {
	request := backlogadmin.SupervisionRequest{
		Version:         backlogadmin.SupervisionVersion,
		Operation:       operation,
		RunID:           parsed.run,
		ActivationEpoch: parsed.activation,
	}
	if !operation.Mutating() {
		return request, nil
	}
	request.RequestKey = parsed.requestID
	request.Reason = parsed.reason
	request.ExpectedRevision = parsed.expectedRevision
	switch operation {
	case backlogadmin.SupervisionDecide:
		if parsed.accept == parsed.reject {
			return backlogadmin.SupervisionRequest{}, errors.New(
				"campaign supervision decide needs exactly one of --accept and --reject")
		}
		if parsed.gate == "" {
			return backlogadmin.SupervisionRequest{}, errors.New("campaign supervision decide requires --gate ID")
		}
		if parsed.evidence == "" {
			return backlogadmin.SupervisionRequest{}, errors.New(
				"campaign supervision decide requires --evidence ID, the evidence snapshot it reviewed")
		}
		if parsed.graphRevision == 0 {
			return backlogadmin.SupervisionRequest{}, errors.New(
				"campaign supervision decide requires --graph-revision N")
		}
		outcome := domain.GateDecisionReject
		if parsed.accept {
			outcome = domain.GateDecisionAccept
		}
		request.Gate = &backlogadmin.SupervisionGateDecision{
			GateID:                parsed.gate,
			Outcome:               outcome,
			EvidenceSnapshotID:    parsed.evidence,
			ExpectedGraphRevision: parsed.graphRevision,
			IncidentID:            parsed.incident,
		}
	case backlogadmin.SupervisionHold:
		if parsed.scope == "" {
			return backlogadmin.SupervisionRequest{}, errors.New(
				"campaign supervision hold requires --scope run or --scope branch:TASK")
		}
		scope, err := campaignSupervisionScope(parsed.scope)
		if err != nil {
			return backlogadmin.SupervisionRequest{}, err
		}
		request.Hold = &backlogadmin.SupervisionHoldRequest{Scope: scope}
	case backlogadmin.SupervisionRelease:
		if parsed.hold == "" {
			return backlogadmin.SupervisionRequest{}, errors.New("campaign supervision release requires --hold ID")
		}
		request.Release = &backlogadmin.SupervisionReleaseRequest{HoldID: parsed.hold}
	case backlogadmin.SupervisionEscalate:
		if parsed.incident == "" {
			return backlogadmin.SupervisionRequest{}, errors.New("campaign supervision escalate requires --incident ID")
		}
		request.Incident = &backlogadmin.SupervisionIncidentRequest{IncidentID: parsed.incident}
	case backlogadmin.SupervisionResolve:
		if parsed.incident == "" {
			return backlogadmin.SupervisionRequest{}, errors.New("campaign supervision resolve requires --incident ID")
		}
		outcome, err := campaignSupervisionOutcome(parsed.outcome)
		if err != nil {
			return backlogadmin.SupervisionRequest{}, err
		}
		request.Incident = &backlogadmin.SupervisionIncidentRequest{
			IncidentID: parsed.incident,
			Outcome:    outcome,
		}
	}
	return request, nil
}

// campaignSupervisionOutcome maps the three resolution words onto the domain
// outcomes. gate-accepted is absent on purpose: an acceptance closes its
// incident through decide.
func campaignSupervisionOutcome(outcome string) (domain.IncidentOutcome, error) {
	switch domain.IncidentOutcome(outcome) {
	case domain.IncidentOutcomeConcludeFailure:
		return domain.IncidentOutcomeConcludeFailure, nil
	case domain.IncidentOutcomeRemediated:
		return domain.IncidentOutcomeRemediated, nil
	case domain.IncidentOutcomeCancelled:
		return domain.IncidentOutcomeCancelled, nil
	case domain.IncidentOutcomeGateAccepted:
		return "", errors.New(
			"a gate acceptance closes its incident through \"campaign supervision decide --accept\", not resolve")
	default:
		return "", fmt.Errorf(
			"campaign supervision resolve requires --outcome conclude-failure, remediated or cancelled, not %q", outcome)
	}
}

func campaignSupervisionVerbNames() []string {
	names := make([]string, 0, len(backlogadmin.SupervisionOperations()))
	for _, operation := range backlogadmin.SupervisionOperations() {
		names = append(names, string(operation))
	}
	return names
}

func campaignSupervisionVerb(word string) (backlogadmin.SupervisionOperation, bool) {
	for _, operation := range backlogadmin.SupervisionOperations() {
		if string(operation) == word {
			return operation, true
		}
	}
	return "", false
}

func printCampaignSupervisionHelp(out io.Writer) error {
	_, err := fmt.Fprint(out, campaignSupervisionUsage)
	return err
}

// runSupervision is the supervision verb family. It is one command rather than
// six so that the flag vocabulary, the audit requirements and the refusal
// classes are stated once.
func (c campaignCLI) runSupervision(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return printCampaignSupervisionHelp(c.stdout)
	}
	operation, known := campaignSupervisionVerb(args[0])
	if !known {
		return fmt.Errorf("unknown campaign supervision verb %q; try one of %s",
			args[0], strings.Join(campaignSupervisionVerbNames(), ", "))
	}
	parsed, err := parseCampaignSupervisionArgs(operation, args[1:])
	if err != nil {
		return err
	}
	request, err := campaignSupervisionRequest(operation, parsed)
	if err != nil {
		return err
	}
	if c.superviseAs == nil {
		return campaignSupervisionVerdict(operation, fmt.Errorf(
			"%w: no coordinator transport carries supervision here", backlogadmin.ErrSupervisionUnavailable))
	}
	// The flag, then the thread environment, then the record the worker wrote
	// into the activation workspace. Inside an overseer thread the last of the
	// three always exists, which is what stops the CLI falling back to this
	// host's own coordinator client and deciding a gate as an operator.
	identity, err := resolveSupervisorIdentity(parsed.supervisorCredential)
	if err != nil {
		return campaignSupervisionVerdict(operation, err)
	}
	response, err := c.superviseAs(ctx, identity, request)
	if err != nil {
		return campaignSupervisionVerdict(operation, err)
	}
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, response)
	}
	return renderCampaignSupervision(c.stdout, response)
}

// renderCampaignSupervision prints the human form: the whole picture for show,
// and one line naming what changed for every mutating verb.
func renderCampaignSupervision(out io.Writer, response backlogadmin.SupervisionResponse) error {
	if response.Operation == backlogadmin.SupervisionShow {
		if response.State == nil {
			_, err := fmt.Fprintf(out, "run %s reported no supervision state\n", response.RunID)
			return err
		}
		return renderCampaignSupervisionState(out, response.RunID, *response.State)
	}
	replay := ""
	if response.Replay {
		replay = " (replay of the answer this request key already produced)"
	}
	_, err := fmt.Fprintf(out, "campaign supervision %s: %s on run %s%s\n",
		response.Operation, campaignSupervisionChange(response), response.RunID, replay)
	return err
}

// campaignSupervisionChange names what one mutating verb changed.
func campaignSupervisionChange(response backlogadmin.SupervisionResponse) string {
	switch response.Operation {
	case backlogadmin.SupervisionDecide:
		outcome := "decided"
		if response.Decision != nil {
			outcome = string(response.Decision.Outcome)
		}
		return fmt.Sprintf("gate %s is %s after %s", response.GateID, response.GateState, outcome)
	case backlogadmin.SupervisionHold:
		if response.Hold == nil {
			return "a hold was placed"
		}
		return fmt.Sprintf("hold %s is %s over %s",
			response.Hold.ID, response.Hold.State, campaignSupervisionScopeLabel(response.Hold.Scope))
	case backlogadmin.SupervisionRelease:
		if response.Hold == nil {
			return "a hold was released"
		}
		return fmt.Sprintf("hold %s is %s", response.Hold.ID, response.Hold.State)
	case backlogadmin.SupervisionEscalate:
		return fmt.Sprintf("incident %s is %s", response.IncidentID, response.IncidentState)
	case backlogadmin.SupervisionReassess:
		return "a reassessment was requested, so the next coordinator boundary wakes a fresh overseer activation"
	case backlogadmin.SupervisionResolve:
		outcome := ""
		if response.Resolution != nil {
			outcome = fmt.Sprintf(" as %s", response.Resolution.Outcome)
		}
		return fmt.Sprintf("incident %s is %s%s", response.IncidentID, response.IncidentState, outcome)
	default:
		return "nothing named"
	}
}

// supervisionAwaitsOperator reports the one activation outcome that no
// automatic boundary will move: a turn that ended without a decision. The
// coordinator escalates it and deliberately starts no replacement, so the run
// is waiting for an operator's reassessment and every surface that shows
// supervision has to say so.
func supervisionAwaitsOperator(activation domain.Activation) bool {
	return activation.State == domain.ActivationSpent &&
		activation.Outcome == domain.ActivationOutcomeNoDecision
}

func campaignSupervisionScopeLabel(scope domain.HoldScope) string {
	if scope.Kind == domain.HoldScopeBranch {
		return "branch:" + scope.BranchRootTaskID
	}
	return string(scope.Kind)
}

func campaignSupervisionActorLabel(actor domain.Actor) string {
	if actor.Principal == "" {
		return string(actor.Kind)
	}
	return string(actor.Kind) + ":" + actor.Principal
}

func renderCampaignSupervisionState(out io.Writer, runID string, state backlogadmin.SupervisionState) error {
	route := state.Record.Config.Route
	if _, err := fmt.Fprintf(out, "supervision of run %s\n", runID); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  route        %s %s\n",
		campaignSupervisionValue(route.ProviderInstanceID), campaignSupervisionValue(route.Model)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  epoch        %d\n", state.Record.ActivationEpoch); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  activations  %d used of %d granted\n",
		state.Record.ActivationsUsed, state.Record.BudgetGrantedActivations); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "  activation   %s %s (epoch %d, %d turns)\n",
		campaignSupervisionValue(state.Activation.ID), campaignSupervisionValue(string(state.Activation.State)),
		state.Activation.Epoch, state.Activation.TurnsUsed); err != nil {
		return err
	}
	// A review that ended without deciding is the one activation outcome that
	// leaves the run waiting for a person rather than for the coordinator, and
	// nothing else on this page says so: the activation reads as spent, the gate
	// as ready for review and the incident as escalated, which is what a run
	// waiting for its overseer looks like too.
	if supervisionAwaitsOperator(state.Activation) {
		if _, err := fmt.Fprintf(out,
			"  waiting      the overseer ended activation %s without a decision; this run waits for an operator to run "+
				"\"t3-steward campaign supervision reassess %s --request-id KEY --reason TEXT\"\n",
			campaignSupervisionValue(state.Activation.ID), runID); err != nil {
			return err
		}
	}
	if state.Activation.OperatorDecisions > 0 {
		if _, err := fmt.Fprintf(out,
			"  operator     %d decision(s) at this activation's epoch were recorded by an operator, not by the overseer\n",
			state.Activation.OperatorDecisions); err != nil {
			return err
		}
	}
	// Route availability is printed before the records, because a gate that is
	// not moving is most often not moving for a reason no gate, hold or incident
	// mentions, and the operator reading show is asking exactly that.
	if state.RouteAvailable {
		if _, err := fmt.Fprint(out, "  supervisor   an overseer can be dispatched for this run\n"); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(out, "  supervisor   no overseer can be dispatched: %s\n",
			campaignSupervisionValue(state.RouteBlockReason)); err != nil {
			return err
		}
	}
	if state.SinkSettled {
		if _, err := fmt.Fprint(out, "  settled      the run is settled; supervision is closed\n"); err != nil {
			return err
		}
	}
	return renderCampaignSupervisionRecords(out, state)
}

func renderCampaignSupervisionRecords(out io.Writer, state backlogadmin.SupervisionState) error {
	for _, view := range state.Gates {
		final := ""
		if view.Gate.Definition.Final {
			final = "  final"
		}
		if _, err := fmt.Fprintf(out, "  gate %s %q %s  revision %d  graph revision %d  evidence %s%s\n",
			view.Gate.Definition.ID, view.Gate.Definition.Name, view.Gate.State,
			view.Gate.Revision, view.Gate.GraphRevision,
			campaignSupervisionValue(view.Gate.EvidenceSnapshotID), final); err != nil {
			return err
		}
		// Who decided a gate is a separate question from what the gate became.
		// An overseer deciding as itself and an operator deciding while that
		// overseer was live leave the same accepted gate, and only the actor on
		// the decision tells the two apart.
		if view.LastDecision != nil {
			if _, err := fmt.Fprintf(out, "    decided %s by %s at %s\n",
				view.LastDecision.Outcome, campaignSupervisionActorLabel(view.LastDecision.Actor),
				view.LastDecision.DecidedAt.UTC().Format(time.RFC3339)); err != nil {
				return err
			}
		}
	}
	for _, hold := range state.Holds {
		if _, err := fmt.Fprintf(out, "  hold %s %s  owner %s  %s  reason %q\n",
			hold.ID, campaignSupervisionScopeLabel(hold.Scope),
			campaignSupervisionActorLabel(hold.Owner), hold.State, hold.Reason); err != nil {
			return err
		}
	}
	for _, view := range state.Incidents {
		if _, err := fmt.Fprintf(out, "  incident %s %s  gate %s  requires %s  reason %q\n",
			view.Incident.ID, view.Incident.State, campaignSupervisionValue(view.Incident.GateID),
			view.Incident.RequiredDisposition, view.Incident.Reason); err != nil {
			return err
		}
	}
	return nil
}

// campaignSupervisionValue renders an absent value as a word rather than as an
// empty column, because a blank column reads as a value nobody printed.
func campaignSupervisionValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "(none)"
	}
	return value
}

// supervisionAppendix renders the supervised status of a run after "campaign
// show" or "campaign explain" has printed the backlog admin answer.
//
// It is deliberately silent about every failure. The two verbs are forwarded
// verbatim to an existing command, and an unsupervised run, an older
// coordinator and a supervision read that was refused must all leave that
// command's output and its exit code exactly as they were. A caller who wants
// the supervision state as a result, with its own refusal classes, asks for it
// with "campaign supervision show".
func (c campaignCLI) supervisionAppendix(ctx context.Context, args []string) {
	if c.superviseAs == nil {
		return
	}
	for _, argument := range args {
		// A machine-readable answer stays exactly one document. Appending human
		// text after it would make the document unparseable.
		if argument == "--json" || argument == "--dot" {
			return
		}
	}
	run := campaignSupervisionRunOf(args)
	if run == "" {
		return
	}
	// The appendix runs under whatever identity the thread already has, which is
	// the supervisor client inside an overseer thread and this host's own admin
	// client anywhere else. It stays silent about every failure, so an
	// unreadable record leaves the forwarded command's output untouched.
	identity, identityErr := resolveSupervisorIdentity("")
	if identityErr != nil {
		return
	}
	response, err := c.superviseAs(ctx, identity, backlogadmin.SupervisionRequest{
		Version:   backlogadmin.SupervisionVersion,
		Operation: backlogadmin.SupervisionShow,
		RunID:     run,
	})
	if err != nil || response.State == nil {
		return
	}
	state := *response.State
	if _, err := fmt.Fprintf(c.stdout, "supervised: %s, epoch %d, %d of %d activations used\n",
		campaignSupervisionValue(string(state.Activation.State)), state.Record.ActivationEpoch,
		state.Record.ActivationsUsed, state.Record.BudgetGrantedActivations); err != nil {
		return
	}
	if supervisionAwaitsOperator(state.Activation) {
		if _, err := fmt.Fprintf(c.stdout,
			"  the overseer ended activation %s without a decision; this run waits for an operator to run "+
				"\"t3-steward campaign supervision reassess %s\"\n",
			campaignSupervisionValue(state.Activation.ID), run); err != nil {
			return
		}
	}
	for _, view := range state.Gates {
		if _, err := fmt.Fprintf(c.stdout, "  gate %s %q %s  revision %d\n",
			view.Gate.Definition.ID, view.Gate.Definition.Name, view.Gate.State, view.Gate.Revision); err != nil {
			return
		}
	}
	for _, view := range state.Incidents {
		if view.Incident.State == domain.IncidentResolved {
			continue
		}
		if _, err := fmt.Fprintf(c.stdout, "  incident %s %s  requires %s  reason %q\n",
			view.Incident.ID, view.Incident.State, view.Incident.RequiredDisposition,
			view.Incident.Reason); err != nil {
			return
		}
	}
}

// campaignSupervisionRunOf finds the run in a forwarded lifecycle command line.
// explain names <run>/<task>, so the run is what precedes the first slash.
func campaignSupervisionRunOf(args []string) string {
	for _, argument := range args[1:] {
		if strings.HasPrefix(argument, "-") {
			continue
		}
		run, _, _ := strings.Cut(argument, "/")
		return run
	}
	return ""
}
