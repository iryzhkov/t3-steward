package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// campaignCancelRunDocument is what the run form of cancel prints. It names
// the tasks the one command covers, because the command's own target is a
// single anchor attempt and an operator reading only that would not know what
// else it took with it.
type campaignCancelRunDocument struct {
	Run string `json:"run"`
	// Tasks is "willCancel" and not "tasks": it is an intention, computed from
	// a read of the run that may already be stale, and nothing has been applied
	// when it is printed -- the command is queued, and the coordinator applies
	// it on its next tick. Under the name "tasks", beside "run", it sits
	// exactly where "task run" prints the tasks that exist, so an automated
	// caller would read it as the outcome. The applied per-task outcome is in
	// "t3-steward backlog commands <run>" and in the audit event.
	Tasks   []string             `json:"willCancel"`
	Anchor  string               `json:"anchorAttempt"`
	Command backlogadmin.Command `json:"command"`
	Event   backlogadmin.Event   `json:"event,omitzero"`
}

// describeCampaignRunDetail reads one run with its tasks and attempts over the
// same admin transport every other live campaign verb uses.
func describeCampaignRunDetail(ctx context.Context, cfg config.Config, runID string) (backlogadmin.WorkflowDetail, error) {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return backlogadmin.WorkflowDetail{}, err
	}
	response, err := transport.client.Query(ctx, backlogadmin.Query{
		Version:       backlogadmin.Version,
		Kind:          backlogadmin.QueryWorkflow,
		Principal:     transport.principal,
		WorkflowRunID: runID,
	})
	if err != nil {
		return backlogadmin.WorkflowDetail{}, err
	}
	if response.Workflow == nil {
		return backlogadmin.WorkflowDetail{}, &backlogadmin.TransportError{
			Class:     backlogadmin.ClassRejected,
			Operation: "workflow",
			Err:       fmt.Errorf("the coordinator knows no run %q", runID),
		}
	}
	return *response.Workflow, nil
}

// isCampaignRunCancel reports the run form: "cancel <run>" with no task.
func isCampaignRunCancel(args []string) bool {
	return len(args) >= 2 && args[0] == "cancel" &&
		strings.TrimSpace(args[1]) != "" && !strings.Contains(args[1], "/") &&
		!strings.HasPrefix(args[1], "-")
}

// runCampaignCancelRun cancels every non-terminal task of one run with one
// command. The task form, "cancel <run>/<task>", is unchanged and still
// forwards to the backlog path.
func (c campaignCLI) runCampaignCancelRun(ctx context.Context, args []string) error {
	runID := args[1]
	reason, commandID, asJSON, err := parseCampaignCancelRunArgs(args[2:])
	if err != nil {
		return err
	}
	if c.detail == nil || c.mutate == nil {
		return errors.New("coordinator admin transport is unavailable")
	}
	if err := c.refuseRunCancelOnAnOlderCoordinator(ctx, runID); err != nil {
		return err
	}
	detail, err := c.detail(ctx, runID)
	if err != nil {
		return err
	}
	attempts := make([]domain.Attempt, 0, len(detail.Tasks))
	names := map[string]string{}
	for _, task := range detail.Tasks {
		if task.Sink != nil || task.Attempt == nil {
			continue
		}
		attempt := *task.Attempt
		attempt.WorkflowRunID = runID
		attempts = append(attempts, attempt)
		names[attempt.ID] = task.Task.Name
	}
	run := detail.Summary.Run
	if run.Progress.Terminal() || run.Sink != nil && run.Sink.Progress.Terminal() {
		return fmt.Errorf("run %s is already settled as %s; there is nothing to cancel (t3-steward campaign show %s)",
			runID, run.Progress, runID)
	}
	// The supervision read is advisory: an unsupervised run, an older
	// coordinator or a refused read leave the cancel exactly as it was, and the
	// coordinator refuses a live overseer again in the transaction that applies
	// the command.
	supervision, supervised := c.cancelSupervisionState(ctx, runID)
	if supervised && (supervision.Activation.State == domain.ActivationPendingDispatch ||
		supervision.Activation.State == domain.ActivationActive) {
		return errors.New(sqlite.RunCancelActivationRefusal(runID, supervision.Activation))
	}
	anchor, ok := backlogadmin.RunCancelAnchorID(attempts, runID)
	var cancelled []string
	var expectedRevision int64
	if !ok {
		// Every task is terminal but the sink is not settled, most often
		// because a supervision incident holds it open. The command closes the
		// run and is fenced on the run's own revision.
		expectedRevision = run.Revision
	}
	for _, attempt := range attempts {
		if attempt.Progress.Terminal() {
			continue
		}
		cancelled = append(cancelled, names[attempt.ID])
		if attempt.ID == anchor {
			expectedRevision = attempt.Revision
		}
	}
	if commandID == "" {
		commandID, err = newAdminCommandID()
		if err != nil {
			return fmt.Errorf("create command id: %w", err)
		}
	}
	response, err := c.mutate(ctx, backlogadmin.Mutation{
		Version: backlogadmin.Version, Principal: backlogadmin.Principal{ID: c.submissionPrincipal()},
		ID: commandID, Kind: domain.AdminCommandCancel, WorkflowRunID: runID,
		ExpectedRevision: expectedRevision, Reason: reason,
		Payload: json.RawMessage(`{"scope":"` + backlogadmin.MutationScopeRun + `"}`),
	})
	if err != nil {
		if !ok && supervised && strings.Contains(err.Error(), "every task of it is already terminal") {
			// A coordinator older than the run-closing cancel. Name the
			// commands that close the run by hand, ids and revisions filled in.
			return fmt.Errorf("%w\n%s", err, campaignCancelByHand(runID, supervision))
		}
		return err
	}
	document := campaignCancelRunDocument{
		Run: runID, Tasks: cancelled, Anchor: anchor,
		Command: response.Command, Event: response.Event,
	}
	if asJSON {
		return encodeCampaignJSON(c.stdout, document)
	}
	if ok {
		fmt.Fprintf(c.stdout, "command %s will cancel %d task(s) of run %s: %s\n",
			response.Command.ID, len(cancelled), runID, campaignList(cancelled))
		fmt.Fprintf(c.stdout, "state %s, fenced on attempt %s at revision %d\n",
			response.Command.State, anchor, expectedRevision)
	} else {
		fmt.Fprintf(c.stdout, "command %s will close run %s: every task is already terminal, so it resolves the run's "+
			"open supervision incidents as cancelled, cancels its undecided gates, releases its holds and settles its sink\n",
			response.Command.ID, runID)
		fmt.Fprintf(c.stdout, "state %s, fenced on run %s at revision %d\n", response.Command.State, runID, expectedRevision)
	}
	if response.Command.TargetType == domain.AdminTargetAttempt && supervised {
		fmt.Fprint(c.stdout, "the same application resolves the run's open supervision incidents as cancelled and releases its holds\n")
	}
	if response.Command.Failure != "" {
		fmt.Fprintf(c.stdout, "failure: %s\n", response.Command.Failure)
	}
	// The list above is what the command covers, not what it has done: it is
	// computed here from a read, and the application happens on the
	// coordinator's next tick.
	_, err = fmt.Fprintf(c.stdout, "the applied outcome, once the coordinator has applied it:\n"+
		"  t3-steward campaign show %s\n  t3-steward backlog commands %s\n", runID, runID)
	return err
}

// cancelSupervisionState reads the run's supervision for the cancel, and
// reports false for an unsupervised run and for any read that fails.
func (c campaignCLI) cancelSupervisionState(ctx context.Context, runID string) (backlogadmin.SupervisionState, bool) {
	if c.superviseAs == nil {
		return backlogadmin.SupervisionState{}, false
	}
	identity, err := resolveSupervisorIdentity("")
	if err != nil {
		return backlogadmin.SupervisionState{}, false
	}
	response, err := c.superviseAs(ctx, identity, backlogadmin.SupervisionRequest{
		Version: backlogadmin.SupervisionVersion, Operation: backlogadmin.SupervisionShow, RunID: runID,
	})
	if err != nil || response.State == nil {
		return backlogadmin.SupervisionState{}, false
	}
	return *response.State, true
}

// campaignCancelByHand is what an operator runs to close a run whose tasks are
// all terminal when the coordinator cannot do it with one cancel: every open
// incident escalated, then every escalated one resolved as cancelled, each
// with the revision it will be at.
func campaignCancelByHand(runID string, state backlogadmin.SupervisionState) string {
	var lines []string
	for _, view := range state.Incidents {
		incident := view.Incident
		revision := incident.Revision
		switch incident.State {
		case domain.IncidentOpen:
			lines = append(lines, fmt.Sprintf("  t3-steward campaign supervision escalate %s --incident %s --expected-revision %d --request-id cancel-escalate-%s-r%d --reason TEXT",
				runID, incident.ID, revision, incident.ID, revision))
			revision++
		case domain.IncidentEscalated:
		default:
			continue
		}
		lines = append(lines, fmt.Sprintf("  t3-steward campaign supervision resolve %s --incident %s --expected-revision %d --outcome cancelled --request-id cancel-%s-r%d --reason TEXT",
			runID, incident.ID, revision, incident.ID, revision))
	}
	if len(lines) == 0 {
		return fmt.Sprintf("this coordinator cannot close the run with one cancel; inspect what holds its sink open: t3-steward campaign supervision show %s", runID)
	}
	return "this coordinator cannot close the run with one cancel; close its supervision by hand, in this order:\n" + strings.Join(lines, "\n")
}

// campaignRunCancelRelease is the first release whose coordinator can apply a
// run-scoped cancel. An older one decodes the request, ignores the scope,
// resolves the empty task to the run itself and then fails at application,
// because its store applies attempt and schedule targets only. The operator
// would get a queued command id and a silent failure some ticks later, so this
// client refuses to send it.
const campaignRunCancelRelease = "v0.11.0-rc.70"

// refuseRunCancelOnAnOlderCoordinator refuses the run form against a
// coordinator that cannot apply it, naming the per-task form instead. A
// release that cannot be read or parsed is not refused -- that would break the
// verb against every build whose release string this rule does not understand
// -- but it is warned about, because a silent non-application is exactly what
// this check exists to prevent.
func (c campaignCLI) refuseRunCancelOnAnOlderCoordinator(ctx context.Context, runID string) error {
	release := ""
	if c.release != nil {
		// A status query that fails is not this command's failure to report:
		// the mutation below reports its own transport trouble in its own words.
		release, _ = c.release(ctx)
	}
	newEnough, known := releaseAtLeast(release, campaignRunCancelRelease)
	switch {
	case known && !newEnough:
		return fmt.Errorf("this coordinator runs %s and cannot apply a run-scoped cancel, which needs %s or newer; "+
			"cancel the tasks one at a time:\n  t3-steward campaign cancel %s/<task> --reason TEXT",
			release, campaignRunCancelRelease, runID)
	case !known:
		fmt.Fprintf(c.stderr, "warning: this coordinator reports no release this client can read (%q), "+
			"so a run-scoped cancel may be accepted and never applied; "+
			"t3-steward campaign cancel %s/<task> is the form every release applies\n", release, runID)
	}
	return nil
}

// releaseAtLeast compares two releases of this project's own form,
// vMAJOR.MINOR.PATCH optionally followed by -rc.N, and reports whether the
// first is at least the second. A release with no candidate suffix is newer
// than every candidate of the same version. The second return value is false
// when either string is not of that form, which is the difference between "an
// older coordinator" and "a release this rule cannot read".
func releaseAtLeast(release, minimum string) (bool, bool) {
	left, leftOK := parseReleaseOrder(release)
	right, rightOK := parseReleaseOrder(minimum)
	if !leftOK || !rightOK {
		return false, false
	}
	for i := range left {
		if left[i] != right[i] {
			return left[i] > right[i], true
		}
	}
	return true, true
}

// parseReleaseOrder turns a release into a comparable tuple: major, minor,
// patch and the candidate number, where a final release sorts above every
// candidate of the same version.
func parseReleaseOrder(release string) ([4]int, bool) {
	value := strings.TrimPrefix(strings.TrimSpace(release), "v")
	if value == "" {
		return [4]int{}, false
	}
	version, candidate, hasCandidate := strings.Cut(value, "-")
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return [4]int{}, false
	}
	var order [4]int
	for i, part := range parts {
		number, err := strconv.Atoi(part)
		if err != nil || number < 0 {
			return [4]int{}, false
		}
		order[i] = number
	}
	order[3] = math.MaxInt
	if hasCandidate {
		digits, ok := strings.CutPrefix(candidate, "rc.")
		if !ok {
			return [4]int{}, false
		}
		number, err := strconv.Atoi(digits)
		if err != nil || number < 0 {
			return [4]int{}, false
		}
		order[3] = number
	}
	return order, true
}

// parseCampaignCancelRunArgs takes the flags of the run form. --reason is
// required, as it is for every mutating verb: a cancellation nobody can
// explain later is what the reason exists to prevent.
func parseCampaignCancelRunArgs(args []string) (string, string, bool, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return "", "", false, err
	}
	reason, commandID := "", ""
	for i := 0; i < len(clean); i++ {
		switch clean[i] {
		case "--reason", "--command-id":
			if i+1 >= len(clean) || strings.TrimSpace(clean[i+1]) == "" {
				return "", "", false, fmt.Errorf("%s needs a value", clean[i])
			}
			if clean[i] == "--reason" {
				reason = clean[i+1]
			} else {
				commandID = clean[i+1]
			}
			i++
		default:
			return "", "", false, fmt.Errorf("campaign cancel usage: cancel <run>[/<task>] --reason TEXT [--command-id ID] [--json] (got %q)", clean[i])
		}
	}
	if reason == "" {
		return "", "", false, errors.New("campaign cancel needs --reason TEXT")
	}
	return reason, commandID, asJSON, nil
}
