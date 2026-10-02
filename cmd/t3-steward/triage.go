package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

const triageUsage = `Usage: t3-steward triage [--stale-days N] [--json]

Everything on the fleet that is waiting for an operator, in one list, each
item with commands that can be run as printed: ids, revisions and idempotency
keys are filled in. It is read-only: it asks the coordinator, and changes
nothing anywhere.

What it lists, items needing action first:

  worker-down           an enrolled worker not connected for longer than
                        notifications.worker_down_after (default 10m)
  worker-disconnected   one inside that grace period (a note)
  worker-maintenance    one drained with accept_backlog: false (a note)
  supervision-reassess  an overseer ended its activation without deciding,
                        and the run waits for "supervision reassess"
  supervision-incident  an escalated review incident, with one resolve
                        command per outcome it permits
  supervision-gate      an escalated gate, with the accept and the reject
  supervision-hold      an active hold (on a settled run it cannot be
                        released, and is listed so it is not mistaken for
                        live work)
  supervision-dispatch  an overseer dispatch that failed
  needs-input           a task's question awaiting an answer: an open ask,
                        with one "t3-steward ask answer" command per option,
                        or an attention request
  ask-unanswered        an ask that reached its deadline with no answer and
                        no default in the last 7 days; its task was told to
                        end failed
  wake-overdue          a wake settled for more than 10 minutes that nothing
                        has claimed, or a wait past its deadline that the
                        coordinator has not settled
  wake-undeliverable    a settled wake stuck offline, busy, held or with an
                        unknown send outcome
  quota-held            a quota pool whose admission is closed, draining or
                        recovering
  intake-quarantined    a submission the coordinator refused permanently
  run-stalled           a run that is not finished and whose record has not
                        changed for --stale-days days

Every run that is not settled has its supervision read; settled runs are read
newest first up to 100, and any left out are named on an INCOMPLETE line. An
incomplete view, or one with a source NOT READ, never says that nothing needs
an operator.

A command that has to run on another host is printed as "on HOST: COMMAND".
Where an item offers several commands that change state, the comment after
each says when to choose it; nothing else is left to fill in. Supervision
commands carry a --reason that says triage proposed them; replace it with your
own reason if you have one.

Flags:
  --stale-days N   days without a change before a run is listed (default 7)
  --json           print one versioned document, t3-steward.triage/v1, with
                   the same items: kind, severity (action, warn, info),
                   subject, run, summary, since and commands (run, host,
                   when)
  --config PATH    configuration file (default
                   $XDG_CONFIG_HOME/t3-steward/config.yaml); the dispatcher
                   removes it before triage sees its arguments, and the path
                   is a separate word

Exit codes:
  0   every source was read; the list may be empty or long
  1   a bad option, or a source failed with no transport class
  3-7 a source could not be read, with the transport class of the first
      failure (see "t3-steward help"); the items of the sources that were
      read are still printed, and the ones that were not are named
`

// triageVersion is the schema of the --json document.
const triageVersion = "t3-steward.triage/v1"

// triageWakeOverdueAfter is how long a settled wake may wait for its delivery
// before triage lists it. The delivery loop runs every 15 seconds and backs
// off to a minute, so ten minutes is a wake nothing is delivering.
const triageWakeOverdueAfter = 10 * time.Minute

// triageSupervisionLimit bounds the settled supervised runs read, most
// recently changed first: one supervision read per run, and a coordinator that
// has supervised hundreds of runs keeps its history in the store. Every run
// that is not settled is read, whatever its age; a settled run left out makes
// the view incomplete, and triage says so.
const triageSupervisionLimit = 100

// triageSources are the coordinator answers triage reads, as seams: the
// command binds them to the admin transport and a test to fixtures.
type triageSources struct {
	query     fleetQuery
	nodeWait  func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error)
	supervise func(context.Context, backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error)
}

type triageOptions struct {
	staleAfter      time.Duration
	workerDownAfter time.Duration
	asJSON          bool
}

// triageCommand is one command an item offers.
type triageCommand struct {
	// Run is the command line, ready to run.
	Run string `json:"run"`
	// Host is where to run it, when that is not here: a worker's own
	// service, or the steward that delivers a host's wakes.
	Host string `json:"host,omitempty"`
	// When says which of several state-changing commands to choose.
	When string `json:"when,omitempty"`
}

type triageItem struct {
	Kind string `json:"kind"`
	// Severity is action (nothing moves until someone acts), warn (something
	// is wrong and may need acting on) or info (worth knowing).
	Severity string          `json:"severity"`
	Subject  string          `json:"subject"`
	Run      string          `json:"run,omitempty"`
	Summary  string          `json:"summary"`
	Since    *time.Time      `json:"since,omitempty"`
	Commands []triageCommand `json:"commands"`
}

type triageReport struct {
	Version         string       `json:"version"`
	GeneratedAt     time.Time    `json:"generatedAt"`
	Coordinator     string       `json:"coordinator,omitempty"`
	StaleAfterDays  int          `json:"staleAfterDays"`
	WorkerDownAfter string       `json:"workerDownAfter"`
	EnrolledWorkers int          `json:"enrolledWorkers"`
	Items           []triageItem `json:"items"`
	// Sources are the coordinator views that answered, and Unavailable the
	// ones that did not, each with why: an empty section is never "nothing
	// is waiting" unless its source is in Sources.
	Sources     []string `json:"sources"`
	Unavailable []string `json:"unavailable,omitempty"`
	// Incomplete names what a source that answered did not cover, because
	// triage bounds how much of it it reads. An incomplete view never says
	// that nothing needs an operator.
	Incomplete []string `json:"incomplete,omitempty"`
	causes     []error
}

func cmdTriage(g globalFlags, args []string) error {
	if answered, err := admitHelp(os.Stdout, []string{"triage"}, args); answered || err != nil {
		return err
	}
	options, err := parseTriageArgs(args)
	if err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	options.workerDownAfter = cfg.Notifications.WorkerDownAfter.D()
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	sources := triageSources{
		query: func(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
			query.Version = backlogadmin.Version
			query.Principal = transport.principal
			return transport.client.Query(ctx, query)
		},
		nodeWait: transport.client.NodeWait,
		supervise: func(ctx context.Context, request backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error) {
			carrier, ok := transport.client.(backlogadmin.SupervisionTransport)
			if !ok {
				return backlogadmin.SupervisionResponse{}, fmt.Errorf(
					"%w: this coordinator client carries no supervision", backlogadmin.ErrSupervisionUnavailable)
			}
			return carrier.Supervise(ctx, request)
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return runTriage(ctx, sources, options, os.Stdout)
}

// parseTriageArgs takes --stale-days and --json, and refuses anything else by
// name rather than ignoring it.
func parseTriageArgs(args []string) (triageOptions, error) {
	clean, asJSON, err := takeJSONFlag(args)
	if err != nil {
		return triageOptions{}, err
	}
	options := triageOptions{staleAfter: 7 * 24 * time.Hour, asJSON: asJSON}
	for i := 0; i < len(clean); i++ {
		switch clean[i] {
		case "--stale-days":
			if i+1 >= len(clean) {
				return triageOptions{}, errors.New("--stale-days needs a whole number of days, for example --stale-days 7")
			}
			days, err := strconv.Atoi(clean[i+1])
			if err != nil || days < 1 {
				return triageOptions{}, fmt.Errorf("--stale-days needs a whole number of days of at least 1, not %q; for example --stale-days 7", clean[i+1])
			}
			options.staleAfter = time.Duration(days) * 24 * time.Hour
			i++
		default:
			return triageOptions{}, fmt.Errorf("triage usage: t3-steward triage [--stale-days N] [--json] (got %q); it takes no run or worker, it lists the whole fleet", clean[i])
		}
	}
	return options, nil
}

// runTriage collects, prints, and reports a source it could not read as the
// exit status, with that source's transport class.
func runTriage(ctx context.Context, sources triageSources, options triageOptions, out io.Writer) error {
	report := collectTriage(ctx, sources, options)
	if options.asJSON {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return err
		}
	} else if err := renderTriage(out, report); err != nil {
		return err
	}
	if len(report.Unavailable) == 0 {
		return nil
	}
	incomplete := incompleteWaitListError{message: "this triage is incomplete: " + strings.Join(report.Unavailable, "; ") +
		"; the items above are from the sources that answered. Run t3-steward triage again once the coordinator answers"}
	for _, cause := range report.causes {
		if backlogadmin.ClassOf(cause) != "" {
			incomplete.cause = cause
			break
		}
	}
	return incomplete
}

func (r *triageReport) unavailable(source string, err error) {
	r.Unavailable = append(r.Unavailable, source+": "+err.Error())
	r.causes = append(r.causes, err)
}

func (r *triageReport) add(item triageItem) { r.Items = append(r.Items, item) }

func collectTriage(ctx context.Context, sources triageSources, options triageOptions) triageReport {
	if options.staleAfter <= 0 {
		options.staleAfter = 7 * 24 * time.Hour
	}
	if options.workerDownAfter <= 0 {
		options.workerDownAfter = backlogadmin.DefaultWorkerDownAfter
	}
	report := triageReport{
		Version: triageVersion, Items: []triageItem{}, Sources: []string{},
		StaleAfterDays: int(options.staleAfter / (24 * time.Hour)), WorkerDownAfter: options.workerDownAfter.String(),
	}
	var notBefore time.Time
	if status, err := sources.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryStatus}); err != nil {
		// Without it an outage is measured from the worker's last sighting
		// alone, which can call a worker down right after a coordinator
		// restart; the reader has to know that.
		report.unavailable("coordinator status", err)
	} else if status.Status != nil {
		report.Sources = append(report.Sources, "coordinator status")
		notBefore = status.Status.Runtime.LastReload
		report.Coordinator = status.Status.Runtime.Owner
		report.GeneratedAt = status.GeneratedAt
	}
	if workers, err := sources.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkers}); err != nil {
		report.unavailable("workers", err)
	} else {
		report.Sources = append(report.Sources, "workers")
		if report.GeneratedAt.IsZero() {
			report.GeneratedAt = workers.GeneratedAt
		}
		triageWorkers(&report, workers, notBefore, options.workerDownAfter)
	}
	if report.GeneratedAt.IsZero() {
		report.GeneratedAt = time.Now().UTC()
	}
	now := report.GeneratedAt
	if quotas, err := sources.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryQuota}); err != nil {
		report.unavailable("quota pools", err)
	} else {
		report.Sources = append(report.Sources, "quota pools")
		triageQuotas(&report, quotas.Quotas)
	}
	if workflows, err := sources.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkflows}); err != nil {
		report.unavailable("runs", err)
	} else {
		report.Sources = append(report.Sources, "runs")
		triageStalledRuns(&report, workflows.Workflows, now, options.staleAfter)
		triageSupervision(ctx, &report, sources, workflows.Workflows)
	}
	triageWaits(ctx, &report, sources, now)
	if quarantine, err := sources.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryQuarantine}); err != nil {
		report.unavailable("intake quarantine", err)
	} else {
		report.Sources = append(report.Sources, "intake quarantine")
		triageQuarantine(&report, quarantine.Quarantine)
	}
	sortTriage(report.Items)
	return report
}

// triageQuarantine reports the refused intake as one item. A coordinator that
// ran the legacy file queue can hold hundreds of these, all with the same
// cause, and one line per file buried everything else; "backlog quarantine"
// lists them, each with its own release command.
func triageQuarantine(report *triageReport, intake []backlogadmin.QuarantinedIntake) {
	if len(intake) == 0 {
		return
	}
	newest := intake[0]
	for _, entry := range intake[1:] {
		if entry.QuarantinedAt.After(newest.QuarantinedAt) {
			newest = entry
		}
	}
	since := newest.QuarantinedAt
	noun := "submissions"
	if len(intake) == 1 {
		noun = "submission"
	}
	report.add(triageItem{
		Kind: "intake-quarantined", Severity: "warn", Subject: fmt.Sprintf("%d %s", len(intake), noun), Since: &since,
		Summary:  fmt.Sprintf("newest %s, refused permanently: %s; %s", newest.Key, newest.Reason, backlogadmin.QuarantineRetryAdvice),
		Commands: []triageCommand{{Run: "t3-steward backlog quarantine"}},
	})
}

func triageWorkers(report *triageReport, workers backlogadmin.Response, notBefore time.Time, after time.Duration) {
	for _, worker := range workers.Workers {
		if worker.Requirement != nil && worker.Enrollment != nil {
			report.EnrolledWorkers++
		}
	}
	outages := backlogadmin.WorkerOutages(workers.Workers, workers.GeneratedAt, notBefore)
	down := map[string]bool{}
	for _, outage := range backlogadmin.WorkersDown(outages, after) {
		down[outage.WorkerID] = true
	}
	for _, outage := range outages {
		since := outage.Since
		seen := "never seen since it was enrolled"
		if !outage.LastSeen.IsZero() {
			seen = "last seen " + outage.LastSeen.UTC().Format(time.RFC3339)
		}
		list := triageCommand{Run: "t3-steward worker list"}
		switch {
		case down[outage.WorkerID]:
			report.add(triageItem{
				Kind: "worker-down", Severity: "action", Subject: outage.WorkerID, Since: &since,
				Summary: fmt.Sprintf("not connected to the coordinator for %s (%s); nothing is dispatched to it", humanDuration(outage.DownFor), seen),
				Commands: []triageCommand{
					{Host: outage.WorkerID, Run: "systemctl --user status t3-steward-worker"},
					{Host: outage.WorkerID, Run: "journalctl --user -u t3-steward-worker -n 50"},
					{Host: outage.WorkerID, Run: "systemctl --user restart t3-steward-worker"},
					list,
				},
			})
		case outage.Maintenance:
			report.add(triageItem{
				Kind: "worker-maintenance", Severity: "info", Subject: outage.WorkerID, Since: &since,
				Summary:  "not connected, and drained (accept_backlog: false or connection removed in the coordinator's worker entry), so not alerted on; " + seen,
				Commands: []triageCommand{list},
			})
		default:
			report.add(triageItem{
				Kind: "worker-disconnected", Severity: "info", Subject: outage.WorkerID, Since: &since,
				Summary:  fmt.Sprintf("not connected for %s (%s); it counts as down after %s", humanDuration(outage.DownFor), seen, humanDuration(after)),
				Commands: []triageCommand{list},
			})
		}
	}
}

func triageQuotas(report *triageReport, quotas []backlogadmin.Quota) {
	for _, quota := range quotas {
		if quota.Admission == nil {
			continue
		}
		switch quota.Admission.Admission {
		case domain.AdmissionClosed, domain.AdmissionDraining, domain.AdmissionRecovering:
		default:
			continue
		}
		since := quota.Admission.AppliedAt
		summary := fmt.Sprintf("admission is %s", quota.Admission.Admission)
		if quota.Admission.Reason != "" {
			summary += ": " + quota.Admission.Reason
		}
		report.add(triageItem{
			Kind: "quota-held", Severity: "warn", Subject: quota.Pool.ID, Since: &since,
			Summary: summary + "; tasks on this pool wait until it reopens",
			Commands: []triageCommand{
				{Run: "t3-steward models"},
				{Run: "t3-steward backlog list --progress paused,ready"},
			},
		})
	}
}

func triageStalledRuns(report *triageReport, runs []backlogadmin.WorkflowSummary, now time.Time, staleAfter time.Duration) {
	for _, summary := range runs {
		run := summary.Run
		if run.Progress.Terminal() || now.Sub(run.UpdatedAt) <= staleAfter {
			continue
		}
		since := run.UpdatedAt
		name := summary.Workflow.Name
		if name == "" {
			name = run.WorkflowID
		}
		report.add(triageItem{
			Kind: "run-stalled", Severity: "warn", Subject: run.ID, Run: run.ID, Since: &since,
			Summary: fmt.Sprintf("campaign %q is %s and its record has not changed for %d days (since %s)",
				name, run.Progress, int(now.Sub(run.UpdatedAt).Hours()/24), run.UpdatedAt.UTC().Format(time.RFC3339)),
			Commands: []triageCommand{
				{Run: "t3-steward campaign show " + run.ID},
				{Run: "t3-steward diagnose " + run.ID},
				{Run: "t3-steward campaign cancel " + run.ID + " --reason " +
					shellQuote("no progress since "+run.UpdatedAt.UTC().Format("2006-01-02")+"; cancelled from triage") +
					fmt.Sprintf(" --command-id triage-cancel-%s-r%d", run.ID, run.Revision),
					When: "if nobody needs it any more"},
			},
		})
	}
}

// triageSupervision reads the supervision of every supervised run, most
// recently changed first, and lists what waits for an operator. A settled
// run's supervision is closed, so only a hold left active on one is listed.
func triageSupervision(ctx context.Context, report *triageReport, sources triageSources, runs []backlogadmin.WorkflowSummary) {
	var supervised []domain.WorkflowRun
	for _, summary := range runs {
		if summary.Run.Supervision != nil {
			supervised = append(supervised, summary.Run)
		}
	}
	if len(supervised) == 0 {
		report.Sources = append(report.Sources, "supervision")
		return
	}
	sort.SliceStable(supervised, func(i, j int) bool { return supervised[i].UpdatedAt.After(supervised[j].UpdatedAt) })
	// Every live run, then the newest settled ones up to the bound.
	var read []domain.WorkflowRun
	settled, skipped := 0, 0
	for _, run := range supervised {
		if !run.Progress.Terminal() {
			read = append(read, run)
			continue
		}
		if settled < triageSupervisionLimit {
			settled++
			read = append(read, run)
			continue
		}
		skipped++
	}
	if skipped != 0 {
		report.Incomplete = append(report.Incomplete, fmt.Sprintf(
			"supervision of %d settled runs was not read (the newest %d were, and every live one); a hold left on one of them is not listed. "+
				"t3-steward campaign supervision show <run> reads one", skipped, triageSupervisionLimit))
	}
	supervised = read
	var failed error
	failures := 0
	for _, run := range supervised {
		response, err := sources.supervise(ctx, backlogadmin.SupervisionRequest{
			Version: backlogadmin.SupervisionVersion, Operation: backlogadmin.SupervisionShow, RunID: run.ID,
		})
		if err != nil {
			if failed == nil {
				failed = err
			}
			failures++
			continue
		}
		if response.State != nil {
			triageSupervisionState(report, run.ID, *response.State)
		}
	}
	if failed != nil {
		report.unavailable("supervision", fmt.Errorf("%d of %d supervised runs could not be read, the first with: %w", failures, len(supervised), failed))
		return
	}
	report.Sources = append(report.Sources, "supervision")
}

func triageSupervisionState(report *triageReport, runID string, state backlogadmin.SupervisionState) {
	show := triageCommand{Run: "t3-steward campaign supervision show " + runID}
	reason := func(text string) string { return " --reason " + shellQuote(text) }
	for _, hold := range state.Holds {
		if hold.State != domain.HoldActive {
			continue
		}
		since := hold.CreatedAt
		item := triageItem{Kind: "supervision-hold", Severity: "warn", Subject: runID, Run: runID, Since: &since}
		if state.SinkSettled {
			item.Severity = "info"
			item.Summary = fmt.Sprintf("hold %s over %s is still recorded active on a settled run; supervision is closed, so it holds nothing and cannot be released",
				hold.ID, campaignSupervisionScopeLabel(hold.Scope))
			item.Commands = []triageCommand{show}
		} else {
			item.Summary = fmt.Sprintf("hold %s over %s by %s keeps its tasks from starting: %s",
				hold.ID, campaignSupervisionScopeLabel(hold.Scope), campaignSupervisionActorLabel(hold.Owner), hold.Reason)
			item.Commands = []triageCommand{show, {
				Run: fmt.Sprintf("t3-steward campaign supervision release %s --hold %s --expected-revision %d --request-id triage-release-%s-r%d",
					runID, hold.ID, state.Record.Revision, hold.ID, state.Record.Revision) + reason("released from triage"),
				When: "once what it was holding for is done",
			}}
		}
		report.add(item)
	}
	if state.SinkSettled {
		return
	}
	if supervisionAwaitsOperator(state.Activation) {
		item := triageItem{Kind: "supervision-reassess", Severity: "action", Subject: runID, Run: runID,
			Summary: fmt.Sprintf("the overseer ended activation %s without a decision; the run waits for an operator", state.Activation.ID)}
		if state.Record.ActivationBudgetRemaining() {
			item.Commands = []triageCommand{show, {
				Run: fmt.Sprintf("t3-steward campaign supervision reassess %s --expected-revision %d --request-id triage-reassess-%s-r%d",
					runID, state.Record.Revision, runID, state.Record.Revision) + reason("operator reassessment requested from triage"),
				When: fmt.Sprintf("to wake a fresh overseer (%d of %d activations used)", state.Record.ActivationsUsed, state.Record.BudgetGrantedActivations),
			}}
		} else {
			item.Summary += fmt.Sprintf("; all %d activations are spent, so reassess is refused: resolve the incident below or decide the gate yourself",
				state.Record.BudgetGrantedActivations)
			item.Commands = []triageCommand{show}
		}
		report.add(item)
	}
	for _, view := range state.Incidents {
		incident := view.Incident
		if incident.State != domain.IncidentEscalated {
			continue
		}
		since := incident.OpenedAt
		resolve := func(outcome domain.IncidentOutcome, when string) triageCommand {
			return triageCommand{
				Run: fmt.Sprintf("t3-steward campaign supervision resolve %s --incident %s --expected-revision %d --outcome %s --request-id triage-resolve-%s-r%d-%s",
					runID, incident.ID, incident.Revision, outcome, incident.ID, incident.Revision, outcome) + reason("resolved from triage"),
				When: when,
			}
		}
		commands := []triageCommand{show,
			resolve(domain.IncidentOutcomeRemediated, "if the cause was fixed and the run should go on"),
			resolve(domain.IncidentOutcomeCancelled, "to give up on what the incident blocks"),
		}
		if view.SourceTaskTerminallyFailed {
			commands = append(commands, resolve(domain.IncidentOutcomeConcludeFailure, "to record the failed task as the outcome"))
		}
		report.add(triageItem{
			Kind: "supervision-incident", Severity: "action", Subject: incident.ID, Run: runID, Since: &since,
			Summary:  fmt.Sprintf("review incident %s of run %s is escalated to an operator: %s", incident.ID, runID, incident.Reason),
			Commands: commands,
		})
	}
	for _, view := range state.Gates {
		gate := view.Gate
		if gate.State != domain.GateEscalated {
			continue
		}
		decide := func(flag, when string) triageCommand {
			return triageCommand{
				Run: fmt.Sprintf("t3-steward campaign supervision decide %s --gate %s %s --evidence %s --expected-revision %d --graph-revision %d --request-id triage-decide-%s-r%d%s",
					runID, gate.Definition.ID, flag, gate.EvidenceSnapshotID, gate.Revision, gate.GraphRevision, gate.Definition.ID, gate.Revision, strings.TrimPrefix(flag, "-")) +
					reason("decided from triage after reviewing evidence "+gate.EvidenceSnapshotID),
				When: when,
			}
		}
		report.add(triageItem{
			Kind: "supervision-gate", Severity: "action", Subject: gate.Definition.ID, Run: runID,
			Summary: fmt.Sprintf("gate %s %q of run %s is escalated to an operator, with evidence %s", gate.Definition.ID, gate.Definition.Name, runID, gate.EvidenceSnapshotID),
			Commands: []triageCommand{show,
				decide("--accept", "if the evidence is good"),
				decide("--reject", "if it is not"),
			},
		})
	}
	for _, failure := range state.DispatchFailures {
		report.add(triageItem{
			Kind: "supervision-dispatch", Severity: "warn", Subject: runID, Run: runID,
			Summary:  fmt.Sprintf("an overseer dispatch failed (%s): %s; next action: %s", failure.Code, failure.SafeMessage, failure.NextAction),
			Commands: []triageCommand{show},
		})
	}
}

// triageWaits lists the questions waiting for an answer and the wakes that
// are not being delivered.
func triageWaits(ctx context.Context, report *triageReport, sources triageSources, now time.Time) {
	nodes, err := sources.nodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "list"})
	if err != nil {
		report.unavailable("waits", err)
		return
	}
	tasks, err := sources.nodeWait(ctx, backlogadmin.NodeWaitOperation{Action: "list-task"})
	if err != nil {
		report.unavailable("waits", err)
		return
	}
	report.Sources = append(report.Sources, "waits")
	for _, w := range nodes.Waits {
		triageNodeWait(report, w, now)
	}
	for _, w := range tasks.TaskWaits {
		triageTaskWait(report, w, now)
	}
}

func triageNodeWait(report *triageReport, w domain.NodeWait, now time.Time) {
	switch w.Delivery {
	case "delivered", "rejected", "cancelled":
		return
	}
	id, host, thread := w.Request.ID, w.Host, w.Request.ThreadID
	inspect := triageCommand{Run: fmt.Sprintf("t3-steward wait list --native --host %s --thread %s", host, thread)}
	cancel := triageCommand{Run: "t3-steward wait cancel " + id, When: "if the thread no longer needs the wake"}
	// A wake whose send may already have happened (sending, or
	// recovery-required) cannot be cancelled: the store refuses it, because
	// cancelling would not take back a message that may be in the thread.
	cancellable := w.Delivery == "pending" || w.Delivery == "held" || w.Delivery == "offline" || w.Delivery == "busy"
	if w.SettledAt == nil {
		if now.Sub(w.Deadline) <= triageWakeOverdueAfter {
			return
		}
		deadline := w.Deadline
		report.add(triageItem{
			Kind: "wake-overdue", Severity: "warn", Subject: id, Since: &deadline,
			Summary:  fmt.Sprintf("wait %q passed its deadline at %s and the coordinator has not settled it", w.Request.Name, deadline.UTC().Format(time.RFC3339)),
			Commands: []triageCommand{inspect, {Run: "t3-steward coordinator identity"}, cancel},
		})
		return
	}
	settled := *w.SettledAt
	if now.Sub(settled) <= triageWakeOverdueAfter {
		return
	}
	if w.Delivery == "pending" {
		report.add(triageItem{
			Kind: "wake-overdue", Severity: "action", Subject: id, Since: &settled,
			Summary: fmt.Sprintf("wait %q settled %s ago and no steward has claimed its wake: the steward on %s is not delivering node wakes",
				w.Request.Name, humanDuration(now.Sub(settled)), host),
			Commands: []triageCommand{
				{Host: host, Run: "systemctl --user status t3-steward"},
				{Host: host, Run: "journalctl --user -u t3-steward -n 100"},
				inspect, cancel,
			},
		})
		return
	}
	summary := fmt.Sprintf("the wake of wait %q to thread %s on %s is %s", w.Request.Name, thread, host, w.Delivery)
	if w.DeliveryError != "" {
		summary += ": " + w.DeliveryError
	}
	commands := []triageCommand{inspect}
	switch {
	case w.Delivery == "held":
		summary += "; wait dry run is on for the steward on " + host
	case !cancellable:
		summary += "; whether the message reached the thread is unknown, so it is never resent and cannot be cancelled, " +
			"and it ends rejected once its thread is gone"
	}
	if cancellable {
		commands = append(commands, cancel)
	}
	report.add(triageItem{
		Kind: "wake-undeliverable", Severity: "warn", Subject: id, Since: &settled,
		Summary: summary, Commands: commands,
	})
}

func triageTaskWait(report *triageReport, w domain.TaskWait, now time.Time) {
	task := w.WorkflowRunID + "/" + w.TaskID
	showRun := triageCommand{Run: "t3-steward campaign show " + w.WorkflowRunID}
	if w.Kind == domain.WaitKindAsk && w.Ask != nil && triageAsk(report, w, task, showRun, now) {
		return
	}
	if w.SettledAt == nil {
		if w.Kind == domain.WaitKindAttention {
			registered := w.RegisteredAt
			prompt := ""
			if w.Attention != nil {
				prompt = ": " + strconv.Quote(w.Attention.Prompt)
			}
			report.add(triageItem{
				Kind: "needs-input", Severity: "action", Subject: w.ID, Run: w.WorkflowRunID, Since: &registered,
				Summary:  "task " + task + " asks an operator" + prompt,
				Commands: []triageCommand{{Run: "t3-steward wait inspect " + w.ID}, showRun},
			})
		}
		return
	}
	settled := *w.SettledAt
	switch w.Delivery {
	case "delivered", "abandoned":
		return
	}
	if now.Sub(settled) <= triageWakeOverdueAfter {
		return
	}
	rewake := triageCommand{Run: "t3-steward backlog rewake " + task + " --reason " + shellQuote("wake not delivered; requested from triage"),
		When: "if the task's thread should get the outcome again"}
	kind, summary := "wake-undeliverable", fmt.Sprintf("the wake of task %s is %s", task, w.Delivery)
	if w.Delivery == "" || w.Delivery == "pending" {
		kind = "wake-overdue"
		summary = fmt.Sprintf("the wait of task %s settled %s ago and its wake has not been sent", task, humanDuration(now.Sub(settled)))
	}
	report.add(triageItem{
		Kind: kind, Severity: "warn", Subject: w.ID, Run: w.WorkflowRunID, Since: &settled,
		Summary: summary, Commands: []triageCommand{showRun, rewake},
	})
}

// triageAskUnansweredFor is how long an ask that expired with no answer stays
// listed after its deadline.
const triageAskUnansweredFor = 7 * 24 * time.Hour

// triageAsk lists an open ask with one ready-to-run answer command per option,
// and an ask that expired unanswered as a warning. It reports whether it dealt
// with the wait; a settled ask that was answered falls through to the ordinary
// wake checks.
func triageAsk(report *triageReport, w domain.TaskWait, task string, showRun triageCommand, now time.Time) bool {
	ask := w.Ask
	if w.SettledAt == nil {
		registered := w.RegisteredAt
		summary := fmt.Sprintf("task %s asks: %s (options: %s; ", task, strconv.Quote(ask.Question), strings.Join(ask.Options, " | "))
		if ask.OnDeadline == domain.AskDeadlineDefault {
			summary += fmt.Sprintf("%s applies in %s)", strings.Join(ask.Default, ", "), humanDuration(w.Deadline.Sub(now)))
		} else {
			summary += fmt.Sprintf("the task fails unanswered in %s)", humanDuration(w.Deadline.Sub(now)))
		}
		if ask.Relay != nil && ask.Relay.ThreadID != "" && ask.Relay.State == domain.AskRelayOpen {
			summary += "; it awaits input in T3 thread " + ask.Relay.ThreadID
		}
		when := ""
		if ask.Requires == domain.AskRequiresApprover {
			summary += "; it requires the approver, so answer it with the approver client's configuration (--config) and not in T3"
			when = "; run it with the approver client's configuration"
		}
		commands := make([]triageCommand, 0, len(ask.Options)+1)
		for _, option := range ask.Options {
			commands = append(commands, triageCommand{
				Run:  "t3-steward ask answer " + w.ID + " --option " + shellQuote(option),
				When: "to answer " + strconv.Quote(option) + when,
			})
		}
		report.add(triageItem{
			Kind: "needs-input", Severity: "action", Subject: w.ID, Run: w.WorkflowRunID, Since: &registered,
			Summary: summary, Commands: append(commands, showRun),
		})
		return true
	}
	if w.AskAnswer == nil && w.Result != nil && w.Result.Outcome == domain.TaskWaitTimedOut {
		settled := *w.SettledAt
		if now.Sub(settled) <= triageAskUnansweredFor {
			report.add(triageItem{
				Kind: "ask-unanswered", Severity: "warn", Subject: w.ID, Run: w.WorkflowRunID, Since: &settled,
				Summary: fmt.Sprintf("task %s asked %s and got no answer before its deadline; it was told to end failed",
					task, strconv.Quote(ask.Question)),
				Commands: []triageCommand{showRun, {Run: "t3-steward task result " + task}},
			})
		}
	}
	return false
}

// triageKindOrder orders items of one severity: workers first, because a
// worker that is down explains much of what follows it.
var triageKindOrder = []string{
	"worker-down", "supervision-reassess", "supervision-incident", "supervision-gate", "needs-input",
	"ask-unanswered", "wake-overdue", "wake-undeliverable", "supervision-hold", "supervision-dispatch", "quota-held",
	"intake-quarantined", "run-stalled", "worker-disconnected", "worker-maintenance",
}

func sortTriage(items []triageItem) {
	severity := map[string]int{"action": 0, "warn": 1, "info": 2}
	kind := map[string]int{}
	for i, name := range triageKindOrder {
		kind[name] = i
	}
	sort.SliceStable(items, func(i, j int) bool {
		if severity[items[i].Severity] != severity[items[j].Severity] {
			return severity[items[i].Severity] < severity[items[j].Severity]
		}
		if kind[items[i].Kind] != kind[items[j].Kind] {
			return kind[items[i].Kind] < kind[items[j].Kind]
		}
		return items[i].Subject < items[j].Subject
	})
}

func renderTriage(out io.Writer, report triageReport) error {
	counts := map[string]int{}
	for _, item := range report.Items {
		counts[item.Severity]++
	}
	coordinator := report.Coordinator
	if coordinator == "" {
		coordinator = "(unnamed)"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Triage of coordinator %s at %s: %d need action, %d warnings, %d notes.\n",
		coordinator, report.GeneratedAt.UTC().Format(time.RFC3339), counts["action"], counts["warn"], counts["info"])
	if len(report.Items) == 0 && len(report.Unavailable) == 0 && len(report.Incomplete) == 0 {
		fmt.Fprintf(&b, "Nothing needs an operator: %d enrolled workers connected, no held quota pool, no undeliverable or overdue wake, "+
			"no unanswered question, no escalated supervision and no run idle for %d days.\n", report.EnrolledWorkers, report.StaleAfterDays)
	}
	for _, item := range report.Items {
		fmt.Fprintf(&b, "\n%s %s %s: %s\n", strings.ToUpper(item.Severity), item.Kind, item.Subject, item.Summary)
		for _, command := range item.Commands {
			line := "  $ " + command.Run
			if command.Host != "" {
				line = "  on " + command.Host + ": " + command.Run
			}
			if command.When != "" {
				line += "    # " + command.When
			}
			fmt.Fprintln(&b, line)
		}
	}
	b.WriteString("\n")
	if len(report.Sources) != 0 {
		fmt.Fprintf(&b, "sources read: %s\n", strings.Join(report.Sources, ", "))
	}
	for _, incomplete := range report.Incomplete {
		fmt.Fprintf(&b, "INCOMPLETE: %s\n", incomplete)
	}
	for _, unavailable := range report.Unavailable {
		fmt.Fprintf(&b, "NOT READ: %s\n", unavailable)
	}
	_, err := io.WriteString(out, b.String())
	return err
}

// shellQuote quotes a value for a POSIX shell. Every command triage prints
// is meant to be pasted, so a reason with a space or an apostrophe must not
// split or end the argument.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
