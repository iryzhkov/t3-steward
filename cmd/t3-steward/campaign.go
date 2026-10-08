package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The campaign help contract asks for help concise enough to enter agent
// context, so the campaign namespace carries the short transport note and
// points at "t3-steward backlog help" for the full one.
const campaignUsage = campaignCommandUsage + coordinatorTransportSummary

const campaignCommandUsage = `Usage: t3-steward campaign <command> [args]

A campaign is a version 2 workflow directory. These commands use the existing
backlog workflow and run records; there is no separate campaign record.

Offline (no configuration or coordinator):
  validate <directory|workflow.yaml> [--json]
  plan     <directory|workflow.yaml> [--json|--dot]
  compile PLAN --out DIR [--unit ID] [--force] [--json]
    Writes one campaign directory per plan unit; never submits.
    --check adds the live readiness check of every unit (coordinator).
Read-only (coordinator):
  check <directory|workflow.yaml> [--json] [--task NAME]
  list [--state open|terminal] [--project P] [--class C] [--json]
    [--thread current|ID] lists the runs that notify that thread.
    --limit N and --since DURATION select the list window; --limit 0 lists all.
  progress [<run>...] [--owner THREAD] [--since RFC3339] [--json]
  show <run> [--json]
  status <run> [--json]                 alias of show
  graph <run> [--json|--dot]
  explain <run>/<task> [--json]
  commit export <run>/<task>/<commit-name> --bundle FILE [--branch NAME]
Mutating (coordinator):
  submit <directory|workflow.yaml> --idempotency-key KEY [--register-only]
    [--json] [--no-notify] [--notify-thread <current|id>]
    [--allow-unverified --reason TEXT]
    Checks readiness first. Creates one run; this thread is notified by default.
    --register-only retains a definition without starting a run.
    Registration refuses supervision/gates and needs an upgraded coordinator.
  fix <run>/<review-task> --idempotency-key KEY [--round-limit N]
    [--gate CMD] [--gate-timeout DUR] [--no-gate] [--commit RUN/TASK/NAME]
    [--context FILE] [--out DIR] [--dry-run] [--json]
    [--notify-thread current|ID | --no-notify]
  rerun <run> --from TASK --idempotency-key KEY
    [--prompt TEXT] [--reason TEXT] [--use-commit] [--json]
  cancel <run>[/<task>] --reason TEXT [--command-id ID] [--json]
  supervision <show|decide|hold|release|escalate|resolve> <run> [flags]
  recovery retry <run> [flags]

Use t3-steward campaign <command> --help full for each command's flags,
JSON keys and exit-code contract. Collect with:
  t3-steward task result <run>[/<task>]

Authoring and lifecycle topics are documented once:
  t3-steward campaign help authoring
  t3-steward campaign help <topic>
  Topics: fresh, readiness, dag-semantics, static-versus-dynamic, plan, graph,
  commits, rerun, notify, ledger, routes, supervision.
Examples: docs/examples/campaign/single-lead, docs/examples/campaign/three-node
`

// campaignValidationSchemaVersion versions the validate document. The agent
// facing output is versioned from the start, because a surface that is not
// versioned becomes unchangeable the moment anything parses it.
const campaignValidationSchemaVersion = 1

// campaignValidation is what validate reports. It is deliberately a summary:
// the full projection is what plan is for.
type campaignValidation struct {
	SchemaVersion int      `json:"schemaVersion"`
	Valid         bool     `json:"valid"`
	Source        string   `json:"source"`
	Name          string   `json:"name"`
	Tasks         int      `json:"tasks"`
	Edges         int      `json:"edges"`
	Roots         []string `json:"roots"`
	Leaves        []string `json:"leaves"`
	InputFiles    int      `json:"inputFiles"`
	Files         int      `json:"files"`
	Bytes         int64    `json:"bytes"`
	Digest        string   `json:"digest"`
}

// campaignCLI holds what the campaign commands need from their surroundings.
// The two function fields are the seams: the submission transport is resolved
// only when something is actually submitted, so validate and plan work on a
// machine with no coordinator, and the lifecycle delegation is one call into
// the existing admin path rather than a second client.
type campaignCLI struct {
	limits campaign.Limits
	stdout io.Writer
	// stderr carries anything that is not part of the result. A human warning
	// printed on stdout ahead of a --json document makes that document
	// unparseable, which turns an advisory into a failure for the agents the
	// JSON exists for.
	stderr      io.Writer
	submissions func() (adminSubmissionService, error)
	admin       func(args []string) error
	// viability is the readiness seam. It is separate from the submission and
	// lifecycle seams so that the campaign test suite can keep proving that
	// validate and plan reach no coordinator while check always does.
	viability func(context.Context, backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error)
	// amend applies one graph amendment. rerun is the only campaign verb that
	// uses it, and it is a separate seam from submission because a rerun sends
	// no bundle: it names a run the coordinator already holds.
	amend func(context.Context, domain.GraphAmendment) (domain.GraphAmendmentResult, error)
	// describe reads one run. rerun needs its graph revision to fence the
	// amendment against a run that changed under it.
	describe func(context.Context, string) (backlogadmin.WorkflowSummary, error)
	// detail reads one run with its tasks and attempts, and mutate sends one
	// revision-fenced admin command. They are the two seams the run form of
	// cancel needs: it fences on an attempt it has to read first, and it sends
	// one command rather than forwarding a command line.
	detail func(context.Context, string) (backlogadmin.WorkflowDetail, error)
	mutate func(context.Context, backlogadmin.Mutation) (backlogadmin.MutationResponse, error)
	query  func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)
	// release is the release the coordinator reports for itself, from the same
	// identity every client already reads. The run form of cancel needs it
	// because an older coordinator accepts that command and cannot apply it,
	// which is a failure the operator would otherwise learn about only from the
	// command never taking effect.
	release func(context.Context) (string, error)
	// notify registers the node wait --notify-thread asks for, and resolveThread
	// turns "current" into a canonical T3 thread id. They are separate seams so
	// that a test can prove the registration creates no workflow state.
	notify        func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error)
	resolveThread func(string) (string, error)
	// wakeHost names the host this client's threads live on. It is a seam only
	// so that a test can state a host without depending on the machine it runs
	// on; nil means os.Hostname, which is the name the wait runner matches a
	// wait's delivery host against.
	wakeHost func() (string, error)
	// delivery reads what this host's steward daemon recorded about delivering
	// node wakes. It is a seam because the answer is a file the daemon writes,
	// and because a test has to be able to state a daemon that is running, one
	// that is not, and one that was never restarted onto this release.
	delivery nodeWakeDelivery
	// superviseAs carries one structured supervision operation, under an
	// optional supervisor client identity. It is its own seam because
	// supervision travels over an optional interface the carrier may not
	// implement: a coordinator client that predates supervision has to report
	// the operation as unavailable, which is a property of this seam and not of
	// the verb that used it.
	//
	// The identity is a parameter of this seam and of no other, which is how the
	// CLI refuses to sign anything but supervision with a supervisor credential:
	// no other command family can reach a transport built from one.
	superviseAs     func(context.Context, supervisorIdentity, backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error)
	retryRecoveryAs func(context.Context, supervisorIdentity, domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error)
	// principal names who is running the command. It appears in the audit
	// record of a submission that skipped the live check.
	principal    string
	exportCommit func(context.Context, backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error)
}

func cmdCampaign(g globalFlags, args []string) error {
	if handled, err := admitCampaignHelp(os.Stdout, args); handled || err != nil {
		return err
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return runCampaign(cfg, args)
}

func runCampaign(cfg config.Config, args []string) error {
	return campaignCLIFor(cfg).run(context.Background(), args)
}

// campaignCLIFor builds the campaign CLI with its real transports. It is a
// function of its own because "task run" composes the same seams: the single
// task start is the campaign path with the authoring removed, and a second
// construction of check, submit and notify would be a second behaviour to keep
// in agreement.
func campaignCLIFor(cfg config.Config) campaignCLI {
	cli := campaignCLI{
		// The coordinator's own message limits decide what a campaign may
		// contain, so a directory this command accepts cannot be refused for
		// size or file count on arrival.
		limits: campaign.Limits{
			MaxFiles: cfg.BacklogV2.MessageLimits.MaxFiles,
			MaxBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		},
		exportCommit: func(ctx context.Context, request backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return backlogadmin.ArtifactContent{}, err
			}
			client, ok := transport.client.(backlogadmin.CommitExportTransport)
			if !ok {
				return backlogadmin.ArtifactContent{}, errors.New("coordinator commit export is unavailable; upgrade the coordinator")
			}
			return client.ExportCommit(ctx, request)
		},
		stdout:      os.Stdout,
		stderr:      os.Stderr,
		submissions: func() (adminSubmissionService, error) { return newCampaignSubmissionClient(cfg) },
		admin:       func(args []string) error { return runCoordinatorAdmin(cfg, args, false) },
		viability: func(ctx context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
			return queryCampaignViability(ctx, cfg, request)
		},
		amend: func(ctx context.Context, request domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return domain.GraphAmendmentResult{}, err
			}
			return transport.client.AmendGraph(ctx, request)
		},
		describe: func(ctx context.Context, runID string) (backlogadmin.WorkflowSummary, error) {
			return describeCampaignRun(ctx, cfg, runID)
		},
		detail: func(ctx context.Context, runID string) (backlogadmin.WorkflowDetail, error) {
			return describeCampaignRunDetail(ctx, cfg, runID)
		},
		mutate: func(ctx context.Context, mutation backlogadmin.Mutation) (backlogadmin.MutationResponse, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return backlogadmin.MutationResponse{}, err
			}
			mutation.Principal = transport.principal
			return transport.client.Mutate(ctx, mutation)
		},
		query: func(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return backlogadmin.Response{}, err
			}
			query.Version, query.Principal = backlogadmin.Version, transport.principal
			return transport.client.Query(ctx, query)
		},
		release: func(ctx context.Context) (string, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return "", err
			}
			response, err := transport.client.Query(ctx, backlogadmin.Query{
				Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus, Principal: transport.principal,
			})
			if err != nil {
				return "", err
			}
			if response.Status == nil {
				return "", nil
			}
			return response.Status.Runtime.Release, nil
		},
		notify: func(ctx context.Context, operation backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			transport, err := newCoordinatorTransport(cfg)
			if err != nil {
				return backlogadmin.NodeWaitResponse{}, err
			}
			return transport.client.NodeWait(ctx, operation)
		},
		resolveThread: func(explicit string) (string, error) { return resolveThread(cfg, explicit) },
		delivery:      nodeWakeDeliveryFor(cfg),
		retryRecoveryAs: func(ctx context.Context, identity supervisorIdentity, request domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error) {
			transport, err := newCoordinatorTransportAs(cfg, identity)
			if err != nil {
				return domain.RecoveryRetryReceipt{}, err
			}
			carrier, ok := transport.client.(backlogadmin.RecoveryRetryTransport)
			if !ok {
				return domain.RecoveryRetryReceipt{}, errors.New("coordinator recovery retry is unavailable")
			}
			return carrier.RetryRecovery(ctx, request)
		},
		superviseAs: func(ctx context.Context, identity supervisorIdentity, request backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error) {
			// This is the only construction in the CLI that may carry a supervisor
			// credential, and it is reached only from the supervision verbs.
			transport, err := newCoordinatorTransportAs(cfg, identity)
			if err != nil {
				return backlogadmin.SupervisionResponse{}, err
			}
			// Supervision is an optional interface on the carrier. A client that
			// predates it is asserted for rather than assumed, so an older
			// coordinator client reports an unavailable operation instead of
			// panicking on a type it never promised to be.
			carrier, ok := transport.client.(backlogadmin.SupervisionTransport)
			if !ok {
				return backlogadmin.SupervisionResponse{}, fmt.Errorf(
					"%w: this coordinator client carries no supervision", backlogadmin.ErrSupervisionUnavailable)
			}
			return carrier.Supervise(ctx, request)
		},
	}
	transport, err := newCoordinatorTransport(cfg)
	if err == nil {
		cli.principal = transport.principal.ID
	}
	return cli
}

// queryCampaignViability asks the coordinator whether a projected campaign
// could run. It travels over the same admin transport as every other query, so
// it works from a host that is not the coordinator without a second carrier.
func queryCampaignViability(ctx context.Context, cfg config.Config, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return backlogadmin.ViabilityMatrix{}, err
	}
	response, err := transport.client.Query(ctx, backlogadmin.Query{
		Version:   backlogadmin.Version,
		Kind:      backlogadmin.QueryViability,
		Principal: transport.principal,
		Viability: &request,
	})
	if err != nil {
		return backlogadmin.ViabilityMatrix{}, err
	}
	if response.Viability == nil {
		return backlogadmin.ViabilityMatrix{}, &backlogadmin.TransportError{
			Class:     backlogadmin.ClassProtocol,
			Operation: "viability",
			Err:       errors.New("the coordinator answered a viability query with no matrix"),
		}
	}
	return *response.Viability, nil
}

// newCampaignSubmissionClient builds the same local transport the backlog admin
// commands use. Campaign submission differs from backlog submission only in
// where the bytes come from, so it must not differ in how they travel.
func newCampaignSubmissionClient(cfg config.Config) (adminSubmissionService, error) {
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return nil, err
	}
	return transport.client, nil
}

func (c campaignCLI) run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		_, err := admitCampaignHelp(c.stdout, nil)
		return err
	}
	if args[0] == "status" {
		args = append([]string{"show"}, args[1:]...)
	}
	switch args[0] {
	case "help", "--help", "-h":
		_, err := admitCampaignHelp(c.stdout, args)
		return err
	case "commit":
		return c.runCommit(ctx, args[1:])
	case "progress":
		return c.runProgress(ctx, args[1:])
	case "collect", "uncollected":
		return c.runCollection(ctx, args)
	case "validate":
		return c.runValidate(args[1:])
	case "plan":
		return c.runPlan(args[1:])
	case "compile":
		return c.runCompile(ctx, args[1:])
	case "check":
		return c.runCheck(ctx, args[1:])
	case "submit":
		return c.runSubmit(ctx, args[1:])
	case "fix":
		return c.runFix(ctx, args[1:])
	case "rerun":
		return c.runRerun(ctx, args[1:])
	case "supervision":
		return c.runSupervision(ctx, args[1:])
	case "recovery":
		return c.runRecovery(ctx, args[1:])
	case "list", "graph", "cancel":
		if args[0] == "cancel" && isCampaignRunCancel(args) {
			// "cancel <run>" is a command of its own: one revision-fenced
			// cancellation of every non-terminal task. "cancel <run>/<task>"
			// stays the forwarded alias it has always been.
			return c.runCampaignCancelRun(ctx, args)
		}
		// Aliases forward the arguments untouched. Parsing or rendering them
		// here would be a second implementation of a command that already
		// exists, and the two would answer differently the day one changed.
		if c.admin == nil {
			return errors.New("coordinator admin transport is unavailable")
		}
		return c.admin(args)
	case "show", "explain":
		if args[0] == "show" {
			var err error
			args, err = c.waitForShow(ctx, args)
			if err != nil {
				return err
			}
		}
		// These two forward exactly as the aliases above do, and then add the
		// supervision projection underneath the answer. The addition is an
		// appendix rather than a rewrite: the forwarded command's own output and
		// exit code are what they always were, and a run that is not supervised
		// prints nothing extra.
		if c.admin == nil {
			return errors.New("coordinator admin transport is unavailable")
		}
		if args[0] == "explain" {
			if err := c.explainNamesATask(ctx, args[1:]); err != nil {
				return err
			}
		}
		if err := c.admin(args); err != nil {
			return err
		}
		c.supervisionAppendix(ctx, args)
		return nil
	default:
		const elsewhere = "\"t3-steward campaign help\" lists every command; task, edge and artifact commands stay under \"t3-steward backlog\""
		if near := nearestCampaignCommand(args[0]); near != "" {
			return fmt.Errorf("unknown campaign command %q; did you mean %q? %s", args[0], near, elsewhere)
		}
		return fmt.Errorf("unknown campaign command %q; %s", args[0], elsewhere)
	}
}

// explainNamesATask refuses "campaign explain <run>" with the run's tasks and
// their state, rather than letting the forwarded verb answer with a bare
// format error: explain is about one task, and the caller who named only the
// run needs the names to choose from. Any other argument shape is left to the
// forwarded verb, which owns its parsing.
func (c campaignCLI) explainNamesATask(ctx context.Context, args []string) error {
	var positional []string
	for _, arg := range args {
		if arg != "--json" {
			positional = append(positional, arg)
		}
	}
	if len(positional) != 1 || strings.Contains(positional[0], "/") || strings.HasPrefix(positional[0], "-") || c.detail == nil {
		return nil
	}
	run := positional[0]
	usage := fmt.Sprintf("campaign explain needs <run>/<task>, such as %s/<task>", run)
	detail, err := c.detail(ctx, run)
	if err != nil {
		return fmt.Errorf("%s; the run's tasks could not be read: %w", usage, err)
	}
	var tasks []string
	for _, task := range detail.Tasks {
		if task.Sink != nil {
			continue
		}
		state, _, _ := taskState(task)
		tasks = append(tasks, fmt.Sprintf("%s (%s)", task.Task.Name, state))
	}
	if len(tasks) == 0 {
		return errors.New(usage)
	}
	return fmt.Errorf("%s; run %s has tasks %s", usage, run, strings.Join(tasks, ", "))
}

// campaignCommands are the subcommands run dispatches, in the order a
// did-you-mean suggestion prefers them.
var campaignCommands = []string{"validate", "plan", "compile", "check", "submit", "list", "progress", "show", "status", "explain", "graph", "cancel", "fix", "rerun", "supervision", "recovery", "commit", "help"}

// nearestCampaignCommand returns the campaign subcommand within two edits of
// name, or "" when none is that close.
func nearestCampaignCommand(name string) string {
	best, bestDistance := "", 3
	for _, command := range campaignCommands {
		if distance := backlog.EditDistance(strings.ToLower(name), command); distance < bestDistance {
			best, bestDistance = command, distance
		}
	}
	return best
}

// admitCampaignHelp is this family's one help admission, before any argument
// is parsed. It goes through the shared helper like every other family, with
// one addition the others have no use for: "campaign help <topic>" names an
// essay rather than a verb, so a word that is a topic and not a verb is
// answered from the topic set, and a word that is neither is refused by name
// rather than answered with the family page.
func admitCampaignHelp(out io.Writer, args []string) (bool, error) {
	request, legacy, help := campaignHelpArguments(args)
	if !help {
		return false, nil
	}
	if handled, err := admitHelpExportRequest(out, []string{"campaign"}, request); handled || err != nil {
		return handled, err
	}
	// Classification is final before legacy config normalization. Render the
	// old reference directly, so exposed operands cannot become exports.
	args = legacy
	reference := func() (bool, error) {
		full := false
		clean := append([]string(nil), args...)
		for i := 0; i+1 < len(clean); i++ {
			if clean[i] == "--" {
				break
			}
			if isHelp(clean[i]) && clean[i+1] == "full" {
				full = true
				clean = append(clean[:i+1], clean[i+2:]...)
				break
			}
		}
		resolution := resolveHelp([]string{"campaign"}, clean)
		if resolution.Unknown != "" {
			return false, unknownHelpVerb(resolution.Parent, resolution.Unknown)
		}
		if !resolution.Answered {
			return false, nil
		}
		page := helpPages[strings.Join(resolution.Path, " ")]
		body := page.renderShort()
		if full {
			body = page.render()
		}
		_, err := fmt.Fprint(out, body)
		return true, err
	}
	if len(args) == 0 {
		args = []string{"--help"}
	}
	if len(args) > 1 && isHelp(args[0]) {
		word := args[1]
		if word == "full" {
			return reference()
		}
		// The explicit "campaign help <word>" form names a topic first. Several
		// topics share a name with a verb -- plan, graph, rerun -- and the essay
		// is what that form has always answered with; the verb's reference is
		// one word away, as "campaign <verb> --help".
		if word == "supervision" {
			return true, printCampaignSupervisionHelp(out)
		}
		for _, topic := range campaign.HelpTopics() {
			if topic.Name == word {
				_, err := fmt.Fprint(out, topic.Body)
				return true, err
			}
		}
		if _, verb := helpPageFor("campaign " + word); !verb {
			names := make([]string, 0, len(campaign.HelpTopics()))
			for _, topic := range campaign.HelpTopics() {
				names = append(names, topic.Name)
			}
			// supervision is answered above rather than from the topic set, and is
			// a topic all the same.
			names = append(names, "supervision")
			return true, fmt.Errorf("unknown campaign help topic %q; try one of %s", word, strings.Join(names, ", "))
		}
	}
	return reference()
}

func (c campaignCLI) runValidate(args []string) error {
	parsed, err := parseCampaignArgs("validate", args, false, false)
	if err != nil {
		return withSchemaVersion(err, campaignValidationSchemaVersion)
	}
	bundle, plan, err := c.prepare(parsed.source)
	if err != nil {
		return withSchemaVersion(err, campaignValidationSchemaVersion)
	}
	summary := campaignValidation{
		SchemaVersion: campaignValidationSchemaVersion,
		Valid:         true,
		Source:        parsed.source,
		Name:          plan.Name,
		Tasks:         plan.Totals.Tasks,
		Edges:         plan.Totals.Edges,
		Roots:         plan.Roots,
		Leaves:        plan.Leaves,
		InputFiles:    plan.Totals.InputFiles,
		Files:         len(bundle.Campaign.Files),
		Bytes:         bundle.Campaign.TotalBytes(),
		Digest:        bundle.ContentDigest,
	}
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, summary)
	}
	_, err = fmt.Fprintf(c.stdout,
		"campaign %s is valid\n  tasks   %d\n  edges   %d\n  roots   %s\n  leaves  %s\n  inputs  %d files\n  bundle  %d files, %d bytes\n  digest  %s\n",
		summary.Name, summary.Tasks, summary.Edges,
		campaignList(summary.Roots), campaignList(summary.Leaves),
		summary.InputFiles, summary.Files, summary.Bytes, summary.Digest,
	)
	return err
}

func (c campaignCLI) runPlan(args []string) error {
	parsed, err := parseCampaignArgs("plan", args, true, false)
	if err != nil {
		return err
	}
	_, plan, err := c.prepare(parsed.source)
	if err != nil {
		return err
	}
	switch {
	case parsed.asJSON:
		document, err := campaign.RenderJSON(plan)
		if err != nil {
			return err
		}
		_, err = c.stdout.Write(document)
		return err
	case parsed.asDOT:
		_, err = fmt.Fprint(c.stdout, campaign.RenderDOT(plan))
		return err
	default:
		_, err = fmt.Fprint(c.stdout, campaign.RenderText(plan))
		return err
	}
}

func (c campaignCLI) runSubmit(ctx context.Context, args []string) error {
	parsed, err := parseCampaignArgs("submit", args, false, true)
	if err != nil {
		return err
	}
	// Submission runs the validation path rather than a shorter one of its
	// own: a campaign that submit would accept and validate would refuse is a
	// difference nobody can explain afterwards.
	bundle, plan, err := c.prepare(parsed.source)
	if err != nil {
		return err
	}
	if parsed.registerOnly && (bundle.Campaign.Manifest.Supervision != nil || len(bundle.Campaign.Manifest.Gates) != 0) {
		return fmt.Errorf("register-only refuses supervised campaigns: scheduled supervision is not implemented; next: t3-steward campaign submit %q --idempotency-key %q", parsed.source, parsed.key)
	}
	// The thread to notify is resolved before submission. An unresolvable
	// --notify-thread must not leave a run behind that nobody is listening for.
	notifyThread, err := c.campaignNotifyThread("campaign submit", parsed.notify)
	if err != nil {
		return err
	}
	// The live check runs before anything is packed and sent. A campaign that
	// can never run must not consume a run ID and a place in the graph before
	// anyone finds out.
	var matrix backlogadmin.ViabilityMatrix
	if parsed.unverified {
		// The warning goes to stderr so that --json output stays one document a
		// strict reader can parse.
		if _, err := fmt.Fprintf(c.warnings(),
			"warning: skipping the live readiness check. principal=%s reason=%s\n"+
				"The coordinator still refuses a permanently impossible campaign at acceptance.\n",
			c.submissionPrincipal(), parsed.reason); err != nil {
			return err
		}
	} else {
		matrix, err = c.checkViability(ctx, plan, bundle, "")
		if err != nil {
			return err
		}
		if matrix.Outcome == backlogadmin.ViabilityImpossible {
			return campaignImpossible(matrix)
		}
	}
	if c.submissions == nil {
		return errors.New("coordinator submission transport is unavailable")
	}
	client, err := c.submissions()
	if err != nil {
		return err
	}
	response, err := client.SubmitArchive(ctx,
		backlogadmin.LocalSubmissionRequest{
			IdempotencyKey:   parsed.key,
			RegisterOnly:     parsed.registerOnly,
			Unverified:       parsed.unverified,
			UnverifiedReason: parsed.reason,
			Principal:        c.submissionPrincipal(),
		},
		bytes.NewReader(bundle.Archive), int64(len(bundle.Archive)))
	if err != nil {
		if parsed.registerOnly && strings.Contains(err.Error(), `invalid operation envelope: json: unknown field "registerOnly"`) {
			return errors.New("coordinator too old for --register-only (needs 0.11.0-rc.103 or later)")
		}
		return err
	}
	if parsed.registerOnly {
		if response.RunID != "" {
			return fmt.Errorf("coordinator created run %s for register-only; next: t3-steward campaign show %s", response.RunID, response.RunID)
		}
		if parsed.asJSON {
			return encodeCampaignJSON(c.stdout, response)
		}
		_, err := fmt.Fprintf(c.stdout, "registered workflow %s (replay=%t); no run started.\nnext: t3-steward schedules put %q --name %q --workflow %s --cron \"0 2 * * *\" --timezone UTC --reason \"schedule registered campaign\"\n", response.WorkflowID, response.Replay, plan.Name, plan.Name, response.WorkflowID)
		return err
	}
	// What this submission may promise about a wake is decided by the same
	// rule "task run" uses, through the same path: a key that replayed onto a
	// run that already ended gets no registration at all, and a run whose
	// progress cannot be read gets no promise. Writing the rule a second time
	// here is how this verb went on saying "End this turn now" for a wait the
	// coordinator had already answered.
	wake := startedRunWake{Run: response.RunID}
	if response.Replay {
		progress, progressErr := c.replayedRunProgress(ctx, response.RunID)
		if progressErr != nil {
			wake.ProgressUnavailable = progressErr.Error()
		} else {
			wake.Progress = string(progress)
		}
	}
	notification, err := c.attachWake(ctx, parsed.key, response.RunID, notifyThread, wake.runIsTerminal())
	if err != nil {
		return err
	}
	wake.Notify = notification
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, campaignSubmission{
			LocalSubmissionResponse: response,
			Outcome:                 matrix.Outcome,
			Matrix:                  campaignWaitingMatrix(matrix),
			Notify:                  notification,
		})
	}
	// The first line is the run, as it is on "task run": one convention for
	// where a caller finds the id, whichever verb started the run.
	if _, err := fmt.Fprintf(c.stdout, "run %s\n", response.RunID); err != nil {
		return err
	}
	if matrix.Outcome == backlogadmin.ViabilityAcceptedWaiting {
		// Accepted comes next: the run exists. The old banner opened with
		// "nothing can start this campaign", which agents read as a failed
		// submission and retried.
		if _, err := fmt.Fprintf(c.stdout,
			"accepted: the campaign was accepted and its run is queued (accepted_waiting).\n"+
				"It is waiting for %s, and starts on its own once that clears; nothing about it\n"+
				"is permanently wrong. Readiness at submission:\n", campaignWaitingFor(matrix)); err != nil {
			return err
		}
		if err := renderCampaignCheck(c.stdout, campaignCheck{
			SchemaVersion: campaignCheckSchemaVersion,
			Source:        parsed.source, Name: plan.Name, Digest: bundle.ContentDigest,
			Matrix: matrix,
		}); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(c.stdout,
		"submission %s: workflow=%s run=%s state=%s replay=%t digest=%s\n",
		response.Key, response.WorkflowID, response.RunID, response.State, response.Replay, response.Digest,
	); err != nil {
		return err
	}
	renderRunProgress(c.stdout, wake)
	renderWake(c.stdout, wake)
	_, err = fmt.Fprintf(c.stdout,
		"next:\n  t3-steward campaign show %s\n  t3-steward campaign graph %s\n  t3-steward task result %s\n",
		response.RunID, response.RunID, response.RunID)
	return err
}

// prepare is the one path validate, plan and submit share. Every one of them
// loads, validates and packs the same way, so they cannot disagree about
// whether a directory is acceptable or about which digest it produces.
func (c campaignCLI) prepare(source string) (campaign.Bundle, campaign.Plan, error) {
	bundle, err := campaign.Prepare(source, c.limits)
	if err != nil {
		return campaign.Bundle{}, campaign.Plan{}, fmt.Errorf("%s: %w", source, err)
	}
	plan, err := campaign.Project(bundle.Campaign.Manifest, campaign.Options{
		Source:     source,
		Digest:     bundle.ContentDigest,
		InputFiles: bundle.Campaign.InputPaths(),
	})
	if err != nil {
		return campaign.Bundle{}, campaign.Plan{}, fmt.Errorf("%s: %w", source, err)
	}
	annotateCampaignLocalRoles(&plan, defaultRoutePolicyPath(), c.stderr)
	return bundle, plan, nil
}

// campaignArgs is one parsed authoring command line.
type campaignArgs struct {
	source string
	key    string
	task   string
	reason string
	asJSON bool
	asDOT  bool
	// unverified skips the client-side readiness check. It never skips the
	// coordinator's own permanent validation at acceptance.
	unverified bool
	// notify names the T3 thread a terminal outcome is delivered to, or
	// "current" for the calling agent's own canonical thread. It defaults to
	// "current" on submit, the same default "task run" has, so that one spelling
	// means one thing on both verbs.
	notify string
	// noNotify is the explicit opt-out, the only way to submit a campaign
	// nobody will be woken for.
	noNotify     bool
	registerOnly bool
}

func parseCampaignArgs(command string, args []string, allowDOT, requireKey bool) (campaignArgs, error) {
	var parsed campaignArgs
	for index := 0; index < len(args); index++ {
		switch argument := args[index]; argument {
		case "--register-only":
			if command != "submit" || parsed.registerOnly {
				return campaignArgs{}, errors.New("--register-only is accepted once by submit only; next: t3-steward campaign submit --help full")
			}
			parsed.registerOnly = true
		case "--json":
			if parsed.asJSON {
				return campaignArgs{}, errors.New("--json may be supplied only once")
			}
			parsed.asJSON = true
		case "--dot":
			if !allowDOT {
				return campaignArgs{}, fmt.Errorf("campaign %s does not accept --dot", command)
			}
			if parsed.asDOT {
				return campaignArgs{}, errors.New("--dot may be supplied only once")
			}
			parsed.asDOT = true
		case "--idempotency-key":
			if !requireKey {
				return campaignArgs{}, fmt.Errorf("campaign %s does not accept --idempotency-key", command)
			}
			if parsed.key != "" || index+1 >= len(args) || args[index+1] == "" {
				return campaignArgs{}, errors.New("--idempotency-key needs one nonempty value")
			}
			index++
			parsed.key = args[index]
		case "--task":
			if command != "check" {
				return campaignArgs{}, fmt.Errorf("campaign %s does not accept --task", command)
			}
			if parsed.task != "" || index+1 >= len(args) || args[index+1] == "" {
				return campaignArgs{}, errors.New("--task needs one nonempty task name")
			}
			index++
			parsed.task = args[index]
		case "--allow-unverified":
			if command != "submit" {
				return campaignArgs{}, fmt.Errorf("campaign %s does not accept --allow-unverified", command)
			}
			if parsed.unverified {
				return campaignArgs{}, errors.New("--allow-unverified may be supplied only once")
			}
			parsed.unverified = true
		case "--reason":
			if command != "submit" {
				return campaignArgs{}, fmt.Errorf("campaign %s does not accept --reason", command)
			}
			if parsed.reason != "" || index+1 >= len(args) || args[index+1] == "" {
				return campaignArgs{}, errors.New("--reason needs one nonempty value")
			}
			index++
			parsed.reason = args[index]
		case "--notify-thread":
			if command != "submit" {
				return campaignArgs{}, fmt.Errorf("campaign %s does not accept --notify-thread", command)
			}
			if parsed.notify != "" || index+1 >= len(args) || args[index+1] == "" {
				return campaignArgs{}, errors.New("--notify-thread needs one value: current or a T3 thread id")
			}
			index++
			parsed.notify = args[index]
		case "--no-notify":
			if command != "submit" {
				return campaignArgs{}, fmt.Errorf("campaign %s does not accept --no-notify", command)
			}
			parsed.noNotify = true
		default:
			if strings.HasPrefix(argument, "-") {
				return campaignArgs{}, fmt.Errorf("unknown campaign %s option %q", command, argument)
			}
			if parsed.source != "" {
				return campaignArgs{}, fmt.Errorf("campaign %s accepts exactly one campaign directory or workflow.yaml path", command)
			}
			parsed.source = argument
		}
	}
	if parsed.source == "" {
		return campaignArgs{}, fmt.Errorf("campaign %s needs a campaign directory or workflow.yaml path", command)
	}
	if parsed.asJSON && parsed.asDOT {
		return campaignArgs{}, errors.New("--json and --dot are mutually exclusive")
	}
	if requireKey && parsed.key == "" {
		// The key is required rather than generated: a generated key would make
		// a repeated submission a second run instead of the replay the caller
		// almost certainly meant.
		return campaignArgs{}, fmt.Errorf("campaign %s requires --idempotency-key KEY", command)
	}
	if parsed.key != "" {
		if err := validateIdempotencyKeyFlag(parsed.key); err != nil {
			return campaignArgs{}, err
		}
	}
	if parsed.unverified && parsed.reason == "" {
		// The escape hatch is audited, and an audit record with no reason is a
		// record that nobody can act on later.
		return campaignArgs{}, errors.New("--allow-unverified requires --reason TEXT, which is recorded in the submission audit record")
	}
	if parsed.reason != "" && !parsed.unverified {
		return campaignArgs{}, errors.New("--reason is only meaningful with --allow-unverified")
	}
	if parsed.noNotify && parsed.notify != "" {
		return campaignArgs{}, errors.New("--no-notify and --notify-thread contradict each other: one says nobody is woken, the other names who is")
	}
	if parsed.registerOnly && parsed.notify != "" {
		return campaignArgs{}, errors.New("--register-only creates no run to notify for; next: t3-steward campaign submit --help full")
	}
	if command == "submit" && !parsed.registerOnly && !parsed.noNotify && parsed.notify == "" {
		// The calling thread is notified by default, as it is on "task run". A
		// campaign nobody will hear about is started only when the caller says
		// so with --no-notify.
		parsed.notify = "current"
	}
	return parsed, nil
}

// warnings is where advisory text goes. It falls back to stdout only when no
// stderr was wired, which keeps a zero-valued campaignCLI usable in a test.
func (c campaignCLI) warnings() io.Writer {
	if c.stderr != nil {
		return c.stderr
	}
	return c.stdout
}

func encodeCampaignJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func campaignList(values []string) string {
	if len(values) == 0 {
		return "(none)"
	}
	return strings.Join(values, ", ")
}
