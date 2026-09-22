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

A campaign is a version 2 workflow authored as a directory. The namespace is a
facade: submit creates exactly one workflow and one run, every lifecycle command
below is an existing backlog operation, and there is no campaign record of its own.

Offline, reaches no coordinator:
  validate <directory|workflow.yaml> [--json]
  plan     <directory|workflow.yaml> [--json|--dot]
Read-only and live, asks the coordinator and creates nothing:
  check    <directory|workflow.yaml> [--json] [--task NAME]
Mutating, checks first and creates one workflow and one run:
  submit   <directory|workflow.yaml> --idempotency-key KEY [--json] [--no-notify]
           [--allow-unverified --reason TEXT] [--notify-thread <current|id>]
Mutating recovery, creates a second run and never changes the first:
  rerun <run> --from TASK --idempotency-key KEY [--prompt TEXT] [--reason TEXT] [--json]

Lifecycle (delegated to backlog, unchanged; explain is read-only and live):
  list [--project P] [--progress STATES] [--class CLASS] [--json]
  show <run> [--json]   graph <run> [--json|--dot]   explain <run>/<task> [--json]
  cancel <run>[/<task>] --reason TEXT [--command-id ID] [--json]   no task = whole run
    cancel --json prints willCancel, the tasks it covers, not the outcome it applied.
Supervised runs, structured decisions only and never prose:
  supervision <show|decide|hold|release|escalate|resolve> <run> [flags] [--json]
    Mutating verbs need --request-id KEY, --reason TEXT and --expected-revision N.
    Flags and refusal classes: t3-steward campaign supervision --help
Graph amendment and artifact commands stay under "t3-steward backlog".
Graph fields: needs (run only after these succeed; acyclic), inputs_from (named
artifacts from a direct dependency, read-only), outputs (the files a task
promises), commits (a Git commit a successor needs), verify (must exit zero).
plan is static and explain is dynamic; check is dynamic too, before there is a
run. plan reports waves, edges and the digest submit will send, and can never
promise a worker, a route or quota. Multi-task work is a static DAG: each task
is its own Steward-scheduled T3 session, and a task prompt must not use native
subagents in place of declared tasks. Help topics: authoring, readiness,
dag-semantics, static-versus-dynamic, plan, graph, commits, rerun, notify.

check reports one outcome per task and per worker:
  ready             at least one worker can take every task now
  accepted_waiting  nobody can now, and waiting fixes it; submit proceeds
  impossible        no worker can ever run it as written; submit is refused
Permanent, so submit refuses: an unknown project, setup profile, provider instance,
model or quota pool; no route at all (declare instance and model, the coordinator
never chooses) or no configured route; invalid repository syntax, repository-not-found,
ref-not-found or authentication-failed; impossible cpu, resource, directory or
capability requirements; a missing credential; a closed timing window. Everything
else is temporary and submit proceeds, including catalog-digest-mismatch, which
means re-enrolling a worker. Codes and recovery: t3-steward campaign help readiness.

submit runs check first. --allow-unverified skips only the client-side check; the
coordinator still refuses an impossible campaign and records the principal and --reason.
Agents should not use it. accepted_waiting is a success: the run exists and stays
queued, so end the turn: this thread is notified by default, and --no-notify opts out.

class: surplus is the default and runs on spare quota, required is admitted first;
placement.hosts and placement.requires narrow eligible workers, never choose one.
Retrying is safe: the same --idempotency-key with the same directory returns the
same run, the archive being packed deterministically; the same key with
different content is refused. rerun behaves the same way. check needs no key.

A complete example, from an empty directory to a running campaign:
mkdir -p demo/prompts && echo 'do the work' > demo/prompts/implement.md
cat > demo/workflow.yaml <<'YAML'
version: 2
name: demo
environment: {project: t3-steward}
tasks: {implement: {prompt_file: prompts/implement.md}}
YAML
t3-steward campaign check demo --json
t3-steward campaign submit demo --idempotency-key demo-1 --json
t3-steward campaign show <run>

Exit codes: 0 on success and 1 on any error, plus the transport classes below for
check, submit, rerun and the lifecycle verbs; an impossible campaign is refused with
class rejected, exit 8. validate and plan use no transport class. --json is on every
verb; read schemaVersion first in validate, plan, check, submit and rerun output.

Required configuration: validate and plan need none; every other verb needs a
coordinator, through its owner-only socket here or a backlog_v2.coordinator_client
block or the UpKeeper-owned ~/.config/t3-steward/coordinator-client.json, whose
credential is a secretref:f03-admin/<client> reference resolved at use.
environment.project must exist in backlog_v2.projects with a repository, a
default ref, a setup profile and credential references the worker can present.

Worked examples: docs/examples/campaign/single-lead, docs/examples/campaign/three-node
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
	principal string
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
	switch args[0] {
	case "help", "--help", "-h":
		_, err := admitCampaignHelp(c.stdout, args)
		return err
	case "validate":
		return c.runValidate(args[1:])
	case "plan":
		return c.runPlan(args[1:])
	case "check":
		return c.runCheck(ctx, args[1:])
	case "submit":
		return c.runSubmit(ctx, args[1:])
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
		// These two forward exactly as the aliases above do, and then add the
		// supervision projection underneath the answer. The addition is an
		// appendix rather than a rewrite: the forwarded command's own output and
		// exit code are what they always were, and a run that is not supervised
		// prints nothing extra.
		if c.admin == nil {
			return errors.New("coordinator admin transport is unavailable")
		}
		if err := c.admin(args); err != nil {
			return err
		}
		c.supervisionAppendix(ctx, args)
		return nil
	default:
		return fmt.Errorf("unknown campaign command %q; recovery and graph amendment stay under \"t3-steward backlog\"", args[0])
	}
}

// admitCampaignHelp is this family's one help admission, before any argument
// is parsed. It goes through the shared helper like every other family, with
// one addition the others have no use for: "campaign help <topic>" names an
// essay rather than a verb, so a word that is a topic and not a verb is
// answered from the topic set, and a word that is neither is refused by name
// rather than answered with the family page.
func admitCampaignHelp(out io.Writer, args []string) (bool, error) {
	if len(args) == 0 {
		args = []string{"--help"}
	}
	if len(args) > 1 && isHelp(args[0]) {
		word := args[1]
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
			return true, fmt.Errorf("unknown campaign help topic %q; try one of %s", word, strings.Join(names, ", "))
		}
	}
	return admitHelp(out, []string{"campaign"}, args)
}

func (c campaignCLI) runValidate(args []string) error {
	parsed, err := parseCampaignArgs("validate", args, false, false)
	if err != nil {
		return err
	}
	bundle, plan, err := c.prepare(parsed.source)
	if err != nil {
		return err
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
			Unverified:       parsed.unverified,
			UnverifiedReason: parsed.reason,
			Principal:        c.submissionPrincipal(),
		},
		bytes.NewReader(bundle.Archive), int64(len(bundle.Archive)))
	if err != nil {
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
	if matrix.Outcome == backlogadmin.ViabilityAcceptedWaiting {
		if _, err := fmt.Fprint(c.stdout,
			"accepted_waiting: nothing can start this campaign yet, and nothing about it is\n"+
				"permanently wrong. It stays queued until the obstruction clears.\n"); err != nil {
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
		"next:\n  t3-steward campaign show %s\n  t3-steward campaign graph %s\n",
		response.RunID, response.RunID)
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
	noNotify bool
}

func parseCampaignArgs(command string, args []string, allowDOT, requireKey bool) (campaignArgs, error) {
	var parsed campaignArgs
	for index := 0; index < len(args); index++ {
		switch argument := args[index]; argument {
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
	if command == "submit" && !parsed.noNotify && parsed.notify == "" {
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
