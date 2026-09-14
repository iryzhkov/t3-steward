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
)

// The campaign help contract asks for help concise enough to enter agent
// context, so the campaign namespace carries the short transport note and
// points at "t3-steward backlog help" for the full one.
const campaignUsage = campaignCommandUsage + coordinatorTransportSummary

const campaignCommandUsage = `Usage: t3-steward campaign <command> [args]

A campaign is a version 2 workflow authored as a directory. The namespace is a
facade: submit creates exactly one workflow and one run, and every lifecycle
command below is the existing backlog operation with the same JSON and the same
exit codes. There is no campaign record, schedule or state of its own.

Authoring (reads the directory, changes nothing):
  validate <directory|workflow.yaml> [--json]
  plan     <directory|workflow.yaml> [--json|--dot]

Submission:
  submit   <directory|workflow.yaml> --idempotency-key KEY [--json]

Lifecycle (delegated to backlog, unchanged):
  list [--project P] [--progress STATES] [--class CLASS] [--json]
  show <run> [--json]
  graph <run> [--json|--dot]
  explain <run>/<task> [--json]
  cancel <run>/<task> --reason TEXT [--command-id ID] [--json]

Recovery, graph amendment and artifact commands stay under "t3-steward backlog".

Directory layout:
  campaign/
    workflow.yaml
    inputs/plan.md
    prompts/implement.md

Smallest valid manifest:
  version: 2
  name: my-campaign
  environment:
    project: <a project configured on this coordinator>
  tasks:
    implement:
      prompt_file: prompts/implement.md

Graph fields, in one line each:
  needs        start only after these tasks succeed; the manifest must be acyclic
  inputs_from  take named artifacts from a direct dependency, mounted read-only
  outputs      the files a task promises; only these are captured and retained
  verify       commands that must exit zero, or the task fails and blocks its
               dependents
Full explanation: t3-steward campaign help dag-semantics

plan is static and explain is dynamic. plan reads workflow.yaml and reports
waves, edges, inherited settings and the digest submit will send; it can never
promise a worker, a route or quota. explain reads a submitted run and answers
why a task is blocked or where it was placed.
Full explanation: t3-steward campaign help static-versus-dynamic

class: surplus is the default and runs on spare provider quota; required is
admitted ahead of surplus work. Declare it once for the workflow, or per task.

placement.hosts and placement.requires narrow which workers are eligible; they
never choose one. The scheduler places the task among the workers that remain,
by capacity and CPU class, after submission.

Retrying is safe: the same --idempotency-key with the same directory returns the
same run, because the archive is packed deterministically. The same key with
different content is refused, so use a new key when the campaign changes.

After submission:
  t3-steward campaign show <run>
  t3-steward campaign graph <run>
  t3-steward campaign explain <run>/<task>

Exit codes: 0 on success, 1 on any error, including an invalid campaign, a
refused submission or an unreachable coordinator. Every --json document is
versioned: read schemaVersion before any other field of validate and plan
output. Lifecycle JSON is the backlog schema, unchanged.

Help topics: t3-steward campaign help <plan|graph|dag-semantics|static-versus-dynamic>
Worked examples: docs/examples/campaign/single-lead
                 docs/examples/campaign/three-node
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
	limits      campaign.Limits
	stdout      io.Writer
	submissions func() (adminSubmissionService, error)
	admin       func(args []string) error
}

func cmdCampaign(g globalFlags, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return printCampaignHelp(os.Stdout, args)
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	return runCampaign(cfg, args)
}

func runCampaign(cfg config.Config, args []string) error {
	cli := campaignCLI{
		// The coordinator's own message limits decide what a campaign may
		// contain, so a directory this command accepts cannot be refused for
		// size or file count on arrival.
		limits: campaign.Limits{
			MaxFiles: cfg.BacklogV2.MessageLimits.MaxFiles,
			MaxBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		},
		stdout:      os.Stdout,
		submissions: func() (adminSubmissionService, error) { return newCampaignSubmissionClient(cfg) },
		admin:       func(args []string) error { return runCoordinatorAdmin(cfg, args, false) },
	}
	return cli.run(context.Background(), args)
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
		return printCampaignHelp(c.stdout, nil)
	}
	switch args[0] {
	case "help", "--help", "-h":
		return printCampaignHelp(c.stdout, args)
	case "validate":
		return c.runValidate(args[1:])
	case "plan":
		return c.runPlan(args[1:])
	case "submit":
		return c.runSubmit(ctx, args[1:])
	case "list", "show", "graph", "explain", "cancel":
		// Aliases forward the arguments untouched. Parsing or rendering them
		// here would be a second implementation of a command that already
		// exists, and the two would answer differently the day one changed.
		if c.admin == nil {
			return errors.New("coordinator admin transport is unavailable")
		}
		return c.admin(args)
	default:
		return fmt.Errorf("unknown campaign command %q; recovery and graph amendment stay under \"t3-steward backlog\"", args[0])
	}
}

func printCampaignHelp(out io.Writer, args []string) error {
	if len(args) > 1 {
		for _, topic := range campaign.HelpTopics() {
			if topic.Name == args[1] {
				_, err := fmt.Fprint(out, topic.Body)
				return err
			}
		}
		names := make([]string, 0, len(campaign.HelpTopics()))
		for _, topic := range campaign.HelpTopics() {
			names = append(names, topic.Name)
		}
		return fmt.Errorf("unknown campaign help topic %q; try one of %s", args[1], strings.Join(names, ", "))
	}
	_, err := fmt.Fprint(out, campaignUsage)
	return err
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
	bundle, _, err := c.prepare(parsed.source)
	if err != nil {
		return err
	}
	if c.submissions == nil {
		return errors.New("coordinator submission transport is unavailable")
	}
	client, err := c.submissions()
	if err != nil {
		return err
	}
	response, err := client.SubmitArchive(ctx,
		backlogadmin.LocalSubmissionRequest{IdempotencyKey: parsed.key},
		bytes.NewReader(bundle.Archive), int64(len(bundle.Archive)))
	if err != nil {
		return err
	}
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, response)
	}
	if _, err := fmt.Fprintf(c.stdout,
		"submission %s: workflow=%s run=%s state=%s replay=%t digest=%s\n",
		response.Key, response.WorkflowID, response.RunID, response.State, response.Replay, response.Digest,
	); err != nil {
		return err
	}
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
	asJSON bool
	asDOT  bool
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
	return parsed, nil
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
