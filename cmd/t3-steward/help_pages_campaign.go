package main

// The campaign family's second-level pages. The namespace is a facade: submit
// creates exactly one workflow and one run, and every lifecycle verb below is
// an existing backlog operation.

// campaignAuthoringNote is on every page whose arguments pass through
// parseCampaignArgs. One parser serves validate, plan, check and submit, so it
// recognises the whole authoring vocabulary and refuses, by name, the options
// that belong to a sibling verb. Naming them here is what makes this page a
// complete account of what that parser will do with this command line.
const campaignAuthoringNote = "validate, plan, check and submit share one option parser. It also recognises " +
	"--dot, --idempotency-key, --task, --allow-unverified, --reason, --notify-thread and --no-notify, and refuses the ones " +
	"that belong to a sibling verb with \"campaign <verb> does not accept <option>\". Any other option is refused " +
	"as unknown."

func campaignHelpPages() []helpPage {
	return []helpPage{
		{
			Path:     "campaign validate",
			Purpose:  "check a campaign directory's structure, graph and size, offline.",
			Usage:    []string{"t3-steward campaign validate <directory|workflow.yaml> [--json]"},
			Flags:    []helpFlag{jsonFlag("the validation summary")},
			Exits:    []helpExit{{0, "the campaign is valid"}, {1, "no source, more than one source, an unknown option, or an invalid campaign"}},
			JSONKeys: []string{"schemaVersion", "valid", "source", "name", "tasks", "edges", "roots", "leaves", "inputFiles", "files", "bytes", "digest"},
			JSONNote: "Read schemaVersion first. No transport class applies: this verb has none.",
			Notes:    "Offline: it reads the directory and reaches no coordinator. It can never say whether a worker could run the campaign; that is \"campaign check\".\n\n" + campaignAuthoringNote,
			Parsers:  []parserSite{{Func: "parseCampaignArgs"}},
		},
		{
			Path:    "campaign plan",
			Purpose: "project a campaign directory into waves, edges and the digest submit would send.",
			Usage:   []string{"t3-steward campaign plan <directory|workflow.yaml> [--json|--dot]"},
			Flags: []helpFlag{
				jsonFlag("the projection"),
				{Name: "--dot", Default: "off", Text: "Print the graph in Graphviz DOT. Mutually exclusive with --json."},
			},
			Exits:    []helpExit{{0, "projected"}, {1, "no source, an unknown option, --json with --dot, or an invalid campaign"}},
			JSONKeys: []string{"schemaVersion", "name", "source", "digest", "waves", "tasks", "edges"},
			JSONNote: "Read schemaVersion first. No transport class applies.",
			Notes:    "Offline and static: it reaches no coordinator and can never promise a worker, a route or quota. The dynamic answer, once a run exists, is \"t3-steward backlog explain\".\n\n" + campaignAuthoringNote,
			Parsers:  []parserSite{{Func: "parseCampaignArgs"}},
		},
		{
			Path:    "campaign check",
			Purpose: "ask the coordinator whether a campaign could run now, creating nothing.",
			Usage:   []string{"t3-steward campaign check <directory|workflow.yaml> [--task NAME] [--json]"},
			Flags: []helpFlag{
				{Name: "--task", Value: "NAME", Default: "every task", Text: "Report one task of the campaign only."},
				jsonFlag("the readiness matrix"),
			},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"schemaVersion", "outcome", "tasks", "workers", "reasons"},
			JSONNote: "Read schemaVersion first. An impossible campaign prints the matrix on stdout and then exits 8; the document is the answer and the exit code is the refusal. " + jsonErrorNote,
			Notes:    "Live and read-only: it creates nothing. Per task and per worker it reports ready, accepted_waiting (nobody can now and waiting fixes it) or impossible (no worker can ever run it as written, and submit is refused). Codes and recovery: t3-steward campaign help readiness.\n\n" + campaignAuthoringNote,
			Parsers:  []parserSite{{Func: "parseCampaignArgs"}},
		},
		{
			Path:    "campaign submit",
			Purpose: "create exactly one workflow and one run from a campaign directory.",
			Usage:   []string{"t3-steward campaign submit <directory|workflow.yaml> --idempotency-key KEY [--allow-unverified --reason TEXT] [--notify-thread current|<id>|--no-notify] [--json]"},
			Flags: []helpFlag{
				{Name: "--idempotency-key", Value: "KEY", Required: true, Text: "The key this submission is recorded under. The same key with the same directory returns the same run, the archive being packed deterministically; the same key with different content is refused. It is required rather than generated, because a generated key turns a retry into a second run."},
				{Name: "--allow-unverified", Default: "off", Text: "Skip the client-side readiness check. The coordinator still refuses an impossible campaign. Requires --reason. Agents should not use it."},
				{Name: "--reason", Value: "TEXT", Default: "none", Text: "Why the check was skipped; recorded with the principal in the submission audit record. Only meaningful with --allow-unverified, and required by it."},
				{Name: "--notify-thread", Value: "current|<id>", Default: "current", Text: "Register a node wait so that this thread, or the named T3 thread, is woken when the run ends. It is the same spelling \"task run\" takes, and it defaults the same way."},
				{Name: "--no-notify", Default: "off", Text: "Submit a campaign nobody is woken for. It is the only way to opt out, because the calling thread is notified by default and a submission no thread resolves for is refused."},
				jsonFlag("the submission receipt"),
			},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"schemaVersion", "key", "digest", "workflowId", "runId", "state", "acceptedAt", "replay"},
			JSONNote: "Read schemaVersion first. An impossible campaign is refused with class rejected, exit 8. " + jsonErrorNote,
			Notes:    "Mutating. It runs check first unless --allow-unverified. accepted_waiting is a success: the run exists and stays queued, so end the turn. The calling thread is notified by default; --no-notify submits a campaign nobody is woken for.\n\n" + campaignAuthoringNote,
			Parsers:  []parserSite{{Func: "parseCampaignArgs"}},
		},
		{
			Path:    "campaign rerun",
			Purpose: "create a second run of an existing campaign, starting again from one task.",
			Usage:   []string{"t3-steward campaign rerun <run> --from TASK --idempotency-key KEY [--prompt TEXT] [--reason TEXT] [--json]"},
			Flags: []helpFlag{
				{Name: "--from", Value: "TASK", Required: true, Text: "The task to start again from. Everything that depends on it is run again."},
				{Name: "--idempotency-key", Value: "KEY", Required: true, Text: "The key the rerun is recorded under. Repeating it with the same content returns the same run."},
				{Name: "--prompt", Value: "TEXT", Default: "source prompt", Text: "Corrected instructions for the selected root task only; retained as a new immutable input."},
				{Name: "--reason", Value: "TEXT", Default: "none", Text: "Why the campaign is being rerun; recorded with the amendment."},
				jsonFlag("the rerun receipt"),
			},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"schemaVersion", "run", "from", "key", "replay"},
			JSONNote: jsonErrorNote,
			Notes:    "Mutating recovery: it creates a second run and never changes the first. It names a run the coordinator already holds, so it sends no bundle. Exactly one run argument.",
			Parsers:  []parserSite{{Func: "parseCampaignRerunArgs"}},
		},
		{
			Path:    "campaign cancel",
			Purpose: "cancel a whole campaign run, or forward one task's cancellation to backlog.",
			Usage: []string{
				"t3-steward campaign cancel <run> --reason TEXT [--command-id ID] [--json]",
				"t3-steward campaign cancel <run>/<task> --reason TEXT [--command-id ID] [--json]",
			},
			Flags: []helpFlag{
				{Name: "--reason", Value: "TEXT", Required: true, Text: "Why the run or the task is being cancelled; recorded with the command."},
				{Name: "--command-id", Value: "ID", Default: "one generated per invocation", Text: "Stable command id, so that a retried cancellation is the same command."},
				jsonFlag("what the cancellation covers"),
			},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"willCancel", "tasks"},
			JSONNote: "--json prints willCancel and the tasks the cancellation covers, which is the plan and not the outcome it applied. " + jsonErrorNote,
			Notes:    "Mutating and revision-fenced. The run form is one command of its own: one revision-fenced cancellation of every non-terminal task, refused on a coordinator too old to apply it. The <run>/<task> form is the forwarded alias of \"t3-steward backlog cancel\".",
			Parsers:  []parserSite{{Func: "parseCampaignCancelRunArgs"}, {Func: "takeJSONFlag"}},
		},
		{Path: "campaign supervision", Body: campaignSupervisionUsage, Parsers: []parserSite{{Func: "parseCampaignSupervisionArgs"}}},
		campaignAliasPage("campaign list", "the campaign runs the coordinator holds, filtered.",
			"t3-steward campaign list [--project P] [--progress STATES] [--class CLASS] [--json]",
			"backlog list"),
		campaignAliasPage("campaign show", "one campaign run with its tasks, plus its supervision projection.",
			"t3-steward campaign show <run> [--json]",
			"backlog show"),
		campaignAliasPage("campaign graph", "one campaign run's task graph, as text, JSON or DOT.",
			"t3-steward campaign graph <run> [--json|--dot]",
			"backlog graph"),
		campaignAliasPage("campaign explain", "why one task of a campaign run is where it is, plus its supervision projection.",
			"t3-steward campaign explain <run>/<task> [--json]",
			"backlog explain"),
	}
}

// campaignAliasPage is a lifecycle verb the campaign namespace forwards to
// backlog untouched. Its arguments are parsed by the backlog verb, so the page
// says which one rather than restating a contract that would drift from it.
func campaignAliasPage(path, purpose, usage, target string) helpPage {
	return helpPage{
		Path:     path,
		Purpose:  purpose,
		Usage:    []string{usage},
		Flags:    []helpFlag{jsonFlag("the answer")},
		Exits:    coordinatorExits(),
		JSONKeys: adminEnvelopeKeys(),
		JSONNote: "The document is the one \"t3-steward " + target + "\" prints; see its --help for the payload keys. " + jsonErrorNote,
		Notes: "Read-only and live. The arguments are forwarded to \"t3-steward " + target +
			"\" untouched and parsed there, so its flags are this verb's flags; parsing them twice would be two contracts to keep in agreement. " +
			"show and explain add the supervision projection underneath the forwarded answer, and print nothing extra for a run that is not supervised.",
		Parsers: []parserSite{{Func: "takeJSONFlag"}},
	}
}
