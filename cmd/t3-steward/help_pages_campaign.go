package main

import "strings"

// The campaign family's second-level pages. The namespace is a facade: submit
// creates exactly one workflow and one run, and every lifecycle verb below is
// an existing backlog operation.

// campaignAuthoringNote is on every page whose arguments pass through
// parseCampaignArgs. One parser serves validate, plan, check and submit, so it
// recognises the whole authoring vocabulary and refuses, by name, the options
// that belong to a sibling verb. Naming them here is what makes this page a
// complete account of what that parser will do with this command line.
const campaignAuthoringNote = "validate, plan, check and submit share one option parser. It also recognises " +
	"--dot, --register-only, --idempotency-key, --task, --allow-unverified, --reason, --notify-thread and --no-notify, and refuses the ones " +
	"that belong to a sibling verb with \"campaign <verb> does not accept <option>\". Any other option is refused " +
	"as unknown."

func campaignHelpPages() []helpPage {
	return []helpPage{
		{Path: "campaign commit", Purpose: "export a declared commit from the coordinator.", Usage: []string{"t3-steward campaign commit export <run>/<task>/<commit-name> --bundle FILE [--branch NAME]"}, Flags: []helpFlag{{Name: "--bundle", Value: "FILE", Required: true, Text: "New local destination. Existing paths are refused."}, {Name: "--branch", Value: "NAME", Default: "commit name", Text: "The bundle advertises exactly refs/heads/NAME."}}, Exits: coordinatorExits(), JSONNote: "--json is not accepted. Export prints commit, base and sha256 as text.", Parsers: []parserSite{{Func: "parseCampaignCommitExportArgs"}}},
		{Path: "campaign commit export", Purpose: "write a verified bundle of a declared commit, read-only on the fleet.", Usage: []string{strings.TrimSpace(campaignCommitExportUsage)}, Flags: []helpFlag{{Name: "--bundle", Value: "FILE", Required: true, Text: "New local destination. Existing paths are refused."}, {Name: "--branch", Value: "NAME", Default: "commit name", Text: "The bundle advertises exactly refs/heads/NAME."}}, Exits: coordinatorExits(), JSONNote: "--json is not accepted. Prints commit, base and sha256 as text.", Notes: "Authenticated and read-only: resolves provenance through the coordinator, reads a retained bundle or the producing worker, and never changes worker refs. Prints commit, base and sha256. The campaign base is the bundle prerequisite; import into a repository that already holds it. Unknown producers, absent provenance, invalid refs, corrupt bundles and old transports are refused.", Parsers: []parserSite{{Func: "parseCampaignCommitExportArgs"}}},
		{
			Path:     "campaign validate",
			Purpose:  "check a campaign directory's structure, graph and size, offline.",
			Usage:    []string{"t3-steward campaign validate <directory|workflow.yaml> [--json]"},
			Flags:    []helpFlag{jsonFlag("the validation summary")},
			Exits:    []helpExit{{0, "the campaign is valid"}, {1, "no source, more than one source, an unknown option, or an invalid campaign"}},
			JSONKeys: []string{"schemaVersion", "valid", "source", "name", "tasks", "edges", "roots", "leaves", "inputFiles", "files", "bytes", "digest"},
			JSONNote: "Read schemaVersion first. No transport class applies: this verb has none. An invalid campaign under --json prints the error envelope with class \"error\" and this document's schemaVersion on standard output.",
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
		campaignCompileHelpPage(),
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
			Purpose: "submit one campaign run, or register a definition for schedules with --register-only.",
			Usage:   []string{"t3-steward campaign submit <directory|workflow.yaml> --idempotency-key KEY [--register-only] [--allow-unverified --reason TEXT] [--notify-thread current|<id>|--no-notify] [--json]"},
			Flags: []helpFlag{
				{Name: "--register-only", Default: "off", Text: "Retain the workflow definition for schedules without starting a run or registering a wake. Refuses supervised manifests until scheduled supervision is implemented. Changing this mode under an existing key is refused."},
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
			Notes:    "Mutating. It runs check first unless --allow-unverified. accepted_waiting is a success: the run exists and stays queued, so end the turn. The calling thread is notified by default; --no-notify submits a campaign nobody is woken for. The text record opens with run <id>, as \"task run\" does, and its next: lines name \"t3-steward task result <run>\", which collects the outcome. A caller with no thread polls \"campaign show <run>\"; \"task result\" exits 1 until the run is terminal.\n\n" + campaignAuthoringNote,
			Parsers:  []parserSite{{Func: "parseCampaignArgs"}},
		},
		{
			Path:    "campaign rerun",
			Purpose: "create a second run from one task; --use-commit reuses a failed attempt's retained commit after verification failure.",
			Usage:   []string{"t3-steward campaign rerun <run> --from TASK --idempotency-key KEY [--prompt TEXT] [--reason TEXT] [--use-commit] [--json]"},
			Flags: []helpFlag{
				{Name: "--from", Value: "TASK", Required: true, Text: "The task to start again from. Everything that depends on it is run again."},
				{Name: "--idempotency-key", Value: "KEY", Required: true, Text: "The key the rerun is recorded under. Repeating it with the same content returns the same run."},
				{Name: "--prompt", Value: "TEXT", Default: "source prompt", Text: "Corrected instructions for the selected root task only; retained as a new immutable input."},
				{Name: "--reason", Value: "TEXT", Default: "none", Text: "Why the campaign is being rerun; recorded with the amendment."},
				{Name: "--use-commit", Default: "false", Text: "Reuse retained commits from a failed attempt whose only failures were verification; requires coordinator 0.11.0-rc.118 or newer."},
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
				blockingWaitFlags()[0], blockingWaitFlags()[1],
			},
			Exits:    append(coordinatorExits(), helpExit{1, "local wait timeout"}, helpExit{130, "wait interrupted"}),
			JSONKeys: []string{"willCancel", "tasks", "run", "outcome"},
			JSONNote: "--json prints willCancel and the tasks the cancellation covers, which is the plan and not the outcome it applied. With --wait, the run form prints run and outcome, the observed command including its state and failure. " + jsonErrorNote,
			Notes:    "Mutating and revision-fenced. The run form is one command of its own: one revision-fenced cancellation of every non-terminal task, refused on a coordinator too old to apply it. On a supervised run the same application resolves every open incident as cancelled, cancels undecided gates, releases holds and settles the sink when nothing is still running; it is refused while an overseer activation is live. A run whose tasks are all terminal but whose sink is open is closed the same way, fenced on the run's revision; a settled run is refused. The <run>/<task> form is the forwarded alias of \"t3-steward backlog cancel\"." + blockingWaitHelp,
			Parsers:  []parserSite{{Func: "parseCampaignCancelRunArgs"}, {Func: "takeJSONFlag"}, {Func: "Parse"}},
		},
		{Path: "campaign recovery", Purpose: "retry a failed supervised operation using fenced evidence.", Usage: []string{"t3-steward campaign recovery <command> [flags]"}, Exits: coordinatorExits(), JSONNote: "The retry command always prints a JSON recovery receipt.", Notes: "See t3-steward campaign recovery retry --help full for its evidence contract."},
		{Path: "campaign recovery retry", Body: campaignRecoveryUsage + "\nRetry one supervised operation using incident, graph and attempt revisions.\n\nAll flags in the synopsis are required except --checkpoint-artifact, which may\nbe repeated. Optional " + supervisorCredentialFlag + " NAME chooses a supervisor\ncredential. --config PATH selects configuration.\nThe result is always a JSON recovery receipt; no --json flag is needed.\nExit: 0 accepted; 1 invalid evidence/options; coordinator transport exits apply.\n" + coordinatorTransportSummary, Parsers: []parserSite{{Func: "runRecovery"}}},
		{Path: "campaign supervision", Body: campaignSupervisionUsage, Parsers: []parserSite{{Func: "parseCampaignSupervisionArgs"}}},
		func() helpPage {
			page := campaignAliasPage("campaign list", "the campaign runs the coordinator holds, filtered.",
				"t3-steward campaign list [--project P] [--state open|terminal|--progress STATES] [--class CLASS] [--limit N] [--since DURATION] [--thread current|ID] [--json]",
				"backlog list")
			// The window flags are the ones an agent misreads: 0 is not "none".
			for _, reference := range backlogHelpPages() {
				if reference.Path == "backlog list" {
					page.Flags, page.Parsers = reference.Flags, reference.Parsers
					break
				}
			}
			page.JSONKeys = []string{"schemaVersion", "version", "kind", "generatedAt", "workflows"}
			page.JSONNote = "schemaVersion 1; workflows is always an array, including when empty. See docs/cli-output.md."
			page.Notes += " --limit 0 prints every run; unset, the text form prints the newest 50 and --json every one. " +
				"--since takes a Go duration or a whole number of days, such as 24h or 7d. " + listThreadNote
			return page
		}(),
		{
			Path: "campaign progress", Purpose: "compact read-only progress mirror from coordinator facts.",
			Usage:    []string{"t3-steward campaign progress [<run>...] [--owner THREAD] [--since RFC3339] [--json]"},
			Flags:    []helpFlag{{Name: "--owner", Value: "THREAD", Default: "all notify threads", Text: "Filter by the recorded notify thread, including settled notifications."}, {Name: "--since", Value: "RFC3339", Default: "open runs and terminal runs from the last 24 hours", Text: "Inclusive last-change timestamp; replaces the default 24-hour terminal window."}, {Name: "--json", Default: "false", Text: "Print schemaVersion 1 with complete values."}},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"schemaVersion", "generatedAt", "runs"},
			JSONNote: "runs and each tasks/outputs field are arrays, including when empty; see docs/backlog-v2-operations.md.",
			Notes:    "Without run IDs, lists open runs and runs terminal in the last 24 hours. Explicit IDs bypass that default window. No result bodies or state writes. Text rows are at most 120 terminal columns; non-ASCII values are escaped. An older coordinator is refused; upgrade it.",
			Parsers:  []parserSite{{Func: "parseProgressArgs"}},
		},
		{
			Path:    "campaign result",
			Purpose: "one short block: run state, one line per task and whether review ACCEPT and verification passed on the same commit.",
			Usage:   []string{"t3-steward campaign result <run> [--json] [--wait [--timeout D]]"},
			Flags:   append([]helpFlag{jsonFlag("the versioned result document")}, blockingWaitFlags()...),
			Exits: append(coordinatorExits(),
				helpExit{2, "the run failed or was cancelled; the result is still printed"},
				helpExit{130, "wait interrupted"}),
			JSONKeys: []string{"schemaVersion", "run", "state", "terminal", "tasks", "acceptance"},
			JSONNote: "schemaVersion is \"t3-steward.campaign-result/v1\". Each task has task, taskId, attemptId, state, failureClass, failure, verdict, reviewGate, commits, verification and reviewed; acceptance has accepted, commit, reviewTask, verifiedTask and reason. See docs/campaign-result.md.",
			Notes: "Read-only. Each task line gives its state, the failure class and failure, the review verdict recorded for its latest attempt (and the commits that verdict is about), the review gate, the declared commit and the verification result. " +
				"The acceptance line is yes only when a recorded ACCEPT and the recorded passing verification name one and the same commit: a review task's ACCEPT is about the commit outputs it consumed, whose producing attempt must have succeeded with every declared verification command reported at exit 0 (and a declared gate passed); a review-declared task's gate passed as accepted-head binds its reviewed head to its declared commit and its own verification. " +
				"It is never inferred from exit codes or task success alone, and a recorded CHANGES_REQUESTED on the same commit makes it no. " +
				"The exit code is the run's: 0 succeeded, 2 failed or cancelled, 1 not terminal yet; acceptance is in the output, not the exit code. " +
				"--wait polls until the run is terminal, backing off from 100ms to 10s; --timeout D bounds it, prints the last state and exits 1. Interrupting never cancels work; reattach with campaign result <run> --wait. " +
				"\"campaign show\" is the full run document and \"task result\" collects a task's files; this verb is the decision summary of both.",
			Parsers: []parserSite{{Func: "runResult"}, {Func: "Parse"}},
		},
		campaignAliasPage("campaign status", "alias of campaign show: one run with its tasks and supervision.",
			"t3-steward campaign status <run> [--json]", "backlog show"),
		func() helpPage {
			page := campaignAliasPage("campaign show", "one campaign run with its tasks, plus its supervision projection.",
				"t3-steward campaign show <run> [--json] [--wait [--timeout D]]", "backlog show")
			page.Flags = append(page.Flags, blockingWaitFlags()...)
			page.Parsers = append(page.Parsers, parserSite{Func: "Parse"})
			page.Notes = "Read-only and live. --wait waits for a terminal run before printing the ordinary show output and supervision projection. On timeout or interrupt it prints no completed result; run campaign show <run> --wait again to reattach." + blockingWaitHelp
			page.Exits = append(page.Exits, helpExit{1, "local wait timeout"}, helpExit{130, "wait interrupted"})
			return page
		}(),
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
