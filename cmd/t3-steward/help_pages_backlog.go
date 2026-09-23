package main

// The backlog family's second-level pages. Every verb here reaches the
// coordinator except the backup and legacy task-file helpers, which say so.

// backlogReadSites are the parser regions a coordinator read verb's arguments
// pass through: the verb's own case in the query parser, and the two wrappers
// every read shares.
func backlogReadSites(verb string) []parserSite {
	return []parserSite{
		{Func: "parseBacklogAdminQueryWithoutSink", Case: verb},
		{Func: "takeJSONFlag"},
	}
}

// backlogSinkSites adds the wrapper that owns --include-sink, which status,
// list and show are the three verbs to accept.
func backlogSinkSites(verb string) []parserSite {
	return append(backlogReadSites(verb), parserSite{Func: "parseBacklogAdminQuery"})
}

var includeSinkFlag = helpFlag{
	Name:    "--include-sink",
	Default: "off",
	Text:    "Include the run sink, the synthetic node that settles a run, in the answer. Only status, list and show accept it.",
}

// amendmentFlags is the option set of every graph amendment. One parser serves
// task add, task set, edge add, edge remove and run clone, so all five accept
// all of them; which ones mean anything is decided by the operation.
func amendmentFlags() []helpFlag {
	return []helpFlag{
		{Name: "--expected-revision", Value: "N", Required: true, Text: "The graph revision this amendment is fenced against. A run that changed under you is refused."},
		{Name: "--request-id", Value: "ID", Required: true, Text: "Stable idempotency id. The same id with the same content replays; with different content it is refused."},
		{Name: "--reason", Value: "TEXT", Required: true, Text: "Why the graph is being amended. It is recorded with the amendment."},
		{Name: "--from", Value: "REF", Default: "none", Text: "edge add and edge remove: the dependency, <task> or <other-run>/<task>. run clone: the run to clone."},
		{Name: "--provider", Value: "INSTANCE", Default: "none", Text: "task add and task set: the provider instance the task runs on."},
		{Name: "--model", Value: "MODEL", Default: "none", Text: "task add and task set: the model."},
		{Name: "--prompt", Value: "TEXT", Default: "none", Text: "task add: the task prompt."},
		{Name: "--verify", Value: "COMMAND", Default: "none", Text: "The verification command; repeatable. task set replaces the whole list."},
		{Name: "--needs", Value: "NAMES", Default: "none", Text: "task add: comma-separated dependencies, <task> or <other-run>/<task>."},
		{Name: "--options", Value: "JSON", Default: "none", Text: "task add and task set: the task options document."},
		{Name: "--timeout", Value: "DURATION", Default: "the project default", Text: "task add and task set: the attempt timeout."},
		{Name: "--class", Value: "required|surplus", Default: "surplus", Text: "task add: whether the task is admitted first or runs on spare quota."},
		{Name: "--max-turns", Value: "N", Default: "1", Text: "task add: the turn budget of the attempt. An amendment defaults it to 1, unlike a submitted manifest, where an unset budget becomes 3."},
		jsonFlag("the amended run and graph"),
	}
}

func amendmentPage(path, purpose, usage string) helpPage {
	return helpPage{
		Path:     path,
		Purpose:  purpose,
		Usage:    []string{usage},
		Flags:    amendmentFlags(),
		Exits:    coordinatorExits(),
		JSONKeys: []string{"run", "graph", "replay"},
		JSONNote: "This verb always prints JSON. " + jsonErrorNote,
		Notes:    "Mutating and revision-fenced. One parser serves all five amendments, so all five accept the whole flag set above; an option the operation has no use for is ignored by it.",
		Parsers:  []parserSite{{Func: "parseGraphAmendment"}},
	}
}

// mutationFlags is the option set of every revision-fenced control.
func mutationFlags() []helpFlag {
	return []helpFlag{
		{Name: "--reason", Value: "TEXT", Required: true, Text: "Why the control is being issued. It is recorded with the command."},
		{Name: "--command-id", Value: "ID", Default: "one generated per invocation", Text: "Stable command id, so that a retried control is the same command rather than a second one."},
		{Name: "--expected-revision", Value: "N", Default: "the attempt revision this client reads first", Text: "Fence the control against a named attempt revision. Without it the client reads the current revision and re-reads it once after a stale-revision rejection."},
		{Name: "--until", Value: "RFC3339", Default: "none", Text: "delay only: when the task becomes eligible again. Required by delay."},
		{Name: "--now", Default: "off", Text: "pause only: pause the running attempt immediately instead of at the next boundary."},
		jsonFlag("the command and the event it produced"),
	}
}

func mutationPage(path, purpose, usage string) helpPage {
	return helpPage{
		Path:     path,
		Purpose:  purpose,
		Usage:    []string{usage},
		Flags:    mutationFlags(),
		Exits:    coordinatorExits(),
		JSONKeys: []string{"version", "command", "event", "currentTarget"},
		JSONNote: jsonErrorNote,
		Notes:    "Mutating and revision-fenced. One parser serves all eight controls, so all eight accept the whole flag set above; --until belongs to delay and --now to pause, and the others refuse them.",
		Parsers:  []parserSite{{Func: "parseBacklogMutation"}, {Func: "parseMutationOptions"}, {Func: "takeJSONFlag"}},
	}
}

// projectsReadPage is the read page of "backlog projects", which is the one
// backlog read whose document has two shapes: the summary both forms print by
// default, and the whole catalog that --verbose and a one-project answer print
// instead. Both key sets are stated, because a page that named one of them
// would be false half the time it is read.
func projectsReadPage() helpPage {
	page := backlogReadPage("backlog projects", "the configured projects and, per project, the eligible workers and the routes they advertise.",
		"t3-steward backlog projects [--project NAME] [--verbose] [--json]",
		[]helpFlag{
			{Name: "--project", Value: "NAME", Default: "every project", Text: "Report one project only. It narrows what the coordinator reads, so the answer is one project in full rather than a slice of the catalog."},
			{Name: "--verbose", Default: "off", Text: "Spell out every project's eligible workers and the routes each advertises. Without it, and without --project, both forms are one row per project with the worker and route counts, because the catalog spelled out is tens of kilobytes on a real fleet. --json honours it the same way: the summarised document carries the counts and the totals, and --verbose --json is the whole catalog."},
		},
		[]string{"summarised", "totalProjects", "totalWorkerRows", "totalRoutes", "projects", "detail"},
		append(backlogReadSites("projects"), parserSite{Func: "takeVerboseFlag"}),
		"Read-only. Both forms summarise the whole catalog and say so, with the counts and the two ways to get the detail; one project, named or the only one, is always printed in full. \"t3-steward task run\" derives its project and route from this answer, and \"t3-steward models\" reports the same routes joined with their quota state.")
	page.JSONNote = "Those keys are the summarised document's. --verbose --json, and an answer that holds one project, print version, kind, generatedAt and projects, where every project carries its eligible workers and their routes. " + jsonErrorNote
	return page
}

// backlogReadPage is a coordinator read: one query, no change, one document.
func backlogReadPage(path, purpose, usage string, extra []helpFlag, keys []string, sites []parserSite, notes string) helpPage {
	return helpPage{
		Path:     path,
		Purpose:  purpose,
		Usage:    []string{usage},
		Flags:    append(extra, jsonFlag("the answer")),
		Exits:    coordinatorExits(),
		JSONKeys: adminEnvelopeKeys(keys...),
		JSONNote: jsonErrorNote,
		Notes:    notes,
		Parsers:  sites,
	}
}

func backlogHelpPages() []helpPage {
	pages := []helpPage{
		{
			Path:    "backlog submit",
			Purpose: "send one packed workflow bundle to the coordinator.",
			Usage:   []string{"t3-steward backlog submit <bundle.tar> [--idempotency-key KEY] [--json]"},
			Flags: []helpFlag{
				{Name: "--idempotency-key", Value: "KEY", Default: "the bundle's content digest", Text: "The key this submission is recorded under. The same key with the same bundle returns the same run; the same key with different content is refused."},
				jsonFlag("the submission receipt"),
			},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"key", "digest", "workflowId", "runId", "state", "acceptedAt", "replay"},
			JSONNote: jsonErrorNote,
			Notes:    "Mutating: it creates one workflow and one run. To author a workflow as a directory rather than pack a tar yourself, use \"t3-steward campaign submit\".",
			Parsers:  []parserSite{{Func: "parseSubmissionArgs"}},
		},
		{
			Path:     "backlog backup",
			Purpose:  "snapshot commands for a stopped coordinator: create, verify and restore.",
			Usage:    []string{"t3-steward backlog backup create|verify|restore <snapshot-directory>"},
			Exits:    localExits(),
			JSONKeys: []string{"version", "schemaVersion", "createdAt", "files"},
			JSONNote: "These verbs always print the snapshot manifest as JSON; they have no text rendering and no --json flag.",
			Notes:    "Offline: they read and write the coordinator's state database and artifact tree directly and reach no coordinator. They require backlog_v2.mode: coordinator, and the coordinator must be stopped.",
		},
		{
			Path:    "backlog task",
			Purpose: "the task verbs of the backlog family: show, add and set.",
			Usage: []string{
				"t3-steward backlog task show <workflow-run>/<task> [--json]",
				"t3-steward backlog task add <run>/<name> [amendment flags]",
				"t3-steward backlog task set <run>/<task> [amendment flags]",
			},
			Exits:    coordinatorExits(),
			JSONNote: "Each verb prints its own document; run its --help.",
			Notes:    "show is a read; add and set are revision-fenced graph amendments. To start a task on the fleet from a checkout, the verb is \"t3-steward task run\", which is a different family.",
		},
		{
			Path:     "backlog artifact",
			Purpose:  "the single-artifact verbs: show its metadata, get its bytes.",
			Usage:    []string{"t3-steward backlog artifact show <artifact> [--json]", "t3-steward backlog artifact get <artifact> [--output PATH]"},
			Exits:    coordinatorExits(),
			JSONNote: "Each verb prints its own document; run its --help.",
			Notes:    "To list the artifacts of a task, the verb is \"t3-steward backlog artifacts\".",
		},
		{
			Path:     "backlog command",
			Purpose:  "show one admin command by id.",
			Usage:    []string{"t3-steward backlog command show <command> [--json]"},
			Exits:    coordinatorExits(),
			JSONNote: "See \"t3-steward backlog command show --help\".",
			Notes:    "To list the commands of a run or a task, the verb is \"t3-steward backlog commands\".",
		},
		{
			Path:     "backlog edge",
			Purpose:  "add or remove one dependency edge of a run's graph.",
			Usage:    []string{"t3-steward backlog edge add|remove <run>/<task> --from <task|other-run/task> [amendment flags]"},
			Exits:    coordinatorExits(),
			JSONNote: "See \"t3-steward backlog edge add --help\".",
			Notes:    "Both forms are revision-fenced graph amendments.",
		},
		{
			Path:     "backlog run",
			Purpose:  "the run-level graph amendment: clone an existing run.",
			Usage:    []string{"t3-steward backlog run clone --from <run> [amendment flags]"},
			Exits:    coordinatorExits(),
			JSONNote: "See \"t3-steward backlog run clone --help\".",
			Notes:    "clone is the only verb under run. To start a run from a campaign directory, use \"t3-steward campaign submit\"; to start one task, \"t3-steward task run\".",
		},
		{
			Path:    "backlog artifact get",
			Purpose: "fetch one artifact's bytes, to a file or to standard output.",
			Usage:   []string{"t3-steward backlog artifact get <artifact> [--output PATH]"},
			Flags: []helpFlag{
				{Name: "--output", Value: "PATH", Default: "standard output", Text: "Write the artifact to this path. Required for a media type that is not text and for an artifact larger than 16 MiB, both of which are refused inline."},
			},
			Exits:    coordinatorExits(),
			JSONNote: "This verb prints the artifact's bytes, not a document, and has no --json flag. Its metadata is \"backlog artifact show\".",
			Notes:    "Read-only. With --output it writes exactly one file on this host and nothing else.",
			Parsers:  []parserSite{{Func: "runArtifactGet"}},
		},
		{
			Path:    "backlog quarantine release",
			Purpose: "clear one intake quarantine after fixing what caused it.",
			Usage:   []string{"t3-steward backlog quarantine release <key> --reason TEXT [--json]"},
			Flags: []helpFlag{
				{Name: "--reason", Value: "TEXT", Required: true, Text: "Why the quarantine is released. The operator, not the file, is what changed, so the reason is recorded with the release."},
				jsonFlag("the release record"),
			},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"key", "released", "digest", "reason", "releasedAt"},
			JSONNote: jsonErrorNote,
			Notes:    "Mutating. Editing the refused file clears its quarantine by itself, because the automatic release is bound to the content digest; this verb is for a refusal the file cannot fix, such as a project no alias mapped.",
			Parsers:  []parserSite{{Func: "runQuarantineRelease"}, {Func: "takeJSONFlag"}},
		},
		{
			Path:    "backlog recover",
			Purpose: "settle one assignment the coordinator cannot account for, on stated evidence.",
			Usage:   []string{"t3-steward backlog recover <assignment> --outcome stopped|failed --coordinator-epoch N --assignment-epoch N --attempt-revision N --evidence-id ID --evidence-sha256 HEX --reason TEXT [--recovery-id ID] [--json]"},
			Flags: []helpFlag{
				{Name: "--outcome", Value: "stopped|failed", Required: true, Text: "What the assignment is recorded as having done."},
				{Name: "--coordinator-epoch", Value: "N", Required: true, Text: "The coordinator epoch the recovery is fenced against."},
				{Name: "--assignment-epoch", Value: "N", Required: true, Text: "The assignment epoch the recovery is fenced against."},
				{Name: "--attempt-revision", Value: "N", Required: true, Text: "The attempt revision the recovery is fenced against."},
				{Name: "--evidence-id", Value: "ID", Required: true, Text: "The identifier of the evidence the outcome rests on."},
				{Name: "--evidence-sha256", Value: "HEX", Required: true, Text: "The evidence's content hash, so the record names bytes rather than a claim."},
				{Name: "--reason", Value: "TEXT", Required: true, Text: "Why the assignment is being settled by hand."},
				{Name: "--recovery-id", Value: "ID", Default: "one generated per invocation", Text: "Stable id, so that a retried recovery replays rather than settling twice."},
				jsonFlag("the recovery decision"),
			},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"recovery", "assignment", "attempt", "event", "replay"},
			JSONNote: jsonErrorNote,
			Notes:    "Mutating, fenced three ways and audited. It is the last resort for an assignment whose worker is gone; every other control is under the revision-fenced verbs.",
			Parsers:  []parserSite{{Func: "parseUnknownRecovery"}},
		},
		{
			Path:     "backlog new",
			Purpose:  "create a legacy task file from a template and print its path.",
			Usage:    []string{"t3-steward backlog new <id>"},
			Exits:    []helpExit{{0, "written"}, {1, "no id, the file already exists, or the directory could not be created"}},
			JSONNote: "This verb prints no JSON document; it prints the path it wrote.",
			Notes:    "Offline: it writes one file under the local backlog directory and reaches no coordinator. The legacy task-file runner is part of \"run\" and is enabled with backlog.enabled. For fleet work the verb is \"t3-steward task run\".",
			Parsers:  []parserSite{{Func: "runBacklogLegacy", Case: "new"}},
		},
		{
			Path:     "backlog path",
			Purpose:  "print the local legacy task directory.",
			Usage:    []string{"t3-steward backlog path"},
			Exits:    localExits(),
			JSONNote: "This verb prints no JSON document; it prints one path.",
			Notes:    "Offline: it resolves a path from the configuration and reads nothing. The path it prints is the one the configuration names, so --config moves it.",
			Parsers:  []parserSite{{Func: "runBacklogLegacy", Case: "path"}},
		},
		{
			Path:     "backlog check",
			Purpose:  "validate a legacy task file: project, provider instance, model, options and host.",
			Usage:    []string{"t3-steward backlog check <file|->"},
			Exits:    []helpExit{{0, "the task is valid"}, {1, "no path given, the file is unreadable, or the task is invalid"}},
			JSONNote: "This verb prints no JSON document; it prints what it resolved and what it refused.",
			Notes:    "Offline: it reads one file, or standard input when the path is -, and reaches no coordinator. This is the legacy task-file checker; the campaign equivalent is \"t3-steward campaign validate\".",
			Parsers:  []parserSite{{Func: "runBacklogLegacy", Case: "check"}},
		},
		{
			Path:     "backlog receive",
			Purpose:  "store a legacy task another host forwarded to this one.",
			Usage:    []string{"t3-steward backlog receive <id>"},
			Exits:    []helpExit{{0, "stored"}, {1, "no id, or the task on standard input does not parse"}},
			JSONNote: "This verb prints no JSON document.",
			Notes:    "Offline, and not an operator verb: forwarding invokes it over SSH with the task on standard input.",
			Parsers:  []parserSite{{Func: "runBacklogLegacy", Case: "receive"}},
		},
	}

	for _, backup := range []struct{ verb, purpose string }{
		{"create", "write a consistent snapshot of a stopped coordinator's state and artifacts."},
		{"verify", "check a snapshot directory against its own manifest."},
		{"restore", "restore a stopped coordinator's state and artifacts from a snapshot."},
	} {
		pages = append(pages, helpPage{
			Path:     "backlog backup " + backup.verb,
			Purpose:  backup.purpose,
			Usage:    []string{"t3-steward backlog backup " + backup.verb + " <snapshot-directory>"},
			Exits:    []helpExit{{0, "done"}, {1, "not a coordinator configuration, the coordinator is running, or the snapshot is unreadable or does not match its manifest"}},
			JSONKeys: []string{"version", "schemaVersion", "createdAt", "files"},
			JSONNote: "This verb always prints the snapshot manifest as JSON; it has no text rendering and no --json flag.",
			Notes:    "Offline: it reads and writes the state database and the artifact tree directly and reaches no coordinator. It requires backlog_v2.mode: coordinator. Exactly one snapshot directory argument.",
			Parsers:  []parserSite{{Func: "runBacklogBackup"}},
		})
	}

	pages = append(pages,
		backlogReadPage("backlog status", "the coordinator's own state: its identity, epoch, health and current load.",
			"t3-steward backlog status [--include-sink] [--json]",
			[]helpFlag{includeSinkFlag}, []string{"status"}, backlogSinkSites("status"),
			"Read-only. It takes no positional argument. For this host's quota buckets instead, the verb is \"t3-steward status\"."),
		projectsReadPage(),
		backlogReadPage("backlog workers", "every worker the coordinator knows, with its enrolment, catalog and readiness.",
			"t3-steward backlog workers [--json]",
			nil, []string{"workers"}, backlogReadSites("workers"),
			"Read-only. It takes no positional argument. \"t3-steward worker list\" is an alias of this verb; \"t3-steward worker enroll\" is what fixes a stale enrolment."),
		backlogReadPage("backlog list", "the workflow runs the coordinator holds, filtered.",
			"t3-steward backlog list [--project P] [--schedule S] [--progress STATES] [--class CLASS] [--worker W] [--quota-pool Q] [--include-sink] [--json]",
			[]helpFlag{
				{Name: "--project", Value: "P", Default: "every project", Text: "Only runs of this project."},
				{Name: "--schedule", Value: "S", Default: "every run", Text: "Only runs this schedule triggered."},
				{Name: "--progress", Value: "STATES", Default: "every state", Text: "Comma-separated progress states: queued, blocked, ready, active, needs-input, waiting-external, verifying, succeeded, failed, cancelled, skipped."},
				{Name: "--class", Value: "required|surplus", Default: "both", Text: "Only tasks of this class."},
				{Name: "--worker", Value: "W", Default: "every worker", Text: "Only work assigned to this worker."},
				{Name: "--quota-pool", Value: "Q", Default: "every pool", Text: "Only work charged to this quota pool."},
				includeSinkFlag,
			},
			[]string{"workflows"}, append(backlogSinkSites("list"), parserSite{Func: "parseWorkflowFilters"}),
			"Read-only. \"backlog list --all\", with no other argument, is a different verb: the offline legacy task-file listing."),
		backlogReadPage("backlog show", "one workflow run with its tasks, attempts and waits.",
			"t3-steward backlog show <workflow-run> [--include-sink] [--json]",
			[]helpFlag{includeSinkFlag}, []string{"workflow"}, backlogSinkSites("show"),
			"Read-only. Exactly one workflow-run id. When a run has failed, \"backlog diagnose\" joins the same run with its worker journal and wait evidence."),
		backlogReadPage("backlog graph", "one run's task graph, as text, JSON or DOT.",
			"t3-steward backlog graph <workflow-run> [--json|--dot]",
			[]helpFlag{{Name: "--dot", Default: "off", Text: "Print the graph in Graphviz DOT instead of the text rendering. Mutually exclusive with --json."}},
			[]string{"graph"}, append(backlogReadSites("graph"), parserSite{Func: "runBacklog"}),
			"Read-only. Exactly one workflow-run id."),
		backlogReadPage("backlog diagnose", "one run's graph, tasks, assignments, worker journal and wait evidence, joined.",
			"t3-steward backlog diagnose <workflow-run> [--json]",
			nil, []string{"diagnosis"}, backlogReadSites("diagnose"),
			"Read-only. Exactly one workflow-run id. \"t3-steward diagnose <run>\" is the same verb spelled at the top level."),
		backlogReadPage("backlog task show", "one task of one run, with its attempts.",
			"t3-steward backlog task show <workflow-run>/<task> [--json]",
			nil, []string{"task"}, backlogReadSites("task"),
			"Read-only. The target is <workflow-run>/<task>; the run alone is refused, and \"backlog show <run>\" is what lists a run's tasks."),
		backlogReadPage("backlog events", "the audit events of one run, oldest first.",
			"t3-steward backlog events <workflow-run> [--json]",
			nil, []string{"events"}, backlogReadSites("events"),
			"Read-only. Exactly one workflow-run id. Intake the coordinator quarantined names no run and does not appear here; \"backlog quarantine\" is where it is."),
		backlogReadPage("backlog usage", "bounded normalized provider usage joined to authoritative V2 execution identity and outcomes.",
			"t3-steward backlog usage <workflow-run> [--raw] [--limit 1..200] [--cursor C] [--json]",
			nil, []string{"usageReport", "usageSemantics"}, backlogReadSites("usage"),
			"Read-only and report-only. The default contains aggregates, coverage and freshness but no raw history. --raw pages audit detail with an opaque run-bound keyset cursor; whole-turn evidence supersedes only calls carrying its exact causal boundary."),
		backlogReadPage("backlog explain", "why one task is where it is: its dependencies, admission and blockers.",
			"t3-steward backlog explain <workflow-run>/<task> [--json]",
			nil, []string{"explanation"}, backlogReadSites("explain"),
			"Read-only and dynamic. The target is <workflow-run>/<task>. Its static counterpart, before a run exists, is \"t3-steward campaign plan\"."),
		backlogReadPage("backlog artifacts", "the artifacts one task produced.",
			"t3-steward backlog artifacts [<task>|<workflow-run>/<task>] [--json]",
			nil, []string{"artifacts"}, backlogReadSites("artifacts"),
			"Read-only. With no target it lists every artifact the coordinator holds, which on a busy fleet is a long answer; name the task. One artifact's bytes are \"backlog artifact get\"."),
		backlogReadPage("backlog artifact show", "one artifact's metadata: its task, media type, size and digest.",
			"t3-steward backlog artifact show <artifact> [--json]",
			nil, []string{"artifact"}, backlogReadSites("artifact"),
			"Read-only. The show word is required; the bytes are \"backlog artifact get\"."),
		backlogReadPage("backlog commands", "the admin commands issued against one run or one task.",
			"t3-steward backlog commands [<workflow-run>[/<task>]] [--json]",
			nil, []string{"commands"}, backlogReadSites("commands"),
			"Read-only. With no target it lists every command the coordinator holds."),
		backlogReadPage("backlog command show", "one admin command by id, with its state and failure.",
			"t3-steward backlog command show <command> [--json]",
			nil, []string{"commands"}, backlogReadSites("command"),
			"Read-only. The show word is required."),
		backlogReadPage("backlog quarantine", "intake the coordinator refused permanently and is now silent about.",
			"t3-steward backlog quarantine [--json]",
			nil, []string{"quarantine"}, backlogReadSites("quarantine"),
			"Read-only. It takes no positional argument. A quarantined intake names no run, so \"backlog events\" cannot show it. \"backlog quarantine release\" clears one."),
		amendmentPage("backlog task add", "add one task to a run's graph.",
			"t3-steward backlog task add <run>/<name> --provider INSTANCE --model MODEL --prompt TEXT --verify COMMAND [--needs NAMES] [--options JSON] [--timeout DURATION] [--class required|surplus] [--max-turns N] --expected-revision N --request-id ID --reason TEXT"),
		amendmentPage("backlog task set", "change one task of a run's graph; --verify replaces the whole verification list.",
			"t3-steward backlog task set <run>/<task> [--model MODEL] [--provider INSTANCE] [--options JSON] [--timeout DURATION] [--verify COMMAND] --expected-revision N --request-id ID --reason TEXT"),
		amendmentPage("backlog edge add", "add one dependency edge to a run's graph.",
			"t3-steward backlog edge add <run>/<task> --from <task|other-run/task> --expected-revision N --request-id ID --reason TEXT"),
		amendmentPage("backlog edge remove", "remove one dependency edge from a run's graph.",
			"t3-steward backlog edge remove <run>/<task> --from <task|other-run/task> --expected-revision N --request-id ID --reason TEXT"),
		amendmentPage("backlog run clone", "clone an existing run into a new one with the same graph.",
			"t3-steward backlog run clone --from <run> --expected-revision N --request-id ID --reason TEXT"),
		mutationPage("backlog start", "start one task now, ahead of its ordinary admission.",
			"t3-steward backlog start <workflow-run>/<task> --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
		mutationPage("backlog resume", "resume one paused task.",
			"t3-steward backlog resume <workflow-run>/<task> --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
		mutationPage("backlog cancel", "cancel one task.",
			"t3-steward backlog cancel <workflow-run>/<task> --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
		mutationPage("backlog retry", "retry one failed task as a new attempt.",
			"t3-steward backlog retry <workflow-run>/<task> --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
		mutationPage("backlog skip", "skip one task and release what depends on it.",
			"t3-steward backlog skip <workflow-run>/<task> --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
		mutationPage("backlog delay", "hold one task until a stated instant.",
			"t3-steward backlog delay <workflow-run>/<task> --until RFC3339 --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
		mutationPage("backlog pause", "pause one task, at the next boundary or immediately.",
			"t3-steward backlog pause <workflow-run>/<task> [--now] --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
		mutationPage("backlog rewake", "wake an attempt left waiting-external after its task wait was cancelled or settled without reaching it.",
			"t3-steward backlog rewake <workflow-run>/<task> --reason TEXT [--expected-revision N] [--command-id ID] [--json]"),
	)

	// "t3-steward diagnose <run>" is the same verb as "backlog diagnose", named
	// at the top level because a failed run is what an operator arrives with.
	// Its path has no family word in it, so the renderer's shared option and
	// the derived family site do not reach it the way they reach every other
	// verb of the family; the dispatcher's clause names diagnose and strips
	// --config for it all the same, so the page states it and the site is
	// declared here rather than derived.
	top := backlogReadPage("diagnose", "one run's graph, tasks, assignments, worker journal and wait evidence, joined.",
		"t3-steward diagnose <workflow-run> [--config PATH] [--json]",
		[]helpFlag{familyConfigFlag("backlog")}, []string{"diagnosis"},
		append(backlogReadSites("diagnose"), familyDispatchSite("diagnose")),
		"Read-only. Exactly one workflow-run id. The dispatcher routes it into the backlog family, so it is the same verb as \"t3-steward backlog diagnose\".")
	return append(pages, top)
}
