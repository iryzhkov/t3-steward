package main

// The second-level pages of the remaining families: wait, task, thread,
// bucket, archive, coordinator, worker and schedules.

func fleetHelpPages() []helpPage {
	pages := []helpPage{
		{
			Path:    "wait add",
			Purpose: "register one wait and end the turn; the steward wakes the thread when it settles.",
			Usage: []string{
				"t3-steward wait add --task current [flags] <condition>   park this task",
				"t3-steward wait add [flags] <condition>                  interactive wait on this thread",
				"conditions: -- <command...> | --at RFC3339 | --for DURATION | --github run <id>|pr <n> | --node <run>[/<task>] | --quota <pool> (--below N|--phase normal|--reset)",
			},
			Flags: []helpFlag{
				{Name: "--task", Value: "current", Default: "an interactive wait", Text: "Park the task this thread is executing, in waiting-external, instead of registering an interactive wait. Only valid inside a task."},
				{Name: "--name", Value: "TEXT", Default: "the condition itself", Text: "What is being waited for; it appears in the wake message."},
				{Name: "--every", Value: "DURATION", Default: "30s", Text: "First poll interval of a shell or github wait; minimum 30s. It doubles after every not-yet, up to --max-every."},
				{Name: "--max-every", Value: "DURATION", Default: "10m", Text: "Ceiling of the doubling poll interval."},
				{Name: "--timeout", Value: "DURATION", Default: "24h; a time wait's default covers its instant", Text: "Give up after this long. For --task current it is the wait's maximum duration, enforced by the coordinator."},
				{Name: "--or-timeout", Default: "off", Text: "Treat the deadline as a normal outcome, timed-out with exit 0, rather than a failure."},
				{Name: "--run-timeout", Value: "DURATION", Default: "1m", Text: "Bound one run of a shell check."},
				{Name: "--dir", Value: "PATH", Default: "the current directory", Text: "Working directory of a shell check."},
				{Name: "--at", Value: "RFC3339", Default: "none", Text: "Time condition: the instant to wait for. An instant that has passed is refused."},
				{Name: "--for", Value: "DURATION", Default: "none", Text: "Time condition, relative to now."},
				{Name: "--github", Value: "run <id>|pr <n>", Default: "none", Text: "GitHub condition. The target is read once at registration with gh and fixed arguments; unreadable, already met or already failed is refused."},
				{Name: "--repo", Value: "owner/name", Default: "the repository of the current directory, resolved and recorded at registration", Text: "The repository of a github target."},
				{Name: "--node", Value: "<run>[/<task>]", Default: "none", Text: "Coordinator condition on a workflow node. A run alone names its sink."},
				{Name: "--quota", Value: "POOL", Default: "none", Text: "Coordinator condition on a quota pool, settled from its merged bucket observations."},
				{Name: "--below", Value: "N", Default: "none", Text: "Quota condition: met when the pool's usage is under N percent."},
				{Name: "--phase", Value: "normal", Default: "none", Text: "Quota condition: met when every bucket of the pool is in that phase."},
				{Name: "--reset", Default: "off", Text: "Quota condition: met when the window current at registration resets."},
				{Name: "--state", Value: "STATE", Default: "pr: merged; node: terminal", Text: "The state waited for. github pr: merged, reviewed or checks-passed. node: terminal, succeeded, paused, waiting-external or active."},
				{Name: "--thread", Value: "ID", Default: "the canonical thread of the task this process is executing, else the provider session resolved from the environment", Text: "The T3 thread to wake. A provider session id is an input to that resolution and never a thread id."},
				{Name: "--group", Value: "NAME", Default: "no group", Text: "Group with other interactive waits of the same thread."},
				{Name: "--wake", Value: "each|all", Default: "each", Text: "Wake on the first settlement, or once every wait of the group has settled. A group is all local kinds or all coordinator kinds; mixing is refused."},
				{Name: "--request-id", Value: "ID", Default: "for --task current, park-<attempt>-<revision>; otherwise generated", Text: "Stable registration id, for retrying one registration safely. Repeating it while that wait is live returns the same wait; repeating it after the wait settled is refused."},
				jsonFlag("the registered wait, with firstExit and firstOutputLine from the registration probe"),
			},
			Exits: []helpExit{
				{0, "registered"},
				{1, "refused or failed: a condition that already holds, an instant that has passed, an unreadable target, a terminal attempt, or a bad option"},
				{3, "node and quota kinds only: client configuration"},
				{4, "node and quota kinds only: authentication"},
				{5, "node and quota kinds only: no coordinator answered"},
				{6, "node and quota kinds only: timeout"},
				{7, "node and quota kinds only: protocol"},
				{8, "node and quota kinds only: the coordinator refused the registration"},
			},
			JSONKeys: []string{"id", "threadId", "kind", "status", "name", "condition", "firstExit", "firstOutputLine"},
			JSONNote: "A coordinator-held wait prints the coordinator's own registration document instead, with waits and taskWaits.",
			Notes: "The arguments after a bare -- are the shell check's own command and are never read as options, so a check that itself takes --help still registers. " +
				"The kinds, the outcomes and the shape of the wake message are on the family page: t3-steward wait --help.",
			Parsers: []parserSite{
				{Func: "parseLocalWaitSpec"}, {Func: "parseCoordinatorWaitSpec"},
				{Func: "currentTaskWaitArgs"}, {Func: "coordinatorWaitArgs"},
				{Func: "normalizeKindArgs"}, {Func: "cmdNodeWait", Case: "add"},
			},
			// --run and --task-run are the superseded spellings of --node. The
			// parser still takes them for the callers that already use them; help
			// does not advertise them, because a superseded path that appears in
			// help is advertised rather than deprecated.
			Undocumented: []string{"--run"},
		},
		{
			Path:    "wait list",
			Purpose: "what this thread, or every thread, is waiting for, from both sources.",
			Usage: []string{
				"t3-steward wait list [--thread ID] [--host HOST] [--all] [--json]",
				"t3-steward wait list --native [--thread ID] [--host HOST] [--json]",
			},
			Flags: []helpFlag{
				{Name: "--thread", Value: "ID", Default: "the thread this process resolves to", Text: "List the waits of one thread, from both sources."},
				{Name: "--host", Value: "HOST", Default: "every host", Text: "List only the waits whose wake would be delivered from one host. A wait recorded for another host is not one this host will deliver."},
				{Name: "--all", Default: "off", Text: "Every thread, and every state: settled and delivered waits are hidden by default."},
				{Name: "--native", Default: "off", Text: "The coordinator's raw inventory instead of the joined answer: node, quota and every task-bound wait it holds, in every state."},
				jsonFlag("the list"),
			},
			Exits:    []helpExit{{0, "listed, from every source"}, {1, "the state database could not be read, a bad option, or a source failed with no transport class"}, {3, "client configuration"}, {5, "no coordinator answered"}, {6, "timeout"}, {8, "the coordinator refused the query"}},
			JSONKeys: []string{"waits", "sources", "unavailable", "hidden", "id", "kind", "threadId", "subject", "state", "delivery", "host", "registeredAt", "deadline", "source", "settled"},
			JSONNote: "One document with waits, the sources that answered, and unavailable naming any source that did not. Read unavailable before reading an empty waits: they are different zeros.",
			Notes:    "It joins this host's local checks with the waits the coordinator holds, so a node wait registered seconds ago appears. A source that could not be read is named in the output and exits non-zero rather than shortening the list silently, keeping the transport class of the first classified failure so the exit code says which transport failed.",
			Parsers:  []parserSite{{Func: "parseWaitListArgs"}, {Func: "cmdNodeWait", Case: "list"}},
		},
		{
			Path:    "wait cancel",
			Purpose: "cancel one wait: a local check, a task-bound wait, or a coordinator-held wait.",
			Usage: []string{
				"t3-steward wait cancel <id>",
				"t3-steward wait cancel <w-tw-id>|<tw-id>",
				"t3-steward wait cancel <nw-id> [--json]",
			},
			Flags: []helpFlag{
				{Name: "--native", Default: "inferred from the id", Text: "Address the coordinator's admin socket rather than this host's local checks."},
				jsonFlag("the cancellation"),
			},
			Exits:    []helpExit{{0, "cancelled"}, {1, "no id, an unknown id, or the state database could not be read"}, {3, "coordinator-held waits only: client configuration"}, {5, "coordinator-held waits only: no coordinator answered"}, {6, "coordinator-held waits only: timeout"}, {8, "coordinator-held waits only: the coordinator refused it"}},
			JSONKeys: []string{"waits", "taskWaits", "changed"},
			Notes:    "Exactly one wait id. Cancelling a task-bound wait settles it at the coordinator first and only then marks the local check, so the attempt resumes with the cancellation as its outcome; if the coordinator cannot be reached nothing is changed and the command fails with the transport exit code.",
			Parsers:  []parserSite{{Func: "cmdNodeWait", Case: "cancel"}},
		},
		{
			Path:    "wait run-now",
			Purpose: "run one wait's check immediately instead of at its next poll.",
			Usage: []string{
				"t3-steward wait run-now <id>",
				"t3-steward wait run-now <nw-id> [--json]",
			},
			Flags: []helpFlag{
				{Name: "--native", Default: "inferred from the id", Text: "Address the coordinator's admin socket rather than this host's local checks."},
				jsonFlag("the result"),
			},
			Exits:    []helpExit{{0, "the check was run"}, {1, "no id, an unknown id, or the state database could not be read"}, {3, "coordinator-held waits only: client configuration"}, {5, "coordinator-held waits only: no coordinator answered"}, {6, "coordinator-held waits only: timeout"}, {8, "coordinator-held waits only: the coordinator refused it"}},
			JSONKeys: []string{"waits", "taskWaits", "changed"},
			Notes:    "Exactly one wait id. It does not change the outcome, only when the check is next evaluated.",
			Parsers:  []parserSite{{Func: "cmdNodeWait", Case: "run-now"}},
		},
		{Path: "task run", Body: taskRunUsage, Parsers: []parserSite{{Func: "parseTaskRunArgs"}}},
		{Path: "task result", Body: taskResultUsage, Parsers: []parserSite{{Func: "parseTaskResultArgs"}}},
		{
			Path:    "task env",
			Purpose: "print the identity of the task this shell is running inside.",
			Usage:   []string{"t3-steward task env", "t3-steward task env --get NAME"},
			Flags: []helpFlag{
				{Name: "--get", Value: "NAME", Default: "print all six as export lines", Text: "Print one value: revision, attempt, task, run, assignment, thread, or a full T3_STEWARD_* variable name. --get=NAME is accepted too."},
			},
			Exits:    []helpExit{{0, "printed"}, {1, "this shell is not inside a t3-steward task, or the name is not an identity name"}},
			JSONNote: "This verb prints no JSON document; it prints shell export lines, or one bare value with --get.",
			Notes:    "It reads the injected environment, or .t3-steward/task.env in the prepared workspace found from the current directory upwards. It loads no configuration and reaches no coordinator. eval \"$(t3-steward task env)\" exports the six variables.",
			Parsers:  []parserSite{{Func: "runTaskEnv"}},
		},
		{
			Path:    "thread stop",
			Purpose: "interrupt one T3 thread's running turn on this host, and optionally end its session.",
			Usage:   []string{"t3-steward thread stop <thread-id> [--session]"},
			Flags: []helpFlag{
				{Name: "--session", Default: "off", Text: "Also dispatch thread.session.stop, so the provider process ends. Without it a thread that is not running is reported and left alone."},
			},
			Exits:    []helpExit{{0, "dispatched, or the thread had no running turn"}, {1, "no thread id, more than one, an unknown flag, or the thread is not on this host's T3 server"}},
			JSONNote: "This verb prints no JSON document; it prints the thread, its turn and each dispatch it sent.",
			Notes:    "Local: it uses the same T3 control client the watchdog uses and reaches no coordinator. policy.dry_run applies, and under it the commands are logged rather than sent.",
			Parsers:  []parserSite{{Func: "parseThreadStop"}},
		},
		{
			Path:     "bucket list",
			Purpose:  "every quota bucket in this host's state database, with its phase and timings.",
			Usage:    []string{"t3-steward bucket list [--json]"},
			Flags:    []helpFlag{jsonFlag("the bucket rows")},
			Exits:    []helpExit{{0, "listed"}, {1, "no state database on this host, or a positional argument was given"}},
			JSONKeys: []string{"buckets"},
			JSONNote: "Each bucket carries key, phase, usedPercent, healthy, observedAt, resetsAt, recoveredAt, stoppedAt, probedAt, appliedThresholds and lastRearm.",
			Notes:    "Local and read-only. It takes no positional argument.",
			Parsers:  []parserSite{{Func: "cmdBucketList"}},
		},
		{
			Path:    "bucket rearm",
			Purpose: "set one bucket's phase back to normal and record the operator's reason.",
			Usage:   []string{"t3-steward bucket rearm <key> --reason TEXT [--force] [--json]"},
			Flags: []helpFlag{
				{Name: "--reason", Value: "TEXT", Required: true, Text: "Why the bucket is rearmed. It is recorded as an action with the actor, and shows in status and in bucket list."},
				{Name: "--force", Default: "off", Text: "Rearm even when the stored usage is at or above stop_percent. Without it that is refused, because the next reading would stop the bucket again."},
				jsonFlag("the before and after states and the recorded action"),
			},
			Exits:    []helpExit{{0, "rearmed"}, {1, "no key, no --reason, no state database, or usage at or above stop_percent without --force"}},
			JSONKeys: []string{"before", "after", "action", "resumeEligible", "resumeBlockedBy"},
			Notes:    "Local and mutating. The key is what bucket list and status print, for example claudeAgent/claude/five_hour. It invents no reading: the stored percentage stays and the next reading decides again.",
			Parsers:  []parserSite{{Func: "cmdBucketRearm"}},
		},
		{
			Path:    "archive candidates",
			Purpose: "the finished threads that would be archived now, and why the others are kept.",
			Usage:   []string{"t3-steward archive candidates"},
			Flags: []helpFlag{
				{Name: "--dry-run", Default: "off", Text: "Read by archive run only; candidates changes nothing in either case and ignores it."},
			},
			Exits:    []helpExit{{0, "listed"}, {1, "archive.destination is not configured, or the T3 server could not be reached"}},
			JSONNote: "This verb prints no JSON document; it prints the candidates and up to fifteen of the threads it kept.",
			Notes:    "Read-only. It reads this host's state database and T3 server and reaches no coordinator.",
			Parsers:  []parserSite{{Func: "cmdArchive", Case: "candidates"}},
		},
		{
			Path:    "archive run",
			Purpose: "archive the candidate threads now.",
			Usage:   []string{"t3-steward archive run [--dry-run]"},
			Flags: []helpFlag{
				{Name: "--dry-run", Default: "off", Text: "Report what would be archived without writing a bundle, deleting from T3 or recording a run."},
			},
			Exits:    []helpExit{{0, "archived, or nothing was eligible"}, {1, "archive.destination is not configured, or the archiving failed"}},
			JSONNote: "This verb prints no JSON document; it prints how many threads it archived.",
			Notes:    "Mutating on this host and on its T3 server: it writes a bundle to archive.destination and, with archive.delete_from_t3, removes the thread. It reaches no coordinator.",
			Parsers:  []parserSite{{Func: "cmdArchive", Case: "run"}},
		},
		{
			Path:     "archive list",
			Purpose:  "the threads this host has archived, with their size and destination state.",
			Usage:    []string{"t3-steward archive list"},
			Exits:    localExits(),
			JSONNote: "This verb prints no JSON document; it prints one line per archived thread.",
			Notes:    "Read-only, and the one archive verb that works without archive.destination configured: it reads the records in this host's state database.",
			Parsers:  []parserSite{{Func: "cmdArchive", Case: "list"}},
		},
		{
			Path:     "archive restore",
			Purpose:  "fetch one archived thread's bundle back and unpack it.",
			Usage:    []string{"t3-steward archive restore <id> [DIR]"},
			Exits:    []helpExit{{0, "restored"}, {1, "no thread id, no archive record for it on this host, or the copy or unpack failed"}},
			JSONNote: "This verb prints no JSON document; it prints where it unpacked the bundle.",
			Notes:    "It writes into DIR, or into ./<id> when DIR is omitted: thread.json, provider-logs/ and transcripts/. It runs scp or cp and tar, and reaches no coordinator.",
			Parsers:  []parserSite{{Func: "cmdArchive", Case: "restore"}},
		},
		{
			Path:     "coordinator identity",
			Purpose:  "which coordinator this host administers, and how it reaches it.",
			Usage:    []string{"t3-steward coordinator identity [--json]"},
			Flags:    []helpFlag{jsonFlag("the identity")},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"version", "kind", "coordinatorId", "owner", "release", "configurationDigest", "epoch", "health", "lastReloadReceipt", "transport"},
			JSONNote: jsonErrorNote,
			Notes:    "Read-only: it runs one status query and changes nothing. It answers, before anything is submitted, where the next submission would go. It also carries the coordinator's last reload receipt, which is how a host that is not the coordinator reads that verdict.",
			Parsers:  []parserSite{{Func: "cmdCoordinator"}},
		},
		{Path: "coordinator reload", Body: coordinatorReloadUsage, Parsers: []parserSite{{Func: "cmdCoordinatorReload"}}},
		{Path: "worker enroll", Body: workerEnrollUsage, Parsers: []parserSite{{Func: "runWorkerEnroll"}}},
		backlogReadPage("worker list", "every worker the coordinator knows; an alias of backlog workers.",
			"t3-steward worker list [--json]",
			nil, []string{"workers"}, backlogReadSites("workers"),
			"Read-only. The arguments are forwarded to \"t3-steward backlog workers\" untouched and parsed there."),
		{
			Path:     "worker serve",
			Purpose:  "run the worker: enrol with the coordinator and execute dispatched attempts.",
			Usage:    []string{"t3-steward worker serve"},
			Exits:    []helpExit{{0, "the process was asked to stop and shut down cleanly"}, {1, "no worker bootstrap under $HOME, an unresolvable credential, or a runtime failure"}},
			JSONNote: "This verb prints no JSON document; it logs to standard error.",
			Notes:    "This is the worker service's ExecStart, and the packaged unit passes --config: the verb reads the configuration file for the T3 connection, the policy and the state database it takes local quota pauses from. It takes no other argument beyond the verb, and it reads the worker bootstrap under $HOME. Enrolment is accepted on the coordinator host with \"t3-steward worker enroll\".",
			Parsers:  []parserSite{{Func: "cmdWorker", Case: "serve"}},
		},
		{
			Path:     "worker bridge",
			Purpose:  "relay standard input and output to the running worker's socket.",
			Usage:    []string{"t3-steward worker bridge"},
			Exits:    []helpExit{{0, "the relay ended cleanly"}, {1, "no worker bootstrap, or no running worker to relay to"}},
			JSONNote: "This verb speaks the framed worker protocol; it prints no document for a reader.",
			Notes:    "The coordinator's SSH exchange uses it. It takes no argument beyond the verb, it reads no configuration file, and unlike the other daemon verbs it does not resolve the bootstrap credential.",
			Parsers:  []parserSite{{Func: "cmdWorker", Case: "bridge"}},
		},
		{
			Path:     "worker inspect-bootstrap",
			Purpose:  "print this worker's identity and the digest of its bootstrap.",
			Usage:    []string{"t3-steward worker inspect-bootstrap"},
			Exits:    []helpExit{{0, "printed"}, {1, "no worker bootstrap under $HOME, or its credential does not resolve"}},
			JSONKeys: []string{"workerId", "coordinatorId", "digest", "credentialRef"},
			JSONNote: "This verb always prints JSON; it has no text rendering and no --json flag.",
			Notes:    "Read-only and local. It takes no argument beyond the verb and reads no configuration file.",
			Parsers:  []parserSite{{Func: "cmdWorker", Case: "inspect-bootstrap"}},
		},
		{
			Path:     "worker inspect-journal",
			Purpose:  "whether restarting this worker would interrupt a dispatched execution.",
			Usage:    []string{"t3-steward worker inspect-journal"},
			Exits:    []helpExit{{0, "printed"}, {1, "no worker bootstrap under $HOME, or its credential does not resolve"}},
			JSONKeys: []string{"safeToRestart", "attempts"},
			JSONNote: "This verb always prints JSON; it has no text rendering and no --json flag.",
			Notes:    "Read-only and local: it summarises the attempt journal. It takes no argument beyond the verb and reads no configuration file.",
			Parsers:  []parserSite{{Func: "cmdWorker", Case: "inspect-journal"}},
		},
		{
			Path:    "worker inspect-directory",
			Purpose: "read-only check of one directory resource registration on this host.",
			Usage:   []string{"t3-steward worker inspect-directory --registration FILE [--expected FILE]"},
			Flags: []helpFlag{
				{Name: "--registration", Value: "FILE", Required: true, Text: "The directory-resource registration to read."},
				{Name: "--expected", Value: "FILE", Default: "no comparison", Text: "Compare what was read against this expected document."},
			},
			Exits:    []helpExit{{0, "the registration was read, and matched --expected when given"}, {1, "the file is missing or unreadable, or it does not match --expected"}},
			JSONNote: "This verb prints the registration it read as JSON.",
			Notes:    "Operator diagnostic. Read-only and local.",
			Parsers:  []parserSite{{Func: "cmdInspectDirectory"}},
		},
		{
			Path:    "worker contained-exec",
			Purpose: "run one containment launch specification in the foreground.",
			Usage:   []string{"t3-steward worker contained-exec --spec FILE"},
			Flags: []helpFlag{
				{Name: "--spec", Value: "FILE", Required: true, Text: "The containment launch specification to run."},
			},
			Exits:    []helpExit{{0, "the contained process exited zero"}, {1, "the specification is missing or invalid, or the process failed"}},
			JSONNote: "This verb prints no JSON document; the contained process's own output passes through.",
			Notes:    "Provider containment, for an operator reproducing one launch by hand. Local.",
			Parsers:  []parserSite{{Func: "cmdContainedExec"}},
		},
		{
			Path:     "worker contained-t3",
			Purpose:  "run the contained T3 server; the containment launcher invokes it.",
			Usage:    []string{"t3-steward worker contained-t3 [--port N] [--node PATH] [--entry PATH] [--opencode-binary PATH] [--opencode-model MODEL]"},
			Flags:    containedT3Flags(),
			Exits:    []helpExit{{0, "the server exited cleanly"}, {1, "it could not start, or it failed while running"}},
			JSONNote: "This verb prints no JSON document; it logs to standard error.",
			Notes:    "Internal: the containment launcher invokes it. An operator does not.",
			Parsers:  []parserSite{{Func: "cmdContainedT3"}},
		},
		{
			Path:    "worker contained-child",
			Purpose: "run the contained child process; the containment launcher invokes it.",
			Usage:   []string{"t3-steward worker contained-child [--control-port N] [--egress] -- COMMAND..."},
			Flags: []helpFlag{
				{Name: "--control-port", Value: "N", Default: "none", Text: "The control port the child reports to."},
				{Name: "--egress", Default: "off", Text: "Enable the constrained provider gateway for the child."},
			},
			Exits:    []helpExit{{0, "the child exited zero"}, {1, "it could not start, or it exited non-zero"}},
			JSONNote: "This verb prints no JSON document.",
			Notes:    "Internal: the containment launcher invokes it. An operator does not.",
			Parsers:  []parserSite{{Func: "cmdContainedChild"}},
		},
		{
			Path:     "schedules put",
			Purpose:  "create or replace one schedule definition.",
			Usage:    []string{"t3-steward schedules put <schedule> --name TEXT --workflow ID --cron \"EXPR\" --timezone IANA --reason TEXT [--after-failure next-cycle|hold] [--disabled] [--expected-revision N] [--request-id ID] [--json]"},
			Flags:    schedulePutFlags(),
			Exits:    coordinatorExits(),
			JSONKeys: []string{"requestId", "id", "name", "workflowId", "expression", "timezone", "afterFailure", "enabled"},
			JSONNote: jsonErrorNote,
			Notes:    "Mutating. Exactly one schedule id, before the flags.",
			Parsers:  []parserSite{{Func: "parseScheduleDefinition"}},
		},
		scheduleReadPage("schedules list", "every schedule the coordinator holds.", "t3-steward schedules list [--json]", "It takes no positional argument."),
		scheduleReadPage("schedules show", "one schedule definition, without its trigger history.", "t3-steward schedules show <schedule> [--json]", "Exactly one schedule id."),
		scheduleReadPage("schedules history", "one schedule's trigger history.", "t3-steward schedules history <schedule> [--json]", "Exactly one schedule id."),
	}

	for _, supervisor := range []struct{ verb, purpose string }{
		{"start", "supervise one contained execution across worker restarts: start it."},
		{"show", "supervise one contained execution across worker restarts: report it."},
		{"stop", "supervise one contained execution across worker restarts: stop it."},
	} {
		pages = append(pages, helpPage{
			Path:    "worker contained-" + supervisor.verb,
			Purpose: supervisor.purpose,
			Usage:   []string{"t3-steward worker contained-" + supervisor.verb + " --spec FILE --state-dir DIR --execution ID"},
			Flags: []helpFlag{
				{Name: "--spec", Value: "FILE", Required: true, Text: "The containment launch specification."},
				{Name: "--state-dir", Value: "DIR", Required: true, Text: "Where the supervisor keeps the execution's state across worker restarts."},
				{Name: "--execution", Value: "ID", Required: true, Text: "The execution this command addresses."},
			},
			Exits:    []helpExit{{0, "done"}, {1, "a missing or invalid flag, an unreadable specification, or a supervision failure"}},
			JSONNote: "This verb prints the execution's state as JSON.",
			Notes:    "Provider containment, local to this host. An operator reaches for it when an execution outlived its worker.",
			Parsers:  []parserSite{{Func: "cmdContainedSupervisor"}},
		})
	}

	for _, control := range []struct{ verb, purpose, usage string }{
		{"run", "trigger one schedule now, out of cycle.", "t3-steward schedules run <schedule> --reason TEXT [--command-id ID] [--json]"},
		{"enable", "enable one schedule.", "t3-steward schedules enable <schedule> --reason TEXT [--command-id ID] [--json]"},
		{"disable", "disable one schedule.", "t3-steward schedules disable <schedule> --reason TEXT [--command-id ID] [--json]"},
		{"delay-next", "hold one schedule's next trigger until a stated instant.", "t3-steward schedules delay-next <schedule> --until RFC3339 --reason TEXT [--command-id ID] [--json]"},
	} {
		pages = append(pages, helpPage{
			Path:     "schedules " + control.verb,
			Purpose:  control.purpose,
			Usage:    []string{control.usage},
			Flags:    mutationFlags(),
			Exits:    coordinatorExits(),
			JSONKeys: []string{"version", "command", "event", "currentTarget"},
			JSONNote: jsonErrorNote,
			Notes:    "Mutating and revision-fenced. One parser serves all four controls, so all four accept the whole flag set above; --until belongs to delay-next, and --now is a backlog pause option the schedule controls refuse.",
			Parsers:  []parserSite{{Func: "parseScheduleMutation"}, {Func: "parseMutationOptions"}, {Func: "takeJSONFlag"}},
		})
	}

	return pages
}

// containedT3Flags are the options of the contained T3 server. They are here
// rather than inline so the page and the launcher read the same list.
func containedT3Flags() []helpFlag {
	return []helpFlag{
		{Name: "--port", Value: "N", Default: "chosen by the launcher", Text: "Port the contained T3 server listens on."},
		{Name: "--node", Value: "PATH", Default: "the node on PATH", Text: "Node binary to run the server with."},
		{Name: "--entry", Value: "PATH", Default: "the packaged entry point", Text: "Server entry point."},
		{Name: "--opencode-binary", Value: "PATH", Default: "none", Text: "OpenCode binary to expose to the contained session."},
		{Name: "--opencode-model", Value: "MODEL", Default: "none", Text: "OpenCode model to expose to the contained session."},
	}
}

// schedulePutFlags are the options of the schedule definition verb.
func schedulePutFlags() []helpFlag {
	return []helpFlag{
		{Name: "--name", Value: "TEXT", Required: true, Text: "Human name of the schedule."},
		{Name: "--workflow", Value: "ID", Required: true, Text: "The workflow the schedule triggers."},
		{Name: "--cron", Value: "EXPR", Required: true, Text: "The cron expression, in --timezone."},
		{Name: "--timezone", Value: "IANA", Required: true, Text: "IANA time zone the expression is read in, for example Europe/Amsterdam."},
		{Name: "--reason", Value: "TEXT", Required: true, Text: "Why the definition is being written; recorded with it."},
		{Name: "--after-failure", Value: "next-cycle|hold", Default: "next-cycle", Text: "What the schedule does after a failed run: carry on at the next cycle, or hold until an operator enables it."},
		{Name: "--disabled", Default: "off", Text: "Write the definition disabled, so nothing triggers until it is enabled."},
		{Name: "--expected-revision", Value: "N", Default: "no fence", Text: "Fence the write against a named definition revision."},
		{Name: "--request-id", Value: "ID", Default: "one generated per invocation", Text: "Stable idempotency id; the same id with the same content replays."},
		jsonFlag("the written definition"),
	}
}

// scheduleReadPage is one of the three schedule reads. They share a parser:
// the schedules dispatcher takes --json and nothing else.
func scheduleReadPage(path, purpose, usage, notes string) helpPage {
	return helpPage{
		Path:     path,
		Purpose:  purpose,
		Usage:    []string{usage},
		Flags:    []helpFlag{jsonFlag("the answer")},
		Exits:    coordinatorExits(),
		JSONKeys: adminEnvelopeKeys("schedules"),
		JSONNote: jsonErrorNote,
		Notes:    "Read-only. " + notes,
		Parsers:  []parserSite{{Func: "takeJSONFlag"}},
	}
}
