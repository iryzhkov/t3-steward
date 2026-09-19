package main

import (
	"fmt"
	"io"
)

// helpPages is the registry every help request is answered from. It is
// assembled once, from the page lists in this file and in
// help_pages_verbs.go, and keyed by the verb path.
var helpPages = map[string]helpPage{}

func init() {
	for _, list := range [][]helpPage{
		topLevelHelpPages(),
		familyHelpPages(),
		backlogHelpPages(),
		campaignHelpPages(),
		fleetHelpPages(),
	} {
		for _, page := range list {
			if _, duplicate := helpPages[page.Path]; duplicate {
				panic("duplicate help page for " + page.Path)
			}
			helpPages[page.Path] = page
		}
	}
}

// printTopLevelHelp answers "t3-steward help [verb...]": the overview, or the
// page of the verb the words name.
func printTopLevelHelp(out io.Writer, words []string) error {
	if answered, err := admitHelp(out, nil, append([]string{"help"}, words...)); answered || err != nil {
		return err
	}
	page := helpPages[""]
	_, err := fmt.Fprint(out, page.render())
	return err
}

// coordinatorExits is the exit-code table of every verb that reaches the
// coordinator. It is the table the transport help spells out, per verb, so
// that one help page is enough to branch on the result.
func coordinatorExits() []helpExit {
	return []helpExit{
		{0, "the coordinator answered"},
		{1, "anything else, including an argument this client refused before sending"},
		{3, "client configuration: this host cannot form a request"},
		{4, "authentication: the coordinator refused the principal or the signature"},
		{5, "unavailable: no coordinator answered"},
		{6, "timeout: the request deadline expired with no answer"},
		{7, "protocol: version or limit mismatch, or a malformed frame"},
		{8, "rejected: the coordinator answered and refused the request"},
	}
}

// localExits is the exit-code table of a verb that touches only this host.
func localExits() []helpExit {
	return []helpExit{
		{0, "done"},
		{1, "refused or failed; the reason is on standard error"},
	}
}

// jsonErrorNote is what every coordinator verb prints on stdout when it fails
// under --json.
const jsonErrorNote = `A failure under --json also prints {"version":"backlog.admin/v1","kind":"error","class":"...","operation":"...","message":"..."} on standard output.`

// adminEnvelopeKeys are the keys every coordinator read answer carries around
// its payload.
func adminEnvelopeKeys(payload ...string) []string {
	return append([]string{"version", "kind", "generatedAt"}, payload...)
}

// jsonFlag is the flag almost every verb carries. Its default is off, and off
// means the human rendering.
func jsonFlag(document string) helpFlag {
	return helpFlag{Name: "--json", Default: "off", Text: "Print " + document + " as one JSON document instead of the text rendering."}
}

// globalHelpFlags are the four options every verb the dispatcher parses itself
// accepts, declared once in registerGlobalFlags.
func globalHelpFlags(stateful bool) []helpFlag {
	dryRun := "Force dry-run mode regardless of the configuration."
	noDryRun := "Disable dry-run mode for this run, overriding the configuration."
	if !stateful {
		dryRun += " Accepted by every verb the dispatcher parses; it changes nothing here."
		noDryRun += " Accepted by every verb the dispatcher parses; it changes nothing here."
	}
	return []helpFlag{
		{Name: "--config", Value: "PATH", Default: "$XDG_CONFIG_HOME/t3-steward/config.yaml", Text: "Configuration file to read."},
		{Name: "--dry-run", Default: "off", Text: dryRun},
		{Name: "--no-dry-run", Default: "off", Text: noDryRun},
		{Name: "--log-level", Value: "LEVEL", Default: "the configuration's log_level", Text: "debug, info, warn or error."},
	}
}

// globalFlagSite is the one place the four shared options are declared, and so
// the one place the contract test reads them from.
var globalFlagSite = parserSite{Func: "registerGlobalFlags"}

// familyDispatchSite is the clause of the dispatcher's own switch that a
// command family is entered through. That clause takes --config out of the
// argument list before the family sees it, so it is a parser site of every
// verb of the family, and the contract test derives it from the verb's path
// rather than reading it off the page. A page therefore cannot leave it out,
// which is the hole nineteen "this verb takes no flags" pages went through.
func familyDispatchSite(family string) parserSite {
	return parserSite{Func: "dispatch", Case: family}
}

// familyConfigFlag is the option every verb of a command family accepts,
// whether or not the verb parses anything of its own. The renderer puts it on
// every second-level page.
//
// The value is a separate word because the dispatcher compares the whole
// argument: "--config=PATH" is not recognised there and travels on to the
// verb, which ignores it (backlog path) or refuses it (worker serve). Saying
// so is the difference between a page that documents the flag and a page a
// caller can act on.
func familyConfigFlag(family string) helpFlag {
	return helpFlag{
		Name:    "--config",
		Value:   "PATH",
		Default: "$XDG_CONFIG_HOME/t3-steward/config.yaml",
		Text: "Configuration file to read. The dispatcher removes it from the arguments before the " + family +
			" family is entered, so every verb of the family accepts it; a verb that reads no configuration accepts it and ignores it. " +
			"The path is a separate word: --config=PATH is not recognised here and is passed on to the verb.",
	}
}

// dispatchSite scopes a flat verb's own options to its case clause in the
// dispatcher's flag switch.
func dispatchSite(verb string) []parserSite {
	return []parserSite{globalFlagSite, {Func: "dispatch", Case: verb}}
}

// topLevelHelpPages are the overview and the verbs the dispatcher parses
// itself: they never reach a command family.
func topLevelHelpPages() []helpPage {
	return []helpPage{
		{Path: "", Body: usage},
		{
			Path:    "init",
			Purpose: "write a commented configuration file and create the state directory.",
			Usage:   []string{"t3-steward init [--force] [--t3-url URL] [--t3-data-dir DIR]"},
			Flags: append([]helpFlag{
				{Name: "--force", Default: "off", Text: "Overwrite an existing configuration file. Without it an existing file is kept and reported."},
				{Name: "--t3-url", Value: "URL", Default: "discovered from the T3 data directory", Text: "T3 server URL to write into the new configuration."},
				{Name: "--t3-data-dir", Value: "DIR", Default: "the T3 default data directory", Text: "T3 data directory to write into the new configuration."},
			}, globalHelpFlags(false)...),
			Exits:    localExits(),
			JSONNote: "This verb prints no JSON document; it prints the paths it wrote and what it discovered.",
			Notes:    "It writes the configuration file and the state directory and nothing else. The discovery report that follows reads the T3 data directory and looks for the t3 binary; neither is required for the file to be written.",
			Parsers:  dispatchSite("init"),
		},
		{
			Path:     "check",
			Purpose:  "verify the T3 connection, token, version and provider logs.",
			Usage:    []string{"t3-steward check [--config PATH]"},
			Flags:    globalHelpFlags(false),
			Exits:    []helpExit{{0, "every check passed"}, {1, "at least one check failed; each line is marked ok, warn or FAIL"}},
			JSONNote: "This verb prints no JSON document; it prints one line per check.",
			Notes:    "It reads the configuration, the T3 data directory, the provider log directory and this host's state database, and it queries the T3 server. It reaches no coordinator.",
			Parsers:  []parserSite{globalFlagSite},
		},
		{
			Path:     "run",
			Purpose:  "run the watchdog in the foreground; this is what the packaged service starts.",
			Usage:    []string{"t3-steward run [--config PATH] [--dry-run|--no-dry-run] [--log-level LEVEL]"},
			Flags:    globalHelpFlags(true),
			Exits:    []helpExit{{0, "the process was asked to stop and shut down cleanly"}, {1, "it could not start, or it failed while running"}},
			JSONNote: "This verb prints no JSON document; it logs to standard error.",
			Notes:    "With backlog_v2 configured this is also the coordinator and the worker runtime. It runs until SIGINT or SIGTERM.",
			Parsers:  []parserSite{globalFlagSite},
		},
		{
			Path:    "status",
			Purpose: "show this host's bucket states, resume intents and recent actions.",
			Usage:   []string{"t3-steward status [--limit N] [--all] [--json]"},
			Flags: append([]helpFlag{
				{Name: "--limit", Value: "N", Default: "20", Text: "Number of recent actions to show."},
				{Name: "--all", Default: "off", Text: "Include resumed and cancelled resume intents, not only the live ones."},
				jsonFlag("the three sections"),
			}, globalHelpFlags(false)...),
			Exits:    []helpExit{{0, "printed"}, {1, "no state database on this host, or it could not be read"}},
			JSONKeys: []string{"buckets", "resumeIntents", "recentActions"},
			Notes:    "Local only: it reads this host's state database and reaches no coordinator. For the fleet's view use \"t3-steward backlog status\".",
			Parsers:  dispatchSite("status"),
		},
		{
			Path:    "replay",
			Purpose: "feed recorded quota events through the policy engine, with no T3 server.",
			Usage:   []string{"t3-steward replay <file> [--speed FACTOR] [--with-state] [--resume]"},
			Flags: append([]helpFlag{
				{Name: "--speed", Value: "FACTOR", Default: "0", Text: "Sleep between events scaled by this factor. 0 replays with no sleeping."},
				{Name: "--with-state", Default: "off", Text: "Use the real state database instead of a temporary in-memory one. It writes to it."},
				{Name: "--resume", Default: "off", Text: "Enable automatic resume during the replay."},
			}, globalHelpFlags(false)...),
			Exits:    []helpExit{{0, "replayed"}, {1, "the file is missing, unreadable, or holds no provider records"}},
			JSONNote: "This verb prints no JSON document; it prints the decisions and the final bucket states.",
			Notes:    "Dry-run is forced on: the fake T3 it drives dispatches nothing. Exactly one file argument is required.",
			Parsers:  dispatchSite("replay"),
		},
		{
			Path:    "report",
			Purpose: "consumption by peak and off-peak hours, hour of day, model and thread.",
			Usage:   []string{"t3-steward report [--days N] [--bucket TEXT] [--peak SCHEDULE] [--remotes HOSTS] [--local] [--from-logs] [--import] [--json]"},
			Flags: append([]helpFlag{
				{Name: "--days", Value: "N", Default: "14", Text: "Period to report, in days."},
				{Name: "--bucket", Value: "TEXT", Default: "every bucket", Text: "Only buckets whose key contains this text."},
				{Name: "--peak", Value: "SCHEDULE", Default: "report.peak in the configuration", Text: "Peak schedule in local time."},
				{Name: "--remotes", Value: "HOSTS", Default: "report.remotes in the configuration", Text: "Comma-separated SSH hosts whose readings are merged in."},
				{Name: "--local", Default: "off", Text: "Ignore the configured remotes and report this host alone."},
				{Name: "--from-logs", Default: "off", Text: "Also scan the provider logs, rotated files included, rather than only the state database."},
				{Name: "--import", Default: "off", Text: "Store the scanned observations in the state database. This writes."},
				jsonFlag("the report"),
			}, globalHelpFlags(false)...),
			Exits:    localExits(),
			JSONKeys: []string{"days", "buckets", "hours", "models", "threads"},
			Notes:    "It reads this host's state database and, with --remotes, runs \"t3-steward export\" over SSH on each named host. It reaches no coordinator.",
			Parsers:  dispatchSite("report"),
		},
		{
			Path:    "forecast",
			Purpose: "interactive-demand map by weekday and hour, and the current backlog headroom.",
			Usage:   []string{"t3-steward forecast [--days N] [--bucket TEXT] [--remotes HOSTS] [--local] [--from-logs] [--import] [--json]"},
			Flags: append([]helpFlag{
				{Name: "--days", Value: "N", Default: "56", Text: "History to learn from, in days."},
				{Name: "--bucket", Value: "TEXT", Default: "every bucket", Text: "Only buckets whose key contains this text."},
				{Name: "--remotes", Value: "HOSTS", Default: "report.remotes in the configuration", Text: "Comma-separated SSH hosts whose readings are merged in."},
				{Name: "--local", Default: "off", Text: "Ignore the configured remotes."},
				{Name: "--from-logs", Default: "off", Text: "Also scan the provider logs."},
				{Name: "--import", Default: "off", Text: "Store the scanned observations in the state database. This writes."},
				jsonFlag("the forecast"),
			}, globalHelpFlags(false)...),
			Exits:    localExits(),
			JSONKeys: []string{"days", "buckets", "weekdays", "headroom"},
			Notes:    "Local and remote readings only; it reaches no coordinator.",
			Parsers:  dispatchSite("forecast"),
		},
		{
			Path:    "export",
			Purpose: "print this host's readings and token samples as JSON, for another host's report.",
			Usage:   []string{"t3-steward export [--days N] [--from-logs]"},
			Flags: append([]helpFlag{
				{Name: "--days", Value: "N", Default: "14", Text: "Period to export, in days."},
				{Name: "--from-logs", Default: "off", Text: "Also scan the provider logs before exporting."},
			}, globalHelpFlags(false)...),
			Exits:    localExits(),
			JSONKeys: []string{"host", "days", "readings", "samples"},
			JSONNote: "This verb always prints JSON; it has no text rendering and no --json flag.",
			Notes:    "\"t3-steward report --remotes\" runs this verb over SSH on each named host.",
			Parsers:  dispatchSite("export"),
		},
		{
			Path:    "install-service",
			Purpose: "install the per-user background service (Linux systemd).",
			Usage:   []string{"t3-steward install-service [--force] [--enable] [--credential-file REF=PATH]..."},
			Flags: append([]helpFlag{
				{Name: "--force", Default: "off", Text: "Overwrite an existing service definition."},
				{Name: "--enable", Default: "off", Text: "Enable and start the service now instead of printing the commands that would."},
				{Name: "--credential-file", Value: "REF=PATH", Default: "no credential files", Text: "Read the credential REF from PATH at use. Repeatable. The path is made absolute and must name a regular file that is not world-readable, so a unit that could never resolve the credential is refused here."},
			}, globalHelpFlags(true)...),
			Exits:    []helpExit{{0, "written"}, {1, "no configuration yet, an existing unit without --force, or a credential path that is not a private regular file"}},
			JSONNote: "This verb prints no JSON document; it prints the unit path and the commands to enable it.",
			Notes:    "Run \"t3-steward init\" first: the unit names the configuration file and the verb refuses to write a unit pointing at a file that does not exist.",
			Parsers:  dispatchSite("install-service"),
		},
		{
			Path:     "uninstall-service",
			Purpose:  "remove the per-user background service.",
			Usage:    []string{"t3-steward uninstall-service"},
			Flags:    globalHelpFlags(false),
			Exits:    localExits(),
			JSONNote: "This verb prints no JSON document; it prints the path it removed.",
			Parsers:  []parserSite{globalFlagSite},
		},
		{
			Path:     "worker-exchange",
			Purpose:  "the restricted SSH worker endpoint; the coordinator invokes it, an operator does not.",
			Usage:    []string{"t3-steward worker-exchange [control|artifact-receive|artifact-send]"},
			Flags:    globalHelpFlags(false),
			Exits:    []helpExit{{0, "the framed exchange completed"}, {1, "the frame, the signature or the operation was refused"}},
			JSONNote: "This verb speaks the framed worker protocol on standard input and output; it prints no document for a reader.",
			Notes:    "It is the ForceCommand of the worker's authorized key. One fixed operation word pins the key to that operation; with no word the operation comes from the signed envelope. To run or inspect a worker use \"t3-steward worker\".",
			Parsers:  []parserSite{globalFlagSite},
		},
		{
			Path:     "coordinator-exchange",
			Purpose:  "the restricted SSH coordinator-admin endpoint; a remote admin client invokes it.",
			Usage:    []string{"t3-steward coordinator-exchange --config PATH [operation]"},
			Flags:    globalHelpFlags(false),
			Exits:    []helpExit{{0, "the framed exchange completed"}, {1, "the frame, the signature or the operation was refused"}},
			JSONNote: "This verb speaks the framed admin protocol on standard input and output; it prints no document for a reader.",
			Notes:    "It is the ForceCommand of a remote admin client's authorized key. To administer a coordinator use \"t3-steward backlog\" or \"t3-steward coordinator\" with a coordinator client configured.",
			Parsers:  []parserSite{globalFlagSite},
		},
		{
			Path:     "version",
			Purpose:  "print the release, the commit, the build date and the T3 versions it was tested with.",
			Usage:    []string{"t3-steward version", "t3-steward --version", "t3-steward -v"},
			Exits:    []helpExit{{0, "printed"}},
			JSONNote: "This verb prints no JSON document; it prints one line.",
			Notes:    "It reads no configuration and reaches nothing.",
		},
	}
}

// familyHelpPages are the breadth pages: one per command family. A family page
// summarises its verbs; the flags live one level down, which is what the two
// depths are for. Each carries the reference this package already holds.
func familyHelpPages() []helpPage {
	return []helpPage{
		{Path: "backlog", Body: backlogUsage},
		{Path: "schedules", Body: schedulesUsage},
		{Path: "wait", Body: waitUsage},
		{Path: "task", Body: taskUsage},
		{Path: "thread", Body: threadUsage},
		{Path: "bucket", Body: bucketUsage},
		{Path: "archive", Body: archiveUsage},
		{Path: "campaign", Body: campaignUsage},
		{Path: "coordinator", Body: coordinatorUsage},
		{Path: "worker", Body: workerUsage},
		{Path: "models", Body: modelsUsage},
		{
			Path:     "ui-archive",
			Purpose:  "read-only: the T3 UI's archive candidates and their classification, as JSON.",
			Usage:    []string{"t3-steward ui-archive candidates [--config PATH]"},
			Flags:    []helpFlag{{Name: "--config", Value: "PATH", Default: "$XDG_CONFIG_HOME/t3-steward/config.yaml", Text: "Configuration file to read. The dispatcher removes it before this verb parses anything."}},
			Exits:    localExits(),
			JSONKeys: []string{"enabled", "dryRun", "visibleThreads", "archivedInShell", "candidates"},
			JSONNote: "This verb always prints JSON; it has no text rendering and no --json flag.",
			Notes:    "candidates is the only verb. It archives nothing: the daemon performs the bounded effects, and this is the read that explains what it would do.",
		},
	}
}
