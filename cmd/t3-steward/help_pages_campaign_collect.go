package main

// The run-collection pages: how a coordinating thread finds the runs it
// submitted and has not yet acted on, and records that it has.

// collectThreadFlag is --thread on both run-collection verbs.
var collectThreadFlag = helpFlag{Name: "--thread", Value: "current|ID", Default: "current",
	Text: "The T3 thread whose runs these are. current resolves as --notify-thread current does, and is refused rather than widened when it does not resolve."}

// collectionNote is the contract both pages share.
const collectionNote = "Ownership: a run belongs to a thread when a node wait of that thread targets it, the notification that campaign submit, task run and review register for --notify-thread; " +
	"this is the rule of campaign list --thread, and a run submitted with --no-notify belongs to no thread. " +
	"Collected means acted on: a run is collected for a thread only by an explicit campaign collect. " +
	"Nothing is collected implicitly, not by task result, campaign show, --wait or a delivered wake, because a session that read a result and then lost its context has not used it. " +
	"Only a finished run is collected, and the collection covers the run as it finished: the coordinator records the run's terminal progress and completion time, " +
	"and a run that finishes again, for example after campaign rerun, is uncollected again. " +
	"Collections are stored on the coordinator, so they survive session restarts, host state pruning and client upgrades."

func campaignCollectHelpPages() []helpPage {
	return []helpPage{
		{
			Path:     "campaign uncollected",
			Purpose:  "list the finished and attention-needing runs a thread submitted and has not collected.",
			Usage:    []string{"t3-steward campaign uncollected [--thread current|ID] [--json]"},
			Flags:    []helpFlag{collectThreadFlag, jsonFlag("the view")},
			Exits:    coordinatorExits(),
			JSONKeys: []string{"schemaVersion", "kind", "thread", "generatedAt", "collectionSupported", "note", "runs", "omitted"},
			JSONNote: "schemaVersion 1, kind t3-steward.uncollected/v1. runs is always an array; each run has run, name, project, state, finishedAt, attention " +
				"(an array of kind, subject, task, detail and since), wake (waitId, delivery, deliveredAt), result and collect, the commands to run next. " +
				"omitted counts collected, active and unknown runs. " + jsonErrorNote,
			Notes: "Read-only. Lists every run the thread owns that is finished and not collected, or still running and needing attention: an open ask or attention request, " +
				"needs-input with no open wait, or failed tasks whose dependents are blocked. A failed or cancelled run says so. " +
				"Running runs that need nothing, collected runs and runs the coordinator no longer lists are counted in the summary, not shown. " +
				"Running runs come first, then finished runs newest first. WAKE is the delivery of the thread's newest node wait on the run.\n\n" +
				collectionNote + "\n\n" +
				"An older coordinator that does not record collections still answers: every finished run is listed, the first line (and collectionSupported false) says to upgrade it, and the exit is 0.",
			Parsers: []parserSite{{Func: "parseCampaignCollectArgs"}},
		},
		{
			Path:    "campaign collect",
			Purpose: "record that a thread has acted on finished runs, so campaign uncollected stops listing them.",
			Usage:   []string{"t3-steward campaign collect RUN... [--thread current|ID] [--json]"},
			Flags:   []helpFlag{collectThreadFlag, jsonFlag("what was recorded")},
			Exits: []helpExit{
				{0, "every run was recorded"},
				{1, "an argument this client refused before sending, such as a run ID with \"/\", or an unresolved thread"},
				{3, "client configuration: this host cannot form a request"},
				{4, "authentication: the coordinator refused the principal or the signature"},
				{5, "unavailable: no coordinator answered"},
				{6, "timeout: the request deadline expired with no answer"},
				{7, "protocol: the coordinator does not record collected runs (upgrade it), or another version mismatch"},
				{8, "rejected: the coordinator refused a run: unknown, not finished, or with no node wait of the thread"},
			},
			JSONKeys: []string{"schemaVersion", "thread", "collected"},
			JSONNote: "schemaVersion 1; collected lists run, progress, completedAt, collectedAt and changed for every run recorded. changed is false when the run was already collected as it finished, which keeps the first collectedAt. " + jsonErrorNote,
			Notes: "Mutating. Every run ID is checked before anything is sent; then one request per run, in argument order, stopping at the first refusal, which names the runs recorded before it. " +
				"The coordinator reads each run's state and checks that a node wait of the thread targets it. " +
				"A run that is still running is refused with its attention and the command that clears it: attention on a running run goes away when its cause is resolved, not by collecting.\n\n" +
				collectionNote + "\n\n" +
				"An older coordinator cannot record collections: the command exits 7 with \"" + runCollectionUpgrade + "\" and records nothing.",
			Parsers: []parserSite{{Func: "parseCampaignCollectArgs"}},
		},
	}
}
