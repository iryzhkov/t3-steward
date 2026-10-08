package main

// campaignFixHelpPages keeps the fix reference separate from shared campaign pages.
func campaignFixHelpPages() []helpPage {
	return []helpPage{{
		Path:    "campaign fix",
		Purpose: "create a bounded fix campaign from a succeeded review that requested changes.",
		Usage:   []string{"t3-steward campaign fix <run>/<review-task> --idempotency-key KEY [--round-limit N] [--gate CMD]... [--gate-timeout DUR] [--no-gate] [--commit RUN/TASK/NAME] [--context FILE]... [--out DIR] [--dry-run] [--json] [--notify-thread current|ID|--no-notify]"},
		Flags: []helpFlag{
			{Name: "--idempotency-key", Value: "KEY", Required: true, Text: "Required stable submission key; retrying the same source and options replays the same run."},
			{Name: "--round-limit", Value: "N", Default: "lineage limit, else 4", Text: "Maximum cross-run fix rounds (1..8). An explicit higher limit permits further rounds after escalation."},
			{Name: "--gate", Value: "CMD", Default: "lineage gate, else producer gate", Text: "Repeatable worker-owned final gate command; overrides inherited commands."},
			{Name: "--gate-timeout", Value: "DUR", Default: "30m for explicit gates", Text: "Timeout for each explicit gate command; the coordinator's verification timeout also applies."},
			{Name: "--no-gate", Default: "off", Text: "Explicitly omit the final gate when no --gate commands are supplied."},
			{Name: "--commit", Value: "RUN/TASK/NAME", Default: "the single declared commit consumed by the review", Text: "Select a declared commit of a succeeded task when automatic resolution is unavailable or ambiguous."},
			{Name: "--context", Value: "FILE", Default: "none", Text: "Repeatable additional context file; basename collisions with generated files or another context are refused before coordinator access."},
			{Name: "--out", Value: "DIR", Default: "private temporary directory removed after submission", Text: "Keep the generated campaign in a new local directory. Existing paths are refused."},
			{Name: "--dry-run", Default: "off", Text: "Requires --out. Generate, validate and print the plan without submitting."},
			jsonFlag("the fix receipt, including the ordinary submission receipt"),
			{Name: "--notify-thread", Value: "current|ID", Default: "current", Text: "Wake the calling thread or the named thread when the new run settles, through ordinary campaign submit."},
			{Name: "--no-notify", Default: "off", Text: "Submit without a thread wake. Mutually exclusive with --notify-thread."},
		},
		Exits:    coordinatorExits(),
		JSONKeys: []string{"schemaVersion", "runId", "replay", "sourceRun", "reviewTask", "reviewedCommit", "verdict", "lineage", "gate", "submission"},
		JSONNote: jsonErrorNote + " Round exhaustion starts the error message with review-round-limit-exhausted and reports roundsUsed/roundLimit.",
		Notes: "Uses the latest succeeded review attempt's recorded verdict; only a review without review_output may fall back to the exact first line VERDICT: ACCEPT or VERDICT: CHANGES_REQUESTED of review.md. Accepted, missing and unsucceeded verdicts are refused. A declared reviewed commit and repository environment are required.\n\n" +
			"The round_limit defaults to 4; each run declares up to two fix rounds. fix1 -> review2 -> fix2 -> review3 carries the brief and lineage forward. fix2 is a no-op after review2 accepts and does not count as work. With one round left, fix1 -> review2 is generated. The worker-owned gate runs on the last fix task; no agent gate task is generated. Gate precedence: --gate, --no-gate, lineage, producer; absent gates require an explicit choice.\n\n" +
			"review-round-limit-exhausted submits nothing and calls for a design pass or an explicit higher --round-limit. This counts cross-run fixes, separately from M16-4's in-task review checkpoints. It replaces mkfixchain's manual copying with retained artifact references; G7/N5 will later own declared loops and escalation. See docs/campaign-fix.md for the template, lineage and refusals. Text starts with run <id>, followed by the fix summary and ordinary submit's receipt and wake instructions.",
		Parsers: []parserSite{{Func: "parseCampaignFixArgs"}},
	}}
}
