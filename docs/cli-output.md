# CLI output contracts

Help defaults to a command synopsis, options and the outcome contract, within
26 lines. Use `t3-steward <command> --help full` for option descriptions,
JSON keys, transport and exit-code details. Campaign authoring topics remain
under `t3-steward campaign help <topic>`.

## Campaign lists

`campaign list` and `backlog list` use the same parser and rendering.
`--state open` selects every nonterminal progress state. `--state terminal`
selects succeeded, failed, cancelled and skipped. It cannot be combined with
`--progress`; project, schedule, class, worker and quota filters still combine.
The state selector translates to the existing progress filter, so it works
with older coordinators and sends no new wire field.

Text has RUN, WORKFLOW, PROJECT, CLASS, STATE and TASKS columns. Project names
containing whitespace or quotes use Go double-quoted string syntax. Parse
quoted cells as strings rather than splitting all whitespace.

`campaign list --json` (also `backlog list --json`) prints:

```json
{
  "schemaVersion": 1,
  "version": "backlog.admin/v1",
  "kind": "workflows",
  "generatedAt": "2026-10-03T00:00:00Z",
  "workflows": []
}
```

The five envelope keys are always present. generatedAt is an RFC 3339 timestamp.
workflows is always an array, including for an empty answer. Each entry has
run, workflow and progress, the existing WorkflowSummary objects. The run
contains id, workflowId, progress and revision; workflow contains id, name,
project and class; progress contains counts including total, succeeded,
failed, cancelled and skipped. Consumers must ignore unknown object keys and
read schemaVersion first. Additive keys keep version 1; incompatible changes
require a new schemaVersion. This is a client output contract, separate from
the coordinator protocol version. Earlier CLI versions omit schemaVersion
and may omit workflows when empty.

## Registration and event selectors

`campaign submit DIR --idempotency-key KEY --register-only` retains a workflow
and its files without creating a run, attempts or a wake. Its response contains
workflowId and an empty runId; use the printed `schedules put` command to attach
a schedule. Retrying the same key and content replays registration. Mixing
registration and ordinary submission under one key is refused. Supervised
manifests and gates are refused because scheduled supervision is not implemented.

Upgrade the coordinator before using register-only: older coordinators reject
its new registerOnly request field. Ordinary submissions omit it and retain their
existing protocol. A coordinator with registrations must not be downgraded to a
version that requires every submission record to have a runId.
Schedules retain their existing limitation with cross-run inputs_from references:
those references are bound to a consumer run and are not rematerialized for each
scheduled occurrence. This change supports workflow-owned prompts and static inputs;
it does not extend cross-run schedule input semantics.

`backlog events <run>/<task>` resolves a task name or ID, then filters events for
that task and all its attempts. Filtering occurs in the client, including when
an older coordinator returns the whole run history.

## Task results and verification

`task result <run>[/<task>]` writes declared outputs and final messages under
the steward state directory, prints the paths and inlines final messages of
at most 4096 bytes in text. Larger final messages remain in the printed file;
JSON continues to inline the full final message. `--output .t3/results`
restores the earlier checkout location explicitly. The state-directory default
was already implemented before this milestone; stale documentation is corrected
here. No skill files in this repository reference the old path.

`campaign show`, its `status` alias and `backlog show` print verification
reports for each task's latest attempt in text. The result comes from the
retained command report, never from task progress. Missing reports say
not reported; inaccessible or malformed reports say unavailable with an
artifact-fetch command. JSON continues to expose the artifact metadata.
