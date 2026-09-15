# Backlog administration

`t3-steward backlog` reads coordinator-owned workflow state and submits revision-fenced administrative commands. Task controls use a `<workflow-run>/<task>` selector:

```text
t3-steward backlog start <workflow-run>/<task> --reason TEXT
t3-steward backlog delay <workflow-run>/<task> --until RFC3339 --reason TEXT
t3-steward backlog pause <workflow-run>/<task> [--now] --reason TEXT
t3-steward backlog resume|cancel|retry|skip <workflow-run>/<task> --reason TEXT
```

Schedule definitions and controls use the schedule ID:

```text
t3-steward schedules put <schedule> --name TEXT --workflow ID \
  --cron "EXPR" --timezone IANA --reason TEXT \
  [--after-failure next-cycle|hold] [--disabled] \
  [--expected-revision N] [--request-id ID] [--json]
t3-steward schedules run <schedule> --reason TEXT
t3-steward schedules delay-next <schedule> --until RFC3339 --reason TEXT
t3-steward schedules enable|disable <schedule> --reason TEXT
t3-steward schedules delete <schedule> --reason TEXT
```

Every control accepts `--command-id ID` for exact replay and `--json` for machine-readable output. A command is stored before execution, then the coordinator applies or rejects it atomically with its audit event. Reusing a command ID with different intent is rejected. A stale target revision is recorded as a durable rejection rather than overwriting newer state.

`start` bypasses normal timing, surplus admission, and ordering, but it still checks successful dependencies, live resource-lock owners, a fresh ready worker, and hard quota admission. It never bypasses draining or closed quota state. `resume` additionally requires the assigned worker and assigned quota route to remain safe. `pause` atomically records draining state and a worker delivery intent bound to the current assignment, thread, workspace, worker epoch, and provider route. Without `--now`, the worker is asked to checkpoint; with `--now`, it is asked to hard-stop. The attempt becomes `paused` or `paused-uncheckpointed` only after acknowledgement, and pending delivery survives restart. Cancelling a task also cancels unfinished descendants and updates the workflow-run projection in the same transaction. Manual schedule runs use the normal overlap and failure-hold policy and the transactional schedule-trigger path.

`delete` removes a schedule definition. It is a revision-fenced command like the other schedule controls rather than a separate transport operation, because a schedule row carries the revision that fence needs. It is refused while the schedule's own run is still open, since removing the definition under a running occurrence would drop the overlap fence that refuses a second one.

When it applies, the schedule row and its template versions are removed. The template versions have to go: they are immutable and keyed by schedule and version, so a surviving version 1 would refuse the definition of a schedule later recreated under the same identity.

The trigger records are kept, and that is deliberate. A trigger's occurrence key is the only thing reserving the deterministic identity of an occurrence that already fired — the trigger ID and the workflow-run ID are both derived from the schedule ID and the nominal time, and the runs a schedule created are kept. Removing the triggers would leave those run identities allocated but unfenced, so a schedule recreated under the same identity that ever reached a nominal time it had already fired would fail inside the seed, roll back, and fail again on every subsequent tick: a schedule that can never run again. Keeping them costs nothing and does not cause a recreated schedule to replay old occurrences, because the timer anchors each schedule at the latest nominal time it holds a trigger for and never regenerates anything at or before it.

Workflow runs the schedule already created are untouched and keep naming it, and the audit event of every firing is immutable and stays, so the history of what ran is not lost with the definition. A deleted schedule's trigger records remain readable in the database but are no longer reachable through `schedules history`, which lists by schedule.

An accepted occurrence creates a run that can be executed, not only a run row: one first attempt per task, the workflow's prompt and input artifacts rebound into the new run's own custody, and the first immutable graph revision. It shares that machinery with the graph clone. If the run cannot be seeded, the whole firing is rolled back and no trigger claims a success, so the occurrence stays available to a later attempt.

`backlog status` includes the active runtime mode, coordinator owner and epoch,
transport kind, fresh/stale worker and quota counts, reconciliation issues,
unknown execution IDs, and durable artifact-custody metadata incidents. Any
stale or incident projection degrades the reported runtime health.

A schedule that cannot fire is isolated: it does not stop the other schedules in
the same reconciliation tick, and it is named in the reconciliation issues as
`schedule:<id>:unschedulable: <reason>`, which degrades the reported health until
the next tick finds it healthy. That is the only signal, because an isolated
failure breaks nothing else that would make an operator look.

Intake that can never be accepted is quarantined, reported once and then
silent. Its audit event names no workflow run, so it is read with its own view:

```text
t3-steward backlog quarantine [--json]
```

The view is read-only and lists the intake key, the namespaced key of the
durable record, the content digest it was recorded for, the time and the
reason, together with the rule that recovers it: change the file, because a
different digest releases the marker and the submission is tried again.

A refusal the file cannot fix, such as a project no alias mapped, is cleared
deliberately once the configuration is right:

```text
t3-steward backlog quarantine release <key> --reason TEXT [--json]
```

The release is audited with the operator and the reason. Releasing a key that
holds no marker reports that there was nothing to release rather than failing,
so an ambiguous response is safe to repeat.

Unknown assignments require a separate evidence-bound recovery operation:

```text
t3-steward backlog recover <assignment> --outcome stopped|failed \
  --coordinator-epoch N --assignment-epoch N --attempt-revision N \
  --evidence-id ID --evidence-sha256 HEX --reason TEXT \
  [--recovery-id ID] [--json]
```

The local socket authenticates the operator from the Unix peer credential and
ignores claimed identity. All epochs and the attempt revision must still match,
and exact replay requires the same recovery ID and content. Recovery never
opens quota or directly starts, resumes, claims, or dispatches work.

Artifact metadata is available with `artifact show`; verified coordinator-owned content is retrieved with:

```text
t3-steward backlog artifact get <artifact>
t3-steward backlog artifact get <artifact> --output PATH
```

Inline output is limited to approved text media types and 16 MiB. ASCII and Unicode terminal controls are escaped, and malformed UTF-8 is replaced. Other media types require `--output`; downloads are written with owner-only permissions, reject a symlink at any output-directory level, and never overwrite an existing path. The coordinator verifies the retained size and SHA-256 digest before returning any content.
