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
```

Every control accepts `--command-id ID` for exact replay and `--json` for machine-readable output. A command is stored before execution, then the coordinator applies or rejects it atomically with its audit event. Reusing a command ID with different intent is rejected. A stale target revision is recorded as a durable rejection rather than overwriting newer state.

`start` bypasses normal timing, surplus admission, and ordering, but it still checks successful dependencies, live resource-lock owners, a fresh ready worker, and hard quota admission. It never bypasses draining or closed quota state. `resume` additionally requires the assigned worker and assigned quota route to remain safe. `pause` atomically records draining state and a worker delivery intent bound to the current assignment, thread, workspace, worker epoch, and provider route. Without `--now`, the worker is asked to checkpoint; with `--now`, it is asked to hard-stop. The attempt becomes `paused` or `paused-uncheckpointed` only after acknowledgement, and pending delivery survives restart. Cancelling a task also cancels unfinished descendants and updates the workflow-run projection in the same transaction. Manual schedule runs use the normal overlap and failure-hold policy and the transactional schedule-trigger path.

Artifact metadata is available with `artifact show`; verified coordinator-owned content is retrieved with:

```text
t3-steward backlog artifact get <artifact>
t3-steward backlog artifact get <artifact> --output PATH
```

Inline output is limited to approved text media types and 16 MiB. ASCII and Unicode terminal controls are escaped, and malformed UTF-8 is replaced. Other media types require `--output`; downloads are written with owner-only permissions, reject a symlink at any output-directory level, and never overwrite an existing path. The coordinator verifies the retained size and SHA-256 digest before returning any content.
