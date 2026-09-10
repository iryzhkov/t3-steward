# T3 Steward

T3 Steward protects interactive agent quota and schedules unattended agent work across a small fleet.

## Language

**Schedule**:
A recurring definition that may create workflow runs at named times.
_Avoid_: Cron task, recurring task

**Trigger**:
One observed firing of a schedule. A trigger may be suppressed without creating a workflow run.
_Avoid_: Cycle

**Workflow**:
An immutable directed acyclic graph submitted for execution.
_Avoid_: DAG task, job

**Workflow run**:
One execution of a workflow, created manually or by a schedule.
_Avoid_: Occurrence, cron run

**Task**:
One node in a workflow.
_Avoid_: Step, job

**Attempt**:
One try to complete a task. Retrying a task creates another attempt.

**Dependency**:
A task that must succeed before another task becomes ready.
_Avoid_: Prerequisite

**Assignment**:
A committed decision to execute one task attempt on one worker through one provider route.

**Worker**:
A host that prepares workspaces and runs assigned T3 threads.
_Avoid_: Runner, execution host

**Provider route**:
An eligible model, provider instance, options, and worker combination for a task.
_Avoid_: Model selection

**Quota pool**:
A provider limit shared by one or more provider instances or workers.
_Avoid_: Provider quota

**Required task**:
A task expected to run in its requested time window, subject to hard quota safety controls.
_Avoid_: Urgent task

**Surplus task**:
A task allowed to run only from quota predicted to remain after interactive demand, required work, active reservations, and the safety margin.
_Avoid_: Optional task, gated task

**Artifact**:
An immutable input, output, checkpoint, log, diff, or summary retained with a workflow run.

**Checkpoint**:
A durable description of incomplete work used to resume a paused attempt safely.

**Admission state**:
The scheduler's current permission to start or resume work against a quota pool.
_Avoid_: Gate

**Admin command**:
An audited request to change scheduler intent, such as start, delay, pause, resume, retry, skip, or cancel.
_Avoid_: Control action
