# T3 Steward

T3 Steward protects interactive agent quota and schedules unattended agent work
across a small fleet. The coordinator owns scheduling; workers own host execution.
See [the system model](docs/architecture/system-model.md) for implemented behavior,
proposed Track S contracts and evidence limits.

## Language

**Schedule**: A recurring definition that may create runs at named times.
Avoid: cron task, recurring task.

**Trigger**: One observed firing of a schedule, possibly suppressed without a run.
Avoid: cycle (use it only for an internal reconciliation pass).

**Workflow**: An immutable submitted definition and input bundle.
Avoid: DAG task, job.

**Workflow run**: One execution of a workflow, created manually or by a schedule.

**Graph revision**: An immutable run-local snapshot of task and edge definitions,
implemented in S3, with parent, digest and audited amendment identity. It is distinct
from mutable progress and attempt revisions. Live amendments preserve run/task
identity, freeze assigned definitions and leave submitted workflow templates intact.

**Task**: One node with stable run-scoped identity. Its current attempt describes
execution; its definition is not overwritten to change execution history.

**Sink task**: The coordinator-owned terminal aggregate of all tasks in a run,
implemented in S1. It has no worker, provider route or attempt. Its published
result is immutable; further work requires a new run.

**Attempt**: One try to complete a task. Retry creates another attempt.

**Dependency**: A typed relation to another task. Ordinary execution requires
verified predecessor success and required artifact custody. The sink instead
waits for terminal state; node waits observe outcomes rather than authorize work.

**Assignment**: A committed decision binding an attempt to a worker, provider
route, execution identity and lease.

**Worker**: A host runtime that prepares workspaces and executes assigned T3
threads. The opt-in S4 persistent path separates configured bootstrap, authenticated
enrollment and observed readiness. Its execution catalog is coordinator-owned;
UpKeeper distributes bootstrap and releases. See [worker operations](docs/worker-operations.md).
Live multi-host qualification is still required before S4 closes.
Avoid: runner, execution host.

**Provider route**: An eligible model, provider instance, options and worker
combination. Its consumed quota buckets must be explicit in S6.

**Quota pool**: The current configured grouping of shared provider capacity and
concurrency. It is not a synonym for a single provider window.

**Quota bucket**: A provider/account window with class and scope. S6 makes this
the authority for observations, pacing, reservations and blocking.

**Admission state**: Permission to start/resume against the relevant quota
capacity. Current pools are replaced by bucket-derived route admission in S6.

**Required task**: Work expected in its requested time window, subject to automatic
hard quota safety controls. Avoid: urgent task.

**Surplus task**: Work admitted only from headroom after interactive demand,
required work, active reservations and reserve. Avoid: optional task, gated task.

**Reservation**: Coordinator-owned capacity/resource commitment tied to an
assignment. Financial expiry does not prove external execution stopped.

**Artifact**: Immutable retained input, output, checkpoint, log, diff or summary,
identified by producer, size and checksum.

**Checkpoint**: Durable evidence of incomplete work used for safe resumption.

**Admin command**: An authenticated, audited request to change scheduler intent,
such as start, delay, pause, resume, retry, skip or cancel. The existing manual
start override has an explicit user-controlled quota waiver; automatic work does
not inherit it.

**Node wait**: A durable observation of a task or sink outcome with a separate
wake-delivery identity, implemented in S2. An uncertain send remains recovery-required
until the stable T3 message ID is observed; it is never blindly retried.

**Recovery-required**: An uncertainty requiring evidence or operator reconciliation.
It is not success, a free resource, or permission to repeat an external effect.

**Supervision record**: The coordinator-owned statement that one workflow run is
supervised, holding the overseer route, the activation limits, the prompt artifact,
the current activation epoch and the supervision revision. Absence of the record is
the unsupervised case, which is every run authored without a `supervision` block;
no empty record is ever created. Avoid: supervision config (the manifest block is
the authoring form, the record is the durable one).

**Overseer**: The independently routed agent session that reviews a supervised run.
It runs as ordinary assigned work on a worker that advertises the campaign
supervision capability, on a provider instance and quota pool separate from the
run's own tasks, and it authenticates to the coordinator as a supervisor principal
scoped to one run and one activation epoch. It is not a second coordinator and it
issues no admin command. Avoid: supervisor agent, reviewer bot.

**Gate**: A declared review point that withholds dispatch of named downstream tasks
until an authorized acceptance is recorded. A gate observes the producers named in
`after` and protects the tasks named in `before`; a final gate protects run
settlement instead of a downstream task. A rejected gate becomes held rather than
re-armed, and reconsideration is an explicit operator transition. Avoid: approval,
checkpoint (a checkpoint is durable evidence of incomplete work).

**Hold**: A dispatch block placed by an overseer or an operator over a whole run or
over one branch and its dependency closure at a recorded graph revision. It is
nonterminal state, not failed verification, and it is owned by whoever placed it:
an overseer cannot clear an operator's hold. Avoid: pause (pause is an admin
command on scheduler intent), block.

**Supervision activation**: One bounded, schedulable turn of the overseer, carrying
its own epoch, lease, deadline and turn budget. An idle overseer holds nothing; an
active one occupies one executor slot for its duration. Correctness comes from the
activation record and the snapshot it is handed, never from conversation history,
so a replacement is started at a new epoch rather than resumed. Avoid: overseer
session, review run.

**Review incident**: A durable statement that something needs an overseer or an
operator decision, with a stable ID, the source event and task attempt that raised
it, a revision, a required disposition and an open, escalated or resolved state. An
unresolved or escalated incident withholds run settlement. An ordinary gate
acceptance closes only its own matching incident. Avoid: alert, review request.
