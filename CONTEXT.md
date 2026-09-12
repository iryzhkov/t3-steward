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

**Graph revision**: A proposed Track S immutable snapshot of a run's task and edge
definitions, with parent, digest and audited amendment identity. It is distinct
from mutable execution progress and attempt revisions. Live amendment versus
resubmission remains a user decision.

**Task**: One node with stable run-scoped identity. Its current attempt describes
execution; its definition is not overwritten to change execution history.

**Sink task**: The proposed coordinator-owned terminal aggregate of all tasks in
a run. It has no worker, provider route or attempt. S1 implements it.

**Attempt**: One try to complete a task. Retry creates another attempt.

**Dependency**: A typed relation to another task. Ordinary execution requires
verified predecessor success and required artifact custody. The sink instead
waits for terminal state; node waits observe outcomes rather than authorize work.

**Assignment**: A committed decision binding an attempt to a worker, provider
route, execution identity and lease.

**Worker**: A host runtime that prepares workspaces and executes assigned T3
threads. Avoid: runner, execution host.

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

**Node wait**: A proposed durable observation of a task or sink outcome with a
separate wake-delivery intent. S2 implements it.

**Recovery-required**: An uncertainty requiring evidence or operator reconciliation.
It is not success, a free resource, or permission to repeat an external effect.
