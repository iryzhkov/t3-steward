# H4 ADR: a parked turn is not a finished task

Status: accepted. Freezes the contract before implementation.
Date: 2026-09-14
Authority: `run-f009add9e09badcc8ef9cf902488c717`, task `qualify_publish`, attempt
`attempt-512767b9961aa9687a9410185a9ab7ea`, thread `thread-303f334c41a36ec19af90de6867b0fb6`.

## The failure this record exists to make impossible

The task registered two CI waits on its own thread and ended its turn. The worker saw a stopped
thread, collected outputs immediately, and the coordinator recorded the turn outcome marker
`done`. Verification ran against outputs the task had not written yet and failed:
`verification command failed (1): test -s final-commit.txt; missing declared output:
final-commit.txt, handoff.md, publication.md`. The attempt became terminal at 17:30:29Z.

Fifteen seconds later, at 17:30:44Z, the steward woke that same thread because the first CI wait
had settled. The thread went on working and published a release, with no coordinator ownership of
anything it did.

Two separate defects meet there. The first is that an ended turn is read as a finished task. The
second is that a terminal attempt left a thread authorized to keep producing external effects.

## Decision

A task-bound wait is a coordinator-owned, revision-fenced record, and registering one is the
evidence that the current turn is parking rather than completing.

The lifecycle becomes:

`running → waiting_external → resumed → running → verifying → succeeded`

`waiting_external` is a new progress state, not a reuse of `needs-input`: needs-input means a
human must answer, waiting_external means a machine condition has not settled, and an operator
reading a queue needs to tell those apart. It is a new control state as well, which holds no
provider slot and is explicitly **not** quiescent, so the workflow sink cannot settle while any
attempt is waiting.

## Registration and its atomicity

A task registers with `t3-steward wait add --task current ...`. Resolving `current` requires the
agent to know its own identity, which today it does not: the full execution identity stops at the
worker and never reaches the agent process. So the identity is injected into the task environment
— workflow run, task, attempt, attempt revision, assignment and canonical T3 thread — and the
contained execution path's environment allowlist is widened by exactly those entries and nothing
else.

Registration and the transition to `waiting_external` are one operation: atomic, idempotent under
a repeated request ID, and fenced on the attempt. A registration that names an attempt which has
moved on is refused rather than applied to whatever the attempt has since become.

Amendment, 2026-09-14, after multi-process qualification. The first implementation read "fenced on
the attempt revision" as equality with the revision injected into the task, and that fence could
never pass: the coordinator stamps that revision when it builds the execution package and then
advances the attempt itself, on the assignment claim and again when the worker reports the thread
running. Every registration from a real task was refused as stale. The injected revision is kept,
renamed `IssuedRevision`, as evidence of what the task was told.

The fence is on the attempt's turn, evaluated inside the registering transaction:

- the attempt still has a live turn — progress `active` or `waiting-external`, control
  `preparing`, `running`, `resuming` or `waiting-external`. An attempt being verified, released,
  paused or drained has moved on underneath the registering thread;
- the registering thread is the attempt's own thread, when the attempt names one;
- the commit is a compare-and-set on the revision read in that same transaction.

The protection the original fence was written for is unchanged, and now it is the property that
is actually checked: a registration cannot be applied to an attempt that has moved on, and a
registration racing a turn completion still loses exactly one of the two.

A terminal attempt refuses new task-bound waits, with a structured error saying the attempt is
terminal and naming its outcome. That refusal is what stops a thread that has already lost its
task authority from quietly acquiring a new reason to keep working.

## The race, and who wins

The dangerous interleaving is the observed one: a wait is registered at about the same moment the
turn ends. Both paths compare-and-set the attempt revision, so exactly one commits first.

- **Wait first.** The attempt is `waiting_external` when the ended turn is observed. The worker
  does not collect, the coordinator does not verify, dependents and the sink do not settle. A
  `done` marker arriving in this state is a contradiction: it is rejected and recorded as a
  reconciliation event, never silently honoured.
- **Completion first.** The attempt is already verifying or terminal when the registration
  arrives. Registration is refused with the terminal reason, and the agent receives that refusal
  as a normal command error it can act on.

The attempt can therefore never be both waiting and terminal. The invariant is stated once, in
the transition function, and tested by racing the two paths against a shared store.

## What is released, what is held

Released while waiting: the executor slot, the CPU, memory and scratch reservation, the provider
slot and any quota tally. This is the point of the state — a task waiting on CI for an hour must
not occupy a worker's executor capacity.

Held while waiting: the attempt and its revision chain, the T3 thread, the workspace, artifacts
and dependency mounts, the history, the assignment's coordinator ownership, every named resource
lock the task declared, and any directory writer binding.

Locks are held on purpose. The waiting thread can still touch its workspace when it wakes, so
releasing a lock would let a conflicting writer in while the first writer is merely parked.
Directory bindings have no deadline by design, so a held binding needs a bound from somewhere
else: every task-bound wait carries a maximum duration, the coordinator enforces it, and expiry
wakes the task with a structured timeout rather than leaving the fleet blocked forever.

## Waking

When the condition settles, the same T3 thread and the same logical attempt resume, with the wait
result included in the initial context. Placement, executor capacity, leases and released locks
are reacquired through the normal paths; resumption is a new turn on an existing attempt, not a
new attempt.

A failed or timed-out wait wakes the task with structured evidence — which wait, which condition,
which exit status, how long it ran — so the agent can either handle it or produce an honest final
failure. Silence is not an outcome.

Multiple waits have explicit semantics: `each` wakes on the first settlement, `all` wakes when
every member has settled. A task stays in `waiting_external` until its wake condition is met.

Coordinator restart, worker restart, duplicate delivery, a lost wake response, wait cancellation,
and a condition settling concurrently with turn completion are all idempotent: one wake, one
resumed turn, one verification.

## Verification happens once, at the end

While a task is waiting, declared outputs are not collected, verification does not run, dependents
do not settle and the sink does not settle. Collection and verification happen after the turn that
ends with no live task-bound wait. Outputs written after waking are the outputs that are collected.

## Fencing a thread that must be abandoned

If an irreconcilable lifecycle error occurs — a contradiction that cannot be resolved into a
single consistent attempt state — the thread's task authority is revoked before the task is
marked terminal. Revocation invalidates the dispatch token that lets the thread claim task
effects, so a thread that keeps talking is refused rather than obeyed. A task must never become
terminal while its thread is still authorized to act for it; that ordering is the whole lesson of
the observed failure.

## Interactive waits are untouched

A wait registered from an interactive session that is not executing a backlog or campaign task
remains exactly what it is today: it wakes the selected interactive thread and creates or alters
no workflow state. The two kinds are distinguished in help by what they wake and what they can
change, and `--task current` outside a task is an error that says so.

`current` resolves through the injected canonical T3 thread identity when present. Outside a task
it falls back to the provider session identifiers, and it treats a provider-local session ID as
what it is — an input to resolution, never a canonical thread ID. Claude, Codex and OpenCode all
resolve; an ambiguity names the candidates and asks for `--thread`.

## Alternatives rejected

Inferring the park from the agent's final message was rejected. A marker in prose is exactly the
signal that failed: the model emitted `done` while two waits were live.

Keeping the executor slot while waiting was rejected as the simple option. A CI wait is minutes
to hours; holding a slot that long on a three-worker fleet turns one parked task into a stalled
queue.

Creating a new attempt on wake was rejected. The workspace, artifacts and thread belong to the
attempt, and a new attempt would either abandon them or have to adopt them, which is the same
ownership problem with more records.

Letting the sink settle on a waiting task, by giving it a quiescent control state, was rejected
for the obvious reason: that is the observed bug wearing a different name.
