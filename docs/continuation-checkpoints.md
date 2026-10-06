# continuation.md checkpoints

A task keeps `continuation.md` at its workspace root: its goal, checklist,
current step, blockers and last verification. Steward treats that file as a
durable checkpoint, so that its content survives an attempt that dies, is
paused, or is superseded, and reaches the next attempt of the same task.

## When a snapshot is taken

The worker snapshots the file at three boundaries:

- **Turn end**: a provider turn has stopped, whether the task parks on a
  task-bound wait or is about to be collected.
- **Pause**: the worker's own quota pause has stopped the turn, or an operator
  or coordinator drain or hard stop was accepted.
- **Collection**: the result is being collected, before verification runs in
  the workspace. A failed attempt (`CollectFailure`) is collected too.

A snapshot keeps at most 64 KiB. A larger file is cut at a character boundary
and ends with a marker naming the original size. A missing file is not an
error: it is reported as "no checkpoint". A symlink, or anything that is not a
regular file, counts as missing.

## Where it is kept

On the worker the latest snapshot lives in the attempt directory, beside the
workspace and never inside the task's tree, so nothing the task commits can
carry it: `<runs>/<run>/<task>/<attempt>/continuation/` holds the snapshot
bodies by digest and a `state.json` naming the latest. The worker journal's
attempt record carries the latest checkpoint's digest, size, boundary and time
(not its content).

Snapshots are idempotent per attempt and turn. The first snapshot taken for a
turn stands; a replay of that turn after a worker or coordinator restart
returns the latest snapshot unchanged, however many turns ago it was:
`state.json` lists the latest 256 turn keys, and an older key is kept as a
marker file under `turns/`, written before the state that drops it.
Unchanged content is not a new snapshot. Each distinct snapshot gets the next
sequence number, so the latest never moves back to an older one.

A pause owes its snapshot durably. The journal records the obligation in the
same update that records the stop (an accepted drain or hard stop, or the
worker's own quota pause), and clears it once the snapshot is taken. A worker
that dies, or fails to read or write the snapshot, in between takes it on its
next reconcile pass, before the attempt can resume.

## How it reaches the coordinator and the next attempt

A worker sends snapshots only when the package declares the package
capability `continuation-checkpoint-v1`, which tells it that the coordinator
accepts them; an older coordinator never receives an object it would reject.

Each new snapshot taken at a turn end or a pause is handed to the coordinator
at once, while the attempt runs, so that an attempt superseded before it has
a result has already handed its latest checkpoint on. The upload rides the
checkpoint channel (`upload-<assignment>-checkpoint-continuation-<epoch>-<sequence>`)
with two objects, the snapshot `continuation-<attempt>-e<epoch>-<sequence>` and
its metadata `continuation-meta-<attempt>-e<epoch>-<sequence>`, where epoch is
the assignment epoch of the dispatch that took it: an attempt offered again
after it lost its lease starts its sequence again at 1, and its snapshots never
collide with the earlier dispatch's. The coordinator imports it
under the authority of the dispatch that took it, not the assignment's current
state: the assignment, its epoch, the worker and the worker epoch must match,
and the assignment must have been claimed (it may since have lost its lease,
been released or completed), for an attempt that has not moved to another
assignment. A snapshot is evidence of what the attempt already did, so a
snapshot the worker queued before the attempt was superseded is still
imported when the worker is next polled; it grants the old attempt nothing
else, and its results and lifecycle keep their fences. A dispatch that was
never claimed, or that the assignment's next epoch replaced, refuses it. The
metadata must describe the snapshot, under the attempt's own identity and
sequence, captured no later than the upload. An upload that fails any of
these is refused for good. Each snapshot is handed on once; one that could not
be handed on is retried at the attempt's next boundary.

In each exchange with a worker the coordinator imports that worker's pending
continuation snapshots first, before it expires leases and builds any offer,
and handles every other upload after.

The latest snapshot also travels with the attempt's result, as
`continuation/snapshot.md` and `continuation/checkpoint.json`, under the
attempt's fixed identity. It goes only where the upload, by object and in
total, still fits the package's limits, for a failed result as for a
successful one; it never costs the result its publication. A snapshot whose
metadata does not describe it is dropped with a warning and never costs the
result its import. The coordinator dates every snapshot artifact by its
capture time.

The coordinator offers the capability to a worker that advertises it. The
decision, and for a retry the snapshot it carries, is frozen with the
assignment's first offer (schema V39, `coordinator_assignment_continuations`),
so a replayed offer is the same package. A retry of a task, and an attempt
offered again at the next assignment epoch after its lease was lost or its
assignment released, receives the latest snapshot the same task in the same
run left, including the attempt's own from its earlier dispatch, as the
static input `.t3/inputs/continuation/previous.md`, and its first-turn prompt
says so in one sentence.

Latest follows the task's execution order, never a worker's clock: the
attempt with the higher number wins; within one attempt the snapshot its
result carries wins (it is the latest the attempt had when it collected), then
the higher assignment epoch, then the higher sequence. Capture time only
orders attempts whose number is unknown or equal, comparing each attempt's own
latest. The order in which snapshots were stored, imported or replayed does
not matter.

The decision is frozen with the first offer, so a snapshot that reaches the
coordinator only after the replacement's first offer is not carried by it.
That happens only when the worker holding it cannot be reached before then (a
lost or partitioned host): the snapshot is imported when the worker
reconnects and counts toward the task's latest from then on, for its reports
and any later attempt.
A snapshot of an earlier dispatch that is polled only after the attempt was
offered again at the next epoch is refused: the coordinator keeps only the
current dispatch's worker identity, so it can no longer authenticate the
earlier one. A reachable worker has its snapshots imported in every exchange,
before any offer is built, so this needs the worker to stay unreachable from
the end of the lease until the attempt is offered again.

A pause snapshot that still cannot be taken when the attempt resumes is
forgone, with a warning, rather than taken later from the next turn; the
resume is never held back for it, and the next turn end takes a snapshot
as usual.

## Where it is reported

`backlog task show`, `campaign explain` (`backlog explain`) and `task result`
report the task's latest checkpoint as
`checkpoint: continuation.md <size> bytes captured <time> by <attempt>`, or
`checkpoint: no checkpoint`. `campaign show` adds the line under a task that
has one. The JSON forms carry `checkpoint` with the attempt, artifact ID, size
and capture time. None of them prints the content; `backlog artifacts` lists
the snapshot artifact and `backlog artifact get` fetches it.
