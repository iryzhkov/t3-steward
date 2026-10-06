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
checkpoint channel (`upload-<assignment>-checkpoint-continuation-<sequence>`)
with two objects, the snapshot `continuation-<attempt>-<sequence>` and its
metadata `continuation-meta-<attempt>-<sequence>`. The coordinator imports it
under the same fences as any checkpoint: the assignment, its epoch, the worker
and the worker epoch must match, with the assignment claimed by a running
attempt or completed with a settled one. The metadata must describe the
snapshot, under the attempt's own identity and sequence, captured no later
than the upload. An upload that fails any of these is refused for good. Each
snapshot is handed on once; one that could not be handed on is retried at the
attempt's next boundary.

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
so a replayed offer is the same package. A retry of a task receives the latest
snapshot an earlier attempt of the same task in the same run left, as the
static input `.t3/inputs/continuation/previous.md`, and its first-turn prompt
says so in one sentence.

A snapshot that is still on its way to the coordinator when the replacement
attempt is first offered is not carried by that offer: the decision is frozen.
A snapshot the coordinator has not imported when the attempt's assignment
moves on (a worker host that is lost or partitioned, and reconnects only
afterwards) is refused by the fences above and stays only in that worker's
custody; the replacement receives the latest snapshot imported before.

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
