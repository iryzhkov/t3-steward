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
returns the latest snapshot unchanged. Unchanged content is not a new
snapshot. Each distinct snapshot gets the next sequence number, so the latest
never moves back to an older one.

## How it reaches the coordinator and the next attempt

The snapshot travels with the attempt's result, as
`continuation/snapshot.md` and `continuation/checkpoint.json` (the metadata),
both artifacts of kind `checkpoint` under the attempt's own identity. A worker
sends them only when the package declares the package capability
`continuation-checkpoint-v1`, which tells it that the coordinator accepts
them; an older coordinator never receives an object it would reject. A
snapshot whose metadata does not describe it is dropped with a warning and
never costs the result its import. The coordinator dates the snapshot artifact
by its capture time.

The coordinator offers the capability to a worker that advertises it. The
decision, and for a retry the snapshot it carries, is frozen with the
assignment's first offer (schema V39, `coordinator_assignment_continuations`),
so a replayed offer is the same package. A retry of a task receives the latest
snapshot an earlier attempt of the same task in the same run left, as the
static input `.t3/inputs/continuation/previous.md`, and its first-turn prompt
says so in one sentence.

Because the coordinator receives snapshots with results, a running attempt's
newer turn-end snapshots are visible in its worker's journal until its result
arrives.

## Where it is reported

`backlog task show`, `campaign explain` (`backlog explain`) and `task result`
report the task's latest checkpoint as
`checkpoint: continuation.md <size> bytes captured <time> by <attempt>`, or
`checkpoint: no checkpoint`. `campaign show` adds the line under a task that
has one. The JSON forms carry `checkpoint` with the attempt, artifact ID, size
and capture time. None of them prints the content; `backlog artifacts` lists
the snapshot artifact and `backlog artifact get` fetches it.
