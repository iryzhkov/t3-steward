# S7 ADR: optional campaign supervision

Status: adopted (2026-09-16). Implemented on `feature/campaign-supervision`;
live qualification on the fleet is still outstanding.

This record fixes the twelve open questions of
[the design seam review](../plans/campaign-supervision-seams.md), section 10, as
decisions. Each one states what was chosen and why the alternative was not, so a
later reader can tell a deliberate constraint from an accident.

## Scenario

A campaign author wants some of the work reviewed by a different, independently
routed agent before it continues, without turning every campaign into a
supervised one and without the reviewer becoming a second scheduler. The feature
is therefore optional in the authoring format, optional in the durable schema and
absent by default in every rendering.

## Decisions

### 1. Credential co-tenancy is a documented limitation, not a solved problem

A supervisor authenticates as an ordinary admin client, and the coordinator binds
that principal server-side to one workflow run and one activation epoch. That
bound is enforced on every request, on both carriers, and it is what actually
limits the capability.

Credential placement is not a boundary and is not claimed as one. The admin
credential is an owner-only file under
`~/.config/upkeeper/secrets/f03-admin/<client>`, and campaign tasks run as
full-access agent sessions under the same user on the same host, so a task on a
host that also holds a supervisor credential can read it. Contained execution
would prevent that by passing only the task identity environment into the
process, but contained execution is an operator qualification path today and not
what ordinary tasks take.

The decision is to ship the server-side scope, state the co-tenancy exposure in
user-facing documentation, and never describe supervision credentials as
isolated. What the exposure amounts to is bounded by the scope: a task that reads
a supervisor credential gains the authority to decide its own run's gates while
an activation of that run is live, which is wrong, but it is not cross-run
access, and between activations the epoch fence makes the credential useless.
Two deployments remove the exposure rather than bounding it: run supervision on a
host that executes no campaign tasks, or make contained execution the default for
campaign tasks. Revisit when the second becomes true.

### 2. Supervision is an inline blocker in the planner, not a planning constraint

A gate is a property of the task, not of a candidate worker or route. A planning
constraint is evaluated once per candidate, so expressing supervision as one
would compute the same answer N times and report it N times in the decision's
candidate list. The readiness predicate is therefore evaluated inline beside the
other progress blockers, which keeps the explanation readable and the work done
once.

### 3. A clone or rerun inherits configuration but no verdicts

A clone or rerun inherits the supervision configuration: the route, the limits
and the gate definitions. It inherits no acceptance, no hold and no incident. An
acceptance is a statement about specific evidence, and the clone does not have
that evidence; carrying one forward would make a fresh run start with a review
nobody performed on it.

### 4. An activation is ordinary assigned work and occupies one executor slot

An activation is dispatched as assigned work on a worker, so an active review
occupies an executor slot for its duration. An idle overseer holds nothing: no
slot is reserved between activations and no task-bound wait is registered for
one.

The alternative, a coordinator-side model client, was rejected because it
reintroduces exactly the raw model processes the coordinator is not allowed to
launch. One slot, bounded by the activation deadline, is the cheaper cost, and
the slot cost is recorded in the activation receipt so it is visible rather than
implicit.

### 5. The overseer route is configured, not inferred to be stronger

The design requires an *independently configured* route, not a stronger model.
No registered model is meaningfully "stronger" in a way the coordinator could
verify, and a system that silently picked one would be asserting something it
cannot check. The operator chooses.

The route adopted for the shipped example and for qualification is `claudeAgent`
/ `claude-fable-5-1` / `claude-main` for the overseer, with the campaign's own
tasks on `codex` / `gpt-5.6-sol` / `codex-main`. Both models are advertised by
the live fleet since the catalog moved to the `"*"` policy on steward rc.56. The
point of the split is pool separation: a busy worker pool cannot starve reviews,
and a busy overseer cannot starve the workers.

### 6. "Held" is a derived label, not a stored progress state

A stored progress state would have to be understood by every terminal predicate,
the quiescence check, the planner, the sink projection and every status renderer.
A label is a rendering concern computed from the gates and holds that already
exist. It is also the cheaper decision to reverse.

### 7. Amendment invalidates conservatively, and carries forward only against a receipt

An amendment that touches a gate's observed or protected set invalidates that
gate's acceptance unconditionally. An amendment that touches neither may carry
the acceptance forward, and only against an audit receipt written in the same
transaction. A false invalidation costs one review; a false carry-forward is an
unreviewed dispatch, and the two are not comparable.

### 8. Fleet re-enrollment is a prerequisite of live qualification

The verification gates that need a live fleet cannot run until the fleet is
re-enrolled. That is recorded as a prerequisite rather than worked around, and
waiting for it is done by registering a steward wait rather than by polling.

### 9. Version 1 resolves one incident at a time

Bulk incident close is deferred. The plan's "exact set with expected revisions"
rule is then vacuously satisfied, the CLI is smaller, and one race does not
exist.

### 10. Escalation reuses the node-wait outbox and the configured notify thread

No new messaging integration and no recipient discovery. Escalation delivery goes
through the same outbox machinery the node waits use, to the thread the campaign
already configured for notification, and is deduplicated on incident ID so a
re-escalation of the same incident does not notify twice.

### 11. Compatibility is manifest strictness plus worker capability strings

`internal/compat` pins the T3 *server* version and is not the
CLI/coordinator/worker negotiation surface. The two real mechanisms are used
instead, and the plan's wording was corrected so a later reader does not go
looking for a negotiation that is not there.

The first is `KnownFields(true)` in the manifest decoder. A binary without the
`supervision` and `gates` fields refuses a supervised manifest outright rather
than ignoring keys it does not understand, and it does so on every path, because
`campaign validate`, `campaign plan` and coordinator ingestion all go through the
same parse. The binary at `origin/main` 94a28a0 answers
`t3-steward campaign validate docs/examples/campaign/supervised-three-node` with
`field supervision not found in type backlog.Manifest` and exit 1, and that text
is recorded as the contract in `internal/backlog/manifest_compat_test.go`.
Since rc.97 the refusal names the release that refused the field first, and the
decoder text that follows names the manifest object instead of the Go type: a
binary of this release would print `field supervision not found in the workflow`.
The refusal itself is unchanged.

The second is the worker inventory capability `campaign-supervision-v1`,
enforced at placement, at `campaign check` and again at the worker exchange.
The handshake capability list is advisory; the durable snapshot inventory is the
enforced one.

### 12. Schema 18 is forward-only

The migration adds nine tables with `CREATE TABLE IF NOT EXISTS` and creates no
row for any existing run: the absence of a supervision record is the unsupervised
case, which is the current behaviour, so there is no backfill and no empty record
is ever written.

There is no reverse migration, and one was not written. The store has no
reverse-migration mechanism to follow, `Migrate` only moves forward, and an older
binary refuses a database whose recorded version exceeds its own constant rather
than degrading it. Rollback after migrating is a coherent stopped backup restore
with an explicit loss window, never a binary downgrade against migrated state.
The loss window, and the reconciliation a lost idempotency receipt forces, are
spelled out in [Backlog-v2 operations](../backlog-v2-operations.md).

Backup and restore required no change. The snapshot copies the whole stopped
database file and the artifact tree with per-file checksums, so every new table,
the append-only decision history and the idempotency receipts are captured by
construction, and there is no table enumeration to keep in step with the schema.

## Consequences

An unsupervised campaign is unchanged. Its plan text, including the content
digest the coordinator ingests, is byte-identical to the release before this one;
its settlement projection gains no bytes, because a run with no supervision rows
contributes an absent read set rather than an empty one; and its execution
packages carry no supervision object. The measured cost is two indexed reads per
fenced item.

A supervised campaign requires a fleet on this release or newer, an operator who
has configured a separate overseer route, and an operator who understands that
the supervisor credential on a worker host is scoped rather than isolated.
