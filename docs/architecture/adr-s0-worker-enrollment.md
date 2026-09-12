# S0 ADR: worker enrollment, catalog ownership and transport

Status: accepted design; implementation/enrollment belongs to S4.

## Scenario and authority

Normandy is the coordinator and the only enrolled worker. Homelab and omarchy-pc
need independently recoverable worker runtimes, not another scheduling authority.
The coordinator owns the effective catalog: projects, eligible workers, provider
routes, quota bucket bindings and setup-profile references. Workers own host-local
paths, secret resolution, capabilities and execution observations.

The current duplicated full catalogs and manually synchronized epochs are a drift
source. S4 replaces them with a coordinator-owned, versioned catalog and a bounded
worker-scoped projection delivered through the authenticated protocol. UpKeeper
distributes host bootstrap configuration and secret references, not quota policy
copies. An enrollment binds worker identity, current epoch, allowed coordinator,
transport principal, credential reference, capabilities and accepted catalog digest.

## Contract and lifecycle

Enrollment is an authenticated, audited admin operation. A fresh worker reports
its actual capabilities and provider availability; the coordinator admits it only
after identity, credentials, catalog digest and freshness match. A configured
capability is not proof that its provider credentials work. Workers must advertise
`git` and `huyang`, and only provider routes available on that host.

Worker states distinguish configured, enrolled, observed, stale, draining and
recovery-required. Status derives observed workers from the same accepted snapshot
store the planner uses; a configured worker without a snapshot remains visible
with an explicit reason. Effective concurrency limits must name their source
(config, accepted projection or runtime reservation), resolving the reported 1/5
limit discrepancy without guessing which source is current.

Restart advances the coordinator epoch while preserving worker custody. A worker
epoch change invalidates old offers/claims; it never proves a running old effect
has stopped. Unknown execution retains ownership until reconciled. Unclaimed offers
may be superseded only with evidence they never dispatched. S4 tests this boundary
before reusing any F02 branch recovery patch.

## Transport

Use a persistent worker process. For co-location use a Unix socket transport,
possibly a separately routed operation on the local listener; worker messages still
pass worker authentication, epochs, sequencing, message limits and the worker
journal. Kernel UID alone must not turn a worker request into an admin command.

For remote workers use persistent authenticated SSH transport to that process,
with strict host verification, bounded frames, deadlines/backpressure and reconnect.
A connection can carry many exchanges; disconnect does not reset durable identity.
Keep artifact streams separately bounded. Retain the current one-shot SSH adapter
as a temporary S4 fallback during transport equivalence testing, not as the final
process-per-exchange architecture. Do not add a second scheduler on the worker.

The `f02-protocol` credential is distributed as an UpKeeper secret reference;
never embed values in catalogs, manifests, artifacts or diagnosis bundles.
No UpKeeper/dev-fleet implementation is included in Track S.

## Reload and deployment seam

S4 adds validated catalog/policy reload: parse and validate a candidate, commit its
revision/digest, then publish projections. A failed reload leaves the prior version
active. In-flight assignments keep their catalog/package identity; invalidating
changes drain future admission rather than rewriting execution. Epoch and storage
identity changes require explicit lifecycle handling, not an ordinary reload.

Status reports running release/build identity, effective configuration/catalog
digest, worker epoch, last successful reload and snapshot age. UpKeeper owns
comparison, deployment and convergence; the steward only reports what it runs.

## S4 evidence required

One task on each worker, remote lease expiry with a live T3 thread, worker restart
mid-attempt, dropped persistent SSH, local transport authorization, catalog reload
rejection/drain, rotated credential references and configured/observed status parity.
The S5 campaign then measures repeated failures. S0 enrolls no hosts.
