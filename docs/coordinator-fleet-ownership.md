# Coordinator fleet configuration ownership

UpKeeper installs its rendered `steward-coordinator-catalog-input` v1 document
at `~/.config/t3-steward/coordinator-fleet.json` on the named coordinator.
Steward loads this owner-only regular file before validating coordinator configuration.
An invalid document fails configuration loading. Absence retains base configuration.

The document owns the complete worker and project membership, worker CPU class,
executor slots, capabilities, provider/model authorization, and project repository,
default ref, setup profile and eligible workers. Omitted workers, projects or providers
are revoked; an empty model list authorizes no models. Review the complete intent before
applying it. CPU classes are `low`, `medium` and `high`.

Existing operator configuration supplies transport, credentials, epochs, containment,
resource sizes, project execution metadata and the quota pool definitions themselves
(`backlog_v2.quota_pools`: a pool's provider and concurrency). An unknown worker fails
closed: the projection cannot create credentials.

A quota binding is different since the stage 6 release. The document may carry, per
worker, `quota_bindings`: a map from a provider instance to the one pool its work may be
charged to. It is authorization, exactly as the model allowlist is, and never a claim
that the instance is installed, signed in or offering a model. An explicit
`backlog_v2.workers.<w>.providers.<instance>.quota_pool` still wins; the projected
binding is used only where the coordinator's own configuration binds nothing, including
where it has no entry for the instance at all. A binding naming an instance the worker
does not run, or a pool it does not join, is refused when the document is decoded.

The key is new, so the order of a release matters on this one host. Upgrade every
coordinator host to this release before any UpKeeper release authors a `quota_bindings`
entry: an older coordinator refuses the whole projection as an unknown field, so its
reload is rejected and the `steward-fleet-configuration` component fails. UpKeeper v0.1.11
and newer roll a refused projection back when the coordinator answered with a receipt; a
coordinator too old to write one leaves the refused projection on disk, where it will also
stop that coordinator from starting the next time it is restarted. Author the binding in a release that also pins this steward version,
and do not converge it with `upkeeper pull --components steward-fleet-configuration`,
which writes the projection without installing the binary that can read it.

An instance with desired models that neither source binds to a pool this coordinator
defines is dropped for that worker rather than failing the whole configuration: the
coordinator logs one warning at startup naming the instance, the worker and the remedy,
`Config.DroppedFleetProviders()` records it with `missing binding` or `no models`, and
`t3-steward models` reports that as the reason the route is not advertised. A document
rendered before `quota_bindings` existed carries no such key and is applied exactly as it
was, which is what a fleet part-way through a release needs.

A project is different. A fleet project with no `backlog_v2.projects` entry is loaded
with an empty local binding: no credentials, no resource locks, no directory resources,
and the type left to the ordinary defaulting (a Git project). Nothing in the local
binding is required for a plain Git project, and refusing the whole configuration for
one unbound project took every coordinator admin query down once. The coordinator logs
one warning per such project at startup, `Config.DefaultedFleetProjects()` names them,
and both the readiness check (`campaign check`, on each candidate's `unchecked` list) and
`explain` (in the explanation's `details` list) carry `project-binding-defaulted` as an
informational detail for a task on that project. The detail is neither a reason nor a
blocker: it never turns a ready fleet into `accepted_waiting`, never refuses a submission
and never changes a task's eligibility. An explicit `backlog_v2.projects` entry, when present, is preserved exactly
as before and is still the only way to bind credentials, locks or directories.

Desired models are authorization, not proof that a provider offers them. Provider
observations and explicit revision-fenced enrollment remain required before dispatch.
Applying a document never enrolls a worker. UpKeeper retains the prior document (or its
absence) with its other fleet projections for transactional restore.

A changed catalog leaves every affected worker's enrollment stale until it is re-enrolled
on the coordinator host: `t3-steward worker enroll <worker> --current-catalog --reason
TEXT` reads the required digest and the worker's enrollment revision from the
coordinator itself, and `t3-steward worker enroll --all --current-catalog --reason TEXT`
re-enrolls every configured worker whose accepted digest is stale. The fenced form
(`--catalog-revision`, `--expected-revision`) remains for a digest read out of band. Both
forms are operator actions on the coordinator host; the remote-admin role is refused
enrollment.
