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

Existing operator configuration supplies transport, credentials, epochs, provider quota
bindings, containment, resource sizes and project execution metadata. An unknown worker
or provider binding fails closed: the projection cannot create credentials or guess a
quota binding.

A project is different. A fleet project with no `backlog_v2.projects` entry is loaded
with an empty local binding: no credentials, no resource locks, no directory resources,
and the type left to the ordinary defaulting (a Git project). Nothing in the local
binding is required for a plain Git project, and refusing the whole configuration for
one unbound project took every coordinator admin query down once. The coordinator logs
one warning per such project at startup, `Config.DefaultedFleetProjects()` names them,
and the readiness check (`campaign check`) carries `project-binding-defaulted` as an
informational detail on every candidate for a task on that project. The detail is not a
reason: it never turns a ready fleet into `accepted_waiting` and never refuses a
submission. An explicit `backlog_v2.projects` entry, when present, is preserved exactly
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
