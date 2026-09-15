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
bindings, containment, resource sizes and project execution metadata. Unknown local
bindings fail closed. The projection cannot create credentials or guess a quota binding.

Desired models are authorization, not proof that a provider offers them. Provider
observations and explicit revision-fenced enrollment remain required before dispatch.
Applying a document never enrolls a worker. UpKeeper retains the prior document (or its
absence) with its other fleet projections for transactional restore.
