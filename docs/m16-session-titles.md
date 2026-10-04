# Initial Steward session titles

This bounded M16 unit names sessions before their first turn using frozen,
descriptive execution-package metadata. It does not enable the inert review
authority foundation or implement nested review.

Workers advertise `session-display-v1` through SupportedPackageCapabilities.
For advertised support the coordinator adds optional `display`:
`{"workflowName":"Release","taskName":"Build","reviewJudge":true}`.
The boolean is omitted when false and derives only from the recorded ReviewJudge
flag. No reviewer role is inferred from a task name. Activations carry only
workflowName with empty taskName; supervision is identified by the existing
activation object. Metadata carries no execution/review authority.

Older, unknown, or unavailable ordinary-task worker inventories receive no
display field or display capability, preserving exact legacy strict JSON and
content addressing. Existing mandatory capability gates remain intact.
Supervision still requires its existing mandatory inventory/package capabilities.
Present display requires the capability; capability without display refuses.
Strict nested decoding, canonical name bounds and content addressing also apply.

Coordinator names are sanitized: whitespace/control/Unicode format characters
collapse to spaces, invalid/replacement runes are removed, and names truncate to
64 runes/256 bytes at UTF-8 boundaries. Worker title components are further bounded
to 32 runes/128 bytes, yielding a title below 512 bytes. Empty names use workflow
and task identities. The form is:
`[Steward] project / workflow / task: name / run <12 hex> / starting`.
Recorded judges use `review judge:`; activations use `supervision: activationID`.
The run key is the first six SHA-256 bytes of the full run ID, avoiding common
prefix collisions. It is a short display identifier, not a unique authority key.

Names affect only the initial title in existing CreateAndStartThread input.
Assignment, attempt, thread, dispatch, route and task environment remain unchanged.
Titles are deterministic across retries of the same task/run; package display
participates in the existing authenticated/content-addressed offer.
Preflight and turn-input validation keep their existing ordering before task
provider effects. No extra T3 commands or metadata updates occur.

Dynamic progress, campaign totals, lifecycle updates, manual rename ownership,
safe public reconciliation and live Codex/T3 field proof remain later integration.
The public metadata-update contract lacks an expected-title fence; no internal
generation commands may be used to fill that gap. Local fixture checks do not
establish runtime qualification, source CI, deployment or full M16 acceptance.
