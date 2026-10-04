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

At the first negotiated decision, older, unknown, or unavailable ordinary-task worker inventories receive no
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


B1 replay boundary: before either task or activation offer delivery, the coordinator
transactionally allocates one display decision per durable assignment ID/epoch.
V33 adds only coordinator_assignment_displays; the V32 authority foundation remains
unchanged/inert. The allocation validates the current coordinator epoch, the
persisted offered assignment (worker ID/epoch, attempt, dispatch, thread, route and
other immutable fields) and its run/task/attempt revision identity. Lease extension
and update timestamps do not create a new decision. The retained binding includes
coordinator identity but not its restart epoch. An INSERT-on-conflict/read transaction
with an assignment writer lock makes concurrent connections reuse the winner.
Explicit JSON null freezes omission; no process-local cache is authoritative.

Unknown/unavailable optional inventory at the first decision proposes null. Later
recovery cannot add display, and later inventory uncertainty cannot remove frozen
display. A known worker lacking session-display-v1 explicitly refuses a retained
display package. Mandatory preflight, recovery, project-context, supervision,
authorization/revocation and worker strict-wire validation remain current gates;
the frozen metadata never grants capability or authority. Activation mandatory
inventory uncertainty can still refuse before this optional decision is consulted.

A storage/identity/epoch failure withholds the offer before worker delivery; it
does not pretend an unpersisted decision is frozen or send a different fallback.
After a successfully persisted first decision, lost responses and failed claim
persistence replay the same producer package. A coordinator restart changes only
its existing epoch field; the worker's existing comparison permits that field
alone. A new assignment or assignment epoch negotiates independently. Existing
in-flight packages first offered by older producer code cannot be reconstructed
from worker journals by this migration; deploy only with those offers drained or
otherwise resolved. This local repair makes no deployment/upgrade qualification
claim.

The pure BuildActivationOffer helper still renders explicit input values; durable
production delivery goes through CoordinatorOfferBuilder, which freezes/rebuilds
its manifest. Storage fixtures use real SQLite allocation. Full reviewer repro
source is preserved verbatim in docs/m16-b1-review-evidence.md (SHA256
7b28f6cb90af726cfa54d411f4ef029bae26a9a6927ada854a02ba483b28c9e6).
Its second isolated repro deliberately mutates a package and remains a refusal;
positive replay is tested using actual frozen producer offers.
