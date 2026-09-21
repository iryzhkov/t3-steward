# Consultations and immutable project context

Status: C0 contract freeze candidate. This document fixes the contract to review and
implement; it does not claim the C0 evidence gates have passed and does not admit C1
schema work.

Reviewed contract: Jocasta 288638cf804a0571f75daa2df06e0cfd@5. Execution is
authorized separately by Jocasta 8551e795ab59445b807b46c9abd3b391@1.

Source baseline for this reconciliation:
b04bfd9967a158db8622b8c6402e28d8cb6c0898.

## Ownership and scope

The coordinator owns consultation identity, recipient binding, scheduling, deadlines,
terminal outcome, response custody, subscriptions and caller resumption. Workers own
inference effects. Advice grants no gate, hold, incident, scheduler or operator
authority. Existing assignments, journals, task waits, quota admission and capacity
accounting remain authoritative; consultations do not create a second scheduler.

A consultation belongs to its original project, run, task, attempt, assignment, thread
and live turn. Recipient aliases resolve once at workflow submission to complete pinned
bindings. Later project-default changes affect later submissions only. Independent
advisor questions use fresh answering executions and share no previous Q&A. An
overseer activation may receive only an explicitly selected immutable request set;
advancing an inbox watermark does not answer or consume an unselected request.

No declaration means no recipient binding, context pin, request, answering execution,
extra model session or changed scheduling decision. Additive JSON fields are omitted
when absent so equivalent historical package and task digests remain stable.

V1 has one question and one terminal response per request. A respondent cannot ask its
caller a follow-up or initiate a consultation. Nested consultations are refused.

## Authoring contract

Unknown fields and ambiguous bindings fail validation. Model selectors use
`INSTANCE/MODEL` and resolve through the advertised route catalog at submission.

The common form declares the binding named `advisor`:

```yaml
advisor:
  model: INSTANCE/MODEL
```

A campaign may inherit the project definition pinned at submission:

```yaml
advisor: project-default
```

Named specialists use `advisors`. More than one binding requires
`default_advisor`; the value must name `advisor` or a key in `advisors`.
The reserved alias `overseer` is allowed only when the campaign already declares
supervision and means advisory response membership in that run's overseer activation,
not supervisor authority.

```yaml
advisor:
  model: INSTANCE/MODEL
  definition: project-default
  context: CONTEXT_VERSION
  options: {effort: medium}
advisors:
  architecture:
    model: INSTANCE/MODEL
    definition: DEFINITION_VERSION
    context: CONTEXT_VERSION
    instructions_file: prompts/architecture-advisor.md
    capability: bounded-continuation-read-v1
    limits:
      deadline: 15m
      max_turns: 4
default_advisor: advisor
consultation_policy:
  allowed_recipients: [advisor, architecture, overseer]
  max_outstanding: 1
```

Each resolved binding contains:

- alias and role (`advisor` or advisory-only `overseer`);
- exact provider instance, model and normalized options;
- pinned definition version and instruction artifact digest;
- optional exact context version;
- required runtime capability and version;
- effective limits and project/run budget identity.

`definition: project-default` pins the current project definition. Omitting
`definition` creates an inline definition from the binding. An inline definition uses
the source-versioned built-in advisor instruction template unless
`instructions_file` names a campaign file; submission custodies that file and pins
its digest. `options` is the normalized provider option map. `capability` defaults
to `controlled-response-v1`; selecting tools or more than one turn requires
`bounded-continuation-read-v1`. Project and task policy may only narrow their parent policy. Campaign deadlines may
increase from the default up to the operator's 24-hour maximum. Other campaign limits
may only lower the defaults below; operator-only synthetic qualification overrides
are explicitly recorded and never alter production defaults.
`context` always names an exact immutable version; there is no floating latest value.

`consultation_policy.allowed_recipients` is optional. When omitted it contains every
binding declared by that campaign. A task may narrow the submitted set and declare
publication slots with the same field name under its task entry:

```yaml
tasks:
  design:
    prompt_file: prompts/design.md
    consultation_policy:
      allowed_recipients: [advisor, architecture]
      publish_context_outputs: [design_context]
```

The task policy cannot add recipients or grant operator/supervisor credentials.
`publish_context_outputs` names typed outputs declared by that task; an undeclared
name fails validation. These field names and locations are candidates for C1 encoding, pending the exact
C0 admission review.

`campaign validate`, `plan` and `check` display each effective alias, route,
definition/context versions, capability, limits, quota/capacity blockers and whether
the binding uses an overseer. Plan output never implies provider installation or live
readiness.

## CLI and response contract

The ordinary caller needs no discovery:

```sh
t3-steward task ask -- "QUESTION"
```

This selects `default_advisor`, or the sole binding. Explicit aliases use
`task ask ALIAS -- "QUESTION"`. The exact advanced commands are:

```sh
t3-steward task ask overseer --question-file question.md --json
t3-steward task ask context:VERSION --question-file question.md --async --json
t3-steward task answer show REQUEST --json
t3-steward task await-answer REQUEST --json
t3-steward task answer submit REQUEST --answer-file answer.md --outcome answered --json
t3-steward context publish --manifest context.yaml --request-id KEY --json
t3-steward context show VERSION --json
```

`context:VERSION` is shorthand only for a unique predeclared, task-allowed advisor
binding pinned to that exact context version. It cannot introduce a route, recipient
or new context pin. Zero or multiple matching bindings are refused with the explicit
allowed alias alternative.

There is no resident MCP surface in V1. Task, run, route, revision, authority and
delivery identity are derived from the live execution. Each ask carries an idempotency
key, recipient, self-contained question, custodied attachment references and deadline.
The CLI's default key derives from canonical caller attempt, logical live turn and
semantic payload digest; it is printed in the receipt. An explicit key permits an
intentional repeated question. Replay comparison covers every semantic field and never
uses transient transport sequence.

The public response outcomes are `answered`, `insufficient-context`, `declined`,
`failed`, `timed-out` and `cancelled`. A response contains request ID, question
digest, respondent identity and epoch, definition/context or overseer snapshot,
custodied answer digest, compact answer, missing references, source references,
omissions and staleness. Empty content is not `answered`.
`insufficient-context` is a successful advisory outcome, not a transport failure.
Advice remains unverified evidence and cannot authorize a campaign mutation.

Errors distinguish permanent refusal, accepted-but-waiting and recovery-required.
Permanent reasons include unauthorized or unknown recipient, unsupported capability,
invalid context, changed replay and size/count limits. Quota, capacity and worker
unavailability remain waiting states until deadline. Ambiguous external effect is
recovery-required.

## Defaults and hard limits

Defaults are conservative and may be lowered by project/campaign policy. No layer may
raise a value beyond the operator maximum or backend capacity.

| Limit | Default | Maximum |
| --- | ---: | ---: |
| question UTF-8 bytes | 16 KiB | 16 KiB |
| answer artifact UTF-8 bytes | 16 KiB | 16 KiB |
| attachments per question | 16 | 16 |
| aggregate question attachments | 1 MiB | 1 MiB |
| retained context bundle | 2 MiB | 2 MiB |
| outstanding requests per task | 1 | 1 |
| nonterminal requests per run | 32 | 32 |
| total requests per run | 64 | 64 |
| concurrent answer executions per run | 4 | 4 |
| end-to-end request deadline | 15 minutes | 24 hours |
| respondent turns, controlled-response-v1 | 1 | 1 |
| respondent turns, bounded-continuation-read-v1 | 4 | 4 |
| overseer-selected consultation IDs per activation | 4 | 4 |
| strict input budget | 32,000 tokens | 32,000 tokens |
| strict output budget | 4,000 tokens | 4,000 tokens |
| concurrent answer executions per project | 4 | 4 |
| nonterminal requests per project | 64 | 64 |
| retained context versions per project | 32 | 32 |
| retained context bytes per project | 64 MiB | 64 MiB |
| unpinned retired-version grace | 30 days | operator may increase |

The project concurrency default matches one run's four-execution maximum while
per-run fair shares prevent monopolization. The 64 pending-request cap admits two
full per-run queues; the 32-version / 64 MiB storage defaults bound a project at
32 maximum-sized bundles. The 30-day unpinned retention grace provides a recovery
window without pinning producer runs. These are conservative lead-review choices,
not measured capacity claims; operator policy may configure the project caps before
submission, subject to route/resource ceilings and existing pins. Aggregate admitted
usage is additionally bounded by the sum of per-run turn/input/output allowances and
normal quota ownership; no project gets an unmetered inference allowance.

Project limits use per-run fair shares. Pinned versions survive the retired-version
grace; the grace starts only after the last workflow/request pin is gone. Normal route
quota always applies. Full storage refuses publication rather than evicting a pinned
context. A byte limit is not a token-fit proof.

## Runtime capabilities and context boundary

C0 freezes two separate capability names.

### `controlled-response-v1`

This is one primary provider submission, one turn and no tools, continuations, access
programs, side inference or ambient project reads. It enforces a bounded serialized
client request and output-token request field. It is suitable only when the complete
advisor input is materialized into one immutable package.

Its claim is **client-envelope bounded**. Provider-added hidden input is not observed
by the current adapter. Until an authenticated backend contract or attestation covers
that input and actual cap compliance, this capability must not be described as a
complete model-visible-context bound. A successful narrow two-call probe qualifies
this capability only; it does not qualify overseer execution or the strict capability.

### `bounded-continuation-read-v1`

This future capability permits allowlisted custodied reads and up to four turns. It
must provide:

- deterministic cursors bound to project/run, revision, query and authority;
- fixed page item and serialized-byte maxima with stable ordering;
- explicit end, stale, unauthorized and context-too-large outcomes;
- bounded preview plus byte count, digest and artifact reference for large results;
- cumulative accounting of instructions, schemas, envelope, transcript, pages, tool
  calls/results, reserved output and framing margin;
- rejection before every continuation that would exceed any byte, token, tool-output,
  turn, deadline or backend-capacity limit.

Tool output is bounded before entering context. Paging consumes the same cumulative
budget; compaction does not reset it. Exact tokenizer accounting is used where
supported, otherwise a qualified conservative upper bound. Request assembly and
runtime transcript admission are separate checks. Unsupported adapters refuse before
thread start. Prompt guidance and full-access agent obedience are not enforcement.

The worker package requests the exact capability/version. A missing, stale or
mismatched worker or route fails closed before dispatch. Mixed-version workers remain
eligible for ordinary work.

## Authorization matrix

| Actor | Allowed | Explicitly denied |
| --- | --- | --- |
| Task caller | Ask manifest-allowed aliases; inspect, await and cancel its own requests; publish only a declared output | Other task/run requests, recipient expansion, answer submission, supervision/operator mutation |
| Independent advisor | Read exact packaged inputs and submit one response for its bound request/epoch | Caller, supervisor, scheduler, context-publication or general artifact authority |
| Advisory overseer member | Read only selected request references/pages and answer selected requests at current run/activation epoch | Answer unselected IDs; advance membership by watermark; use advice as a gate decision |
| Normal overseer | Existing revision/epoch-fenced supervision reads and mutations | Consultation authority not explicitly selected; unbounded campaign/transcript import |
| Operator | Existing admin-scope inspect/cancel/recovery; publish/retire context | Bypass replay, custody, deadline or containment fences |
| Coordinator | Bind authority, validate custody/deadline/replay, commit terminal outcome and delivery intent | Model inference |

Environment and task identifiers are discovery only. Consultation calls also require a
purpose-separated execution capability plus authoritative live
attempt/assignment/thread/turn checks. The caller capability allows ask, own inspect,
await and cancel for immutable allowed aliases. The answer capability allows one bound
request/epoch response. Advisor packages receive no supervisor or operator credential.

Existing deterministic dispatch tokens are effect identities, not authentication
secrets. Use a cryptographically random capability or purpose-separated MAC under a
coordinator secret, store only its digest, compare in constant time and check the
current authoritative binding transactionally. Redelivery recovers the same capability.
Assignment replacement, retry, terminality and expiry revoke applicability. Same-UID
full-access processes are not hostile-process isolation; this is application authority.

## Custody, state and transactions

Question and answer bodies are bounded content-addressed coordinator artifacts.
Metadata names digest, size, media/role, provenance and retention owners. Inline answer
delivery is a verified presentation copy, not another authoritative answer.

The consultation state is:

```text
accepted -> queued -> executing -> terminal outcome
    |          |          |
    +----------+----------+-> cancelled / timed-out
```

Containment and delivery are independent. A timed-out request may still expose an
uncontained execution. Terminal outcomes are immutable.

| Operation | Atomic commit and recovery contract |
| --- | --- |
| Ask | Validate live caller/capability, pinned binding, quotas, custody and canonical replay; insert request plus stable dispatch intent. Default ask also checks exclusive wait compatibility and parks atomically. Refusal parks nothing. |
| Async ask | Same acceptance without subscription or promised automatic wake. |
| Dispatch | Use existing assignment/lease commit and journal before external effect. Ambiguous create/start reconciles the same dispatch/external identity; it never creates semantic retry work. |
| Await | Return terminal result or bind one subscription to the current live turn in one transaction. Answer-before-await returns immediately. |
| Respond | Verify custodied bytes, bound execution epoch, nonterminal request and coordinator time strictly before deadline; commit one terminal response and attached wait settlement or durable settlement intent. Identical accepted replay succeeds after deadline; changed/second response fails. |
| Cancel/timeout | Revoke response authority and settle subscription. Stop an exclusively owned execution or detach one shared activation member. Hold resources until stop/containment evidence. |
| Early resume/retry/completion/amend | Detach or revoke subscription with authority change. Never inject into a busy turn, wake a retired attempt or reopen a final sink. |
| Wake | Reacquire normal capacity/admission and commit stable delivery intent while preserving run/task/attempt/thread identity and retained workspace/locks. Unknown delivery remains recovery-required. |
| Context publish | Verify staged bytes and successful producer provenance before atomically publishing an immutable version. Replay returns the original version; failure exposes no partial version. |

Default ask is logically synchronous through durable parking, not a blocked CLI
process. Await is exclusive within one parking episode. A live unrelated wait causes a
pre-parking refusal naming `--async`; registering another external wait while a
consultation owns parking is likewise refused. Early operator/recovery resume detaches
the subscription, preserves the answer for explicit read/re-await, and never injects
it into the live turn.

Selected unanswered requests fail explicitly when their answer execution ends.
Unselected requests remain queued. Overseer membership is an immutable set of request
IDs committed with request-to-execution membership and separate from the event
high-water mark. Cancellation removes only that member's response authority and does
not cancel surviving review/finalization obligations. New requests wait for a later
activation.

Caller terminality cancels nonterminal requests. Sink settlement waits for answer
execution effects to be contained, but an idle context version does not block it.
Clone/rerun copies policy and pinned inputs, never request, answer, wait, acceptance or
capability identities.

## Overseer materialization and scheduling

A fresh overseer activation receives a bounded compact campaign projection, current
required decisions/incidents, and at most four deterministically selected consultation
IDs. Aggregate assembly admission includes the fixed envelope, schemas, selected
events, consultation material and reserved output. It never automatically imports full
worker transcripts, all historical Q&A, every event or an unbounded ID list.

Large campaign/evidence data uses the bounded read capability. Omitted rows remain
durable and unconsumed. An item that cannot fit alone receives an explicit
context-too-large outcome and cannot head-of-line block smaller eligible requests.
Campaign decisions remain structured facts with provenance; a summary carries a
source revision/watermark and is never gate authority.

Fair scheduling serves the oldest eligible request within per-run and per-project
shares, subject to normal route quota and capacity. Parked callers release only the
existing executor slot; workspace and locks remain held. Wake reacquires capacity
normally. No default headroom is reserved. Overseer consultation work has a separate
allowance within the existing total activation budget; existing required finalization
and gate/incident review take priority over advisory requests in deterministic
oldest-first order. Reserve only the budget of a demonstrated existing finalization
contract. The unused final-report trigger does not establish a separate guaranteed
post-settlement model report or justify adding an activation to ordinary campaigns.

## Implementation seams and ownership

C1 owns separate durable request, dispatch-intent and subscription identities, plus
purpose-scoped capability digest registration, validation and revocation. C2 owns
worker-side private capability emission/recovery and the actual answering assignment.
Every mutation commits a stable native audit event; exact replay returns its existing
receipt without duplicating events. Subscription state links to the existing task-wait
owner rather than copying its scheduling or delivery logic.

`CoordinatorArtifactStore.Publish` stages and hashes bytes before metadata publication.
`PruneArtifacts` currently protects whole runs with `coordinator_retention_pins`, and
`ArtifactStoragePathReferenced` sees only `coordinator_artifacts`. Project contexts need
independent project-owned references; both prune and failed-publication cleanup must
consult them. Retaining an entire producer run is not the context-lifetime solution.
`backupsnapshot.Manager.Create` already captures database metadata and artifact bytes
under the stopped-database lock; restore and shared-blob retirement races still need
qualification. Production custody is required for the vertical slice, not replaced by
a test adapter.

## Immutable context custody

Context publication creates project-owned immutable metadata and independent retention
references over content-addressed bytes. It binds project, schema, pinned source
commits, explicit instruction/content roles, producer run/task/attempt/result revision,
artifact hashes, summary provenance and consumption policy.

Publication requested by a task remains staged until successful verified producer
completion. Operator import is a distinct audited operation. Failed/unverified
producers cannot publish. Same-campaign consumers declare an ordinary dependency and a
typed output reference; cross-run consumers name an exact same-project version.
Later/dependent-task references fail DAG validation.

Submitted workflows and live requests pin exact versions. Retirement blocks new
references and preserves existing pins. Deletion waits for every workflow/request and
retention owner. Backup includes metadata and bytes. Producer archival does not break a
published context. Historical answers never become project memory unless an operator
publishes reviewed content as a new version.

## Compatibility and rollout matrix

| Coordinator | Worker | Campaign declaration | Result |
| --- | --- | --- | --- |
| old | old/new | absent | Existing behavior |
| new, feature disabled | old/new | absent | Existing behavior; migrations only |
| new, feature disabled | any | present | Validate/submit refusal |
| new, enabled | old or capability absent | absent | Ordinary work remains eligible |
| new, enabled | old or capability absent | present | Precise unsupported-capability refusal before dispatch |
| new, enabled | matching capability | present | Bounded package may dispatch |
| new, enabled | mismatched capability version | present | Fail closed before dispatch |
| new then rollback | mixed | pending requests | Disable new asks; continue response/wake/containment reconciliation until drain |

Upgrade readers/coordinator/workers before enabling authoring. Unknown fields fail
closed. Downgrade requires drain/cancel plus containment and an explicit DB
compatibility check; an old binary must not open unknown live records.

## Numeric non-regression budgets

Capture at least five identical samples for median-only checks and at least thirty for
any p95 check, both before semantic edits and on the branch under the same fixed fixture
and host load. These are lead-review candidates and may be tightened before C1;
loosening requires a recorded C0 decision before comparison.

- Feature absent: no additional sessions, assignments, waits, artifacts or external
  effect intents; state-transition and receipt diff is exact after declared
  ID/timestamp normalization.
- No-feature ready-work scheduling throughput: median regression at most 2%.
- Coordinator reconciliation p95 wall time: regression at most 5% and at most 25 ms
  absolute per pass.
- SQLite statements and transaction count for an ordinary no-feature scheduling pass:
  no increase. Median SQLite execution time regression at most 5%.
- Idle coordinator memory: at most 8 MiB or 2%, whichever is smaller.
- Mixed load: every continuously eligible ordinary task, consultation answer, caller
  wake and required overseer finalization is offered within 10 scheduling passes of
  becoming the oldest eligible member of its fair share, unless quota/capacity is
  explicitly closed.
- One-slot worker: no overcommit; parking and wake preserve the existing slot and lock
  accounting exactly.

Investigate a statistically meaningful breach; do not relabel it feature cost.
Repository tests, race/vet/lint/build and the section-16 behavioral matrix remain
release gates.

## Evidence still required before C0 admission

This contract freeze records reviewable documentation choices only. C0 requires
source-backed feasibility spikes, a concrete happy/failure path, compatibility and
baseline receipts, not production implementation of C1–C5 before C1. The following
obligations remain open. For C0, each needs an accepted feasibility receipt and a
concrete integration contract; full production and flood/race qualification belongs
to its owning C1–C5 exit gate and remains required before release:

1. Live caller capability issuance, private worker custody, registration,
   redelivery/recovery, expiry/revocation and every wait/wake authority path.
2. Capacity/fairness proof at authoritative offer, claim and wake boundaries under
   simultaneous callers, shared routes, closed quota and one-slot workers.
3. `controlled-response-v1` immutable artifact/runtime identity, effective launch
   configuration, fresh thread-to-client-state mapping and authenticated two-call
   receipts. The result remains client-envelope-only because backend-hidden input is
   not attested.
4. A real `bounded-continuation-read-v1` adapter enforcing page/tool/transcript/token
   budgets before every continuation. It is not implemented or qualified.
5. Overseer bounded package construction, maximum-four selection, durable omission,
   oversized-item behavior, finalization priority and 100/1,000-request flood receipts.
6. Production question/answer/context custody and retention/backup/retirement races.
7. Cold-call UX receipt: one ordinary ask, no model/worker/run/credential discovery,
   automatic wake with compact answer, and precise no-advisor/wrong-alias help.
8. Before-feature deterministic regression fixtures and measured numeric baselines
   against the budgets above.
9. Mixed-version refusal and rollback/drain qualification.
10. Exact C0 review of the final ADR with no open safety decision.

No C0 pass, consultation implementation, live runtime qualification, deployment,
utility result or benefit claim follows from this document alone.
