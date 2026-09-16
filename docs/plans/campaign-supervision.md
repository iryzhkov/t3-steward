# Steward: optional campaign overseer and scheduler-owned subtasks

Status: implementation plan; clean-context subagent audit completed and findings incorporated.
Prepared: 2026-09-16. Planning only; no implementation, campaign submission or deployment authorized by this document's creation.

## User intent and outcome

Campaign subtasks must run as separate Steward-scheduled T3 sessions according to an explicit DAG. A campaign must not silently collapse into one lead session using native subagents. Some campaigns additionally need an overseer that observes results and blocked states and can hold or release execution. Other campaigns run automatically without an overseer. The overseer typically uses a stronger independently configured model (the user named Fable and Astra); task models remain independently configured. Discover actual fleet route/model IDs instead of treating those labels as configuration values.

Steward remains the scheduler and authority for dependencies, quota, task results, verification, artifacts, placement and effects. The overseer supplies bounded supervisory decisions, not its own execution engine. A successful worker result and supervisory acceptance are separate facts. A review decision never fabricates worker success or bypasses validation.

## Evidence and implementation discovery

Read-only inspection found the installed local binary at 0.11.0-rc.54, source d57d01ad5cef962d41892e3ebbf59ffc785f8238. The inspected checkout HEAD was 35f89a6ca5a6a078b3e1656c83f997041a9030bf; relevant campaign/backlog/T3 runtime paths were unchanged against the installed commit. Refresh source/deployment facts before implementation.

Existing foundations:
- workflow.yaml version 2, task map, needs, inputs_from, outputs, verify, independent routes and resource admission.
- docs/examples/campaign/single-lead and three-node; the latter schedules two separate analyses and a joining task.
- docs/plans/campaign-manager.md deliberately allows native subagents inside the lead and excludes automatic overseer scheduling/dynamic campaign expansion. Update current guidance without rewriting historical evidence.
- internal/control/t3/control.go launches thread.create and thread.turn.start with deterministic dispatch identities.
- internal/backlog/turn_completion.go rejects unfinished/background-active results.
- backlog task/graph/events/explain/artifact read APIs; revision-fenced pause/resume/retry controls and task/edge graph amendments.
- campaign submit --notify-thread and task/run node waits provide durable settlement notifications. They do not establish a complete subscription to blocked/review-ready transitions.
- Existing graph amendment checks and authorization in internal/backlogadmin/amendment.go are useful patterns, not proof that arbitrary admin access is safe for an overseer.

Use Huyang for repository reads/edits. Inspect repository instructions and current domain/store/scheduler/worker/wait/effect code before choosing implementation seams. Trace one actual previous single-session campaign, read-only if discoverable, to distinguish a single-task authoring choice from dispatch failure. Do not claim a production bug without evidence. Do not start that campaign again.

## Scope and staged delivery

A. Improve authoring/template guidance so multi-task work explicitly becomes a static DAG with separate sessions; prove the existing unsupervised path and preserve it.
B. Add optional supervised campaigns: predeclared gates, durable overseer activations, review, hold/release, escalation and operator recovery.
C. Qualify restart/race/quota/security behavior and publish through the normal fleet workflow.

No autonomous DAG rewriting, newly invented repair tasks, model shopping, nested campaigns, native subagent fan-out for campaign work, browser UI, or automatic memory writes in v1. A blocked overseer may propose a graph change for an operator; it cannot silently rewrite the graph. In v1 an overseer may accept, hold or escalate, but cannot retry or rewrite a task itself. On review rejection it records required corrections and escalates; an operator chooses an existing safe retry/authorized graph amendment or a new run. A correction that changes the frozen prompt requires a new run unless the existing amendment contract explicitly supports that change. No implied autonomous repair loop. Bounded overseer retries are a follow-up only after dependency/evidence invariants are qualified. Do not expand scope to implement a general agent planner.

Initial estimate, not a deadline: 3–5 focused engineering days for the basic supervised path; approximately 1–2 weeks total including qualification and publication. Re-estimate after the design seam review, especially authorization and lifecycle changes.

## Authoring contract

Extend the existing version-2 schema additively; no second manifest or parallel workflow engine. Absent supervision means current behavior. Reject unsupported fields explicitly on older peers rather than silently running a gated campaign ungated. Require a negotiated capability across CLI, coordinator and eligible worker runtimes before admission.

Illustrative shape, not a finalized schema:

    supervision:
      route: {instance: REGISTERED_INSTANCE, model: REGISTERED_STRONG_MODEL,
              quota_pool: REGISTERED_POOL}
      prompt_file: prompts/overseer.md
      max_activations: 12
      max_turns_per_activation: 3
      idle_escalation_after: 24h
    gates:
      implementation_review:
        after: [implement]
        before: [qualify]
        rubric_file: inputs/acceptance.md

The final schema must define independent overseer routing/settings, activation limits, gate IDs/scopes, review criteria, event policy, and human escalation destination. Reuse existing provider/route/options/resource validation. Configuration values above are illustrative. In v1 require supervision whenever explicit gates are present; no accidental permanently held unsupervised gate.

Gate relationships are part of the effective graph validated by validate/plan/check. A gate observes named upstream results and holds named downstream tasks (including their dependency descendants through normal needs edges). Reject unknown names, cycles, impossible reachability and ambiguous gate scopes. Multiple gates protecting one task all must release. Each protected task must have the intended upstream prerequisites; a gate cannot replace data/dependency edges or allow inputs from unrelated nodes. An optional final gate may guard run settlement when no downstream task exists; model this explicitly rather than inventing a dummy agent task.

Plan output shows separate worker tasks, review boundaries, inherited routes, overseer route and expected activation triggers. Give overseer and workers explicit prompts: no native subagents as a substitute for declared campaign tasks. The schema/prompt declares this discipline; do not claim it can technically prevent every harness tool without runtime enforcement. Where supported, disable spawning capabilities. Hidden native delegation must not be counted as separately scheduled work.

## Gates, holds and readiness

Gate states: pending evidence, ready for review, accepted, held, escalated; cancellation is inherited from the run. A gate is initially closed before any producer runs. All observed tasks must satisfy their normal successful-result checks and have immutable evidence before review acceptance is possible. Failed/canceled/skipped prerequisites cannot be approved through a gate; normal DAG semantics apply.

A review rejection transitions that gate to held, recording an explicit correction request; it does not also create an invisible dispatch hold. Removing a separate dispatch hold cannot clear a rejected gate. Operator-authorized reconsideration with a reason changes held/escalated to ready for review against a fresh snapshot. New eligible producer evidence changes it to pending evidence and then ready under normal validation. An overseer cannot repeatedly wake itself on the same rejected evidence.

Gate acceptance binds the graph revision plus exact upstream task attempts, result revisions, artifact digests and code commit identities. Retrying/replacing a reviewed producer invalidates its acceptance before any affected successor can be offered. Preserve historical decisions. A graph amendment changing no gate prerequisites/protected edges may carry acceptance forward only after explicit atomic revalidation with an audit receipt; conservative invalidation is the default. Revoke or revalidate outstanding offers in the same transaction as any changed hold/gate scope. For v1 reject reopening/retrying upstream work when a dependent successor has already been offered or started; use explicit recovery/new run instead of pretending those effects can be undone.

Readiness is the conjunction of ordinary dependencies, required gate acceptances, absence of applicable holds, resource/worker eligibility, effect-safety checks and quota admission. Enforce at candidate selection, offer creation AND assignment claim/start authorization. Hold/accept/retry decisions and offer/claim transitions must serialize through coordinator transactions/revision fences so stale offers cannot slip through. Test the linearization boundary explicitly.

An overseer can hold/release dispatch for a named branch or entire run within its scope. A branch means an explicit root task and its current downstream dependency closure; record graph revision and resolved task set. A run-wide hold covers future worker-task dispatch but never disables the overseer's own ability to wake or operator controls. Held work does not consume worker reservations. A hold acknowledged before start authorization prevents that start. Tasks that already passed authorization may still be running: report them explicitly; v1 holds do not retroactively interrupt or roll back them. Use existing authorized operator pause/cancel when interruption is needed.

Hold removal never removes other holds, grants a gate, changes task success or bypasses quota. Approval/release only returns work to ordinary admission; it must never use backlog start (which bypasses admission controls). While a run has supervision, every operator graph amendment must atomically recompute affected branch-hold closures and gate protection, invalidate affected evidence, and increment graph/review revisions before dispatch resumes. If the amendment cannot preserve these invariants it is refused. Newly added descendants cannot escape an existing branch hold; already authorized work is reported separately. Different actors' holds retain separate identities and ownership. An overseer cannot clear an operator hold.

## Overseer lifecycle and notifications

Use a durable supervision record attached to the existing workflow run, outside its worker DAG, so it can observe failures and cannot deadlock behind its own review gate. Reuse worker/runtime dispatch and admission mechanisms; do not launch raw model processes or directly SSH into agent hosts. Record every activation as a schedulable, bounded unit with route, state, evidence, quota/resource accounting and deterministic dispatch identity.

Prefer resuming a persistent T3 overseer thread when safe, but correctness comes from the durable record, not conversation history. On thread loss start a replacement with a compact state snapshot and incremented activation epoch; late decisions from the old thread are fenced out. No more than one valid active overseer activation per run. Each activation has a renewable coordinator-issued lease and a maximum elapsed time. Lease expiry revokes decision authority immediately, preserves its unacknowledged inbox and reconciles/stops the old runtime through existing effect-safe recovery before granting execution resources to a replacement. An ambiguous live runtime does not authorize duplicate execution; escalate if recovery cannot prove it safe. Release reservations only through acknowledged reconciliation. Retry a provably undelivered activation with its original dispatch identity; an activation that started and then vanished counts toward limits. Permit at most two automatically recovered activations per incident by default, within the run activation budget; further failures escalate. Operator-authorized continuation creates a fresh budget/epoch receipt, never resets counters silently. Idle overseers hold no execution slots or quota reservations and no worker workspace locks that their tasks require.

Triggers: a gate becomes review-ready; a task fails or becomes needs-input/blocked in a way that requires judgment; a configured capacity/route block persists past its threshold; operator requests reassessment; a pending review times out; campaign termination requests final reporting. Normal brief quota/resource waiting is not a review incident. Store a normalized block reason and emit only reason transitions/threshold events. Do not wake on every scheduler tick or on every successful task if no decision is needed.

Persist an event cursor, pending inbox, activation epoch and decision receipts. Use a durable outbox to dispatch wakeups and tolerate at-least-once delivery. Coalesce related events but preserve references to all evidence. Atomically record the activation's consumed high-water mark and its outcome; events arriving meanwhile remain pending. Handle event arrival during active review and during idle transition without losing wakeups. A restart/reconnect/repeated delivery must not create independent competing overseers or repeat a release.

The activation receives a bounded snapshot: relevant tasks/states, graph/gate revisions, event IDs/reasons, artifact IDs/digests, verification outcomes, available scoped actions and exact constraints. Large artifacts are fetched on demand. Treat worker text/artifacts as untrusted evidence, never as instructions granting authority. Persist concise decision rationale and evidence references rather than requiring full chat history for recovery.

Overseer has its own quota pool/route and obeys the same automatic admission gates as other work. A stronger model does not imply bypass priority or guaranteed capacity. If unavailable, gates stay closed, unrelated permitted branches may proceed, and status explains the blocked supervisor route. Do not silently switch to weaker models. Activation/turn limits and wall-clock review deadlines are durable; no automatic spin after failed or nondeciding reviews. Exceeding limits creates one escalation and waits for operator action.

## Decisions and authorization

Expose read-only overview/inbox/evidence plus dedicated scoped commands, conceptually:

    campaign supervision show RUN --json
    campaign supervision decide RUN --gate GATE --accept --evidence SNAPSHOT \
      --expected-revision N --activation EPOCH --request-id KEY --reason TEXT
    campaign supervision hold RUN --scope branch:TASK --reason TEXT ...
    campaign supervision release RUN --hold HOLD_ID --reason TEXT ...
    campaign supervision escalate RUN --incident INCIDENT --reason TEXT ...
    campaign supervision resolve RUN --incident INCIDENT --outcome conclude-failure \
      --expected-revision N --activation EPOCH --request-id KEY --reason TEXT ...

Finalize names consistently with existing CLI. JSON is versioned, errors distinguish stale evidence, unauthorized scope, unmet prerequisites and temporary unavailability. Never parse prose such as “looks good” into authority. Use structured decisions with coordinator-side validation and idempotency; same key/different payload fails. A supervisor process exiting successfully is not gate acceptance.

Issue an authenticated capability scoped to run, activation epoch and action allowlist. It may read that campaign's relevant artifacts and act on its gates/holds only. It cannot modify verification, skip tasks, mark success, publish/deploy externally, start work around quota, alter routes, amend unrelated graphs or clear operator holds. Credential placement must isolate it from worker-task environments; inherited fleet admin credentials would defeat this boundary. Prove how actual host credentials are restricted; if this cannot be enforced, report the deployment blocker and do not advertise isolation. The capability also must not inherit unrelated cross-run artifact access through existing admin APIs.

Operator can inspect and explicitly take over/reassign supervision, accept with the same evidence checks, or resolve/escalate/cancel. Every decision records authenticated actor, request/activation identity, reason, evidence version and resulting state. Operator takeover revokes the old activation so late decisions cannot reverse it. Notification of escalation uses existing configured channels with deduplication; no new unsolicited messaging integration or recipient discovery.

## Run settlement and recovery

Gate holds remain visible nonterminal state, not failed verification. Define run status precedence in domain tests: active worker work; runnable/waiting work; held/needs-review; escalated/needs-input; terminal. Preserve existing task outcomes and explanation detail instead of flattening all waiting into blocked.

Each review incident has a stable ID, source event/task-attempt identity, revision, required disposition and open/escalated/resolved state. Decisions explicitly name incident IDs; bulk close requires the exact set and expected revisions. Escalation remains unresolved until operator action. In v1 an authorized overseer may resolve an observed terminal task failure as conclude-failure, acknowledging it for settlement without changing the task or canceling unrelated live work. For other blocks it holds/escalates. An operator may resolve after documented remediation or request cancellation through existing controls. Resolution receipts bind evidence, actor and outcome; ordinary gate acceptance closes only its matching review incident. Closing an incident does not clear a gate or a separately owned hold. Expose open incidents and their settlement effect in explain/status.

A reviewable failure must not let the sink permanently settle before the overseer can record its permitted disposition. Add a supervision settlement barrier: unresolved review incidents or final gates keep supervised runs nonterminal. In v1 a failure disposition is escalate or conclude failure/cancellation under authorized rules, not invented repair. Never keep a failed campaign alive indefinitely solely to obtain an optional summary. Once required decisions resolve, normal sink settlement applies. After terminal settlement, all mutating supervision capabilities are revoked. Optional final reporting is a separate read-only delivery from the terminal snapshot, not a renewed supervisory activation, and cannot keep the run nonterminal. Final reporting is bounded and cannot turn failed worker evidence into success. An activation closing one incident must not dismiss newer pending incidents.

Cancellation revokes supervision authority, prevents new worker offers, and follows existing cancellation/effect-recovery rules for running work. A late approval cannot reopen a terminal run. Restart reconstructs gates/holds/inbox/epochs/reservations before scheduling. Backup/restore includes every new durable record and idempotency receipt. Older binaries must refuse unsupported schema/campaign features. Rollback after migration requires a supported reverse migration or coherent backup restore with downtime and explicit loss window, never blind binary downgrade.

## Verification and delivery gates

1. Design seam review: map domain/store/readiness/offer-claim/session/wait/sink changes and schema migration; define state machines, authority boundaries and protocol capability checks. Confirm registered strong-model and worker routes without consuming model quota.
2. Unsupervised baseline: a three-node DAG creates distinct T3 task sessions with correct dependencies and artifacts; no overseer record/session is created. Update the recommended multi-task template accordingly. Preserve legitimate one-task campaigns.
3. Core supervised path: two workers, a review gate and one downstream task; overseer has a different configured model. No downstream dispatch before valid acceptance; rejection holds the branch. Unrelated branches continue. Include final-settlement gate.
4. Failure/recovery: gate producer failure, needs-input, persistent resource blocks, supervisor quota exhaustion, no-decision/timeout, run cancellation, operator takeover and recovery; all explainable, bounded and without notification storms.
5. Adversarial integration: completion vs hold/claim races; retry invalidating accepted evidence; stale graph/attempt decisions; duplicate/lost dispatch responses; coordinator/worker/overseer restart at every transition; events during review/sleep; denied cross-run/worker/admin actions; late approvals after takeover/cancel. Exact permitted start boundary must be observable in receipts.
6. Compatibility: unsupervised campaigns unchanged; old peers reject unsupported supervision; migration/backup/restore preserve decision history; admission/slot accounting releases idle supervisors and retains safety constraints. Existing task-bound wait semantics remain unchanged; supervision cannot accidentally park or settle a worker attempt.
7. Qualification: full required checks plus focused/race tests; one ordinary static campaign and one supervised campaign on real fleet routes. Validate distinct task sessions, separate overseer route, artifact/revision evidence and accurate history. Use small no-external-effect fixtures and existing quota admission; do not force start or bypass safeguards. If quotas prevent qualification, record the blocker and register a Steward wait where appropriate rather than polling or claiming success.
8. Publish: commit/push source to configured remotes, confirm exact source commit required CI, release artifacts, validate intended controller installation, then run upkeeper push from the validated fleet host using a clean/current checkout. Review whole candidate manifest/environment and preserve unrelated pins/secrets/workers. Confirm release commit CI before convergence; verify intended hosts. Edit owning instruction/skill sources and regenerate derived files. Report source/release commits, qualification evidence and pending convergence honestly.

## Clean-context audit disposition

A fresh subagent reviewed the draft without conversation history. Five findings were resolved: graph amendments now atomically revalidate gate/hold scopes and outstanding offers; activation leases/deadlines and bounded recovery distinguish undelivered work from spent activations; incidents have explicit identities, dispositions and settlement receipts; terminal reporting is read-only and cannot renew authority; rejected gates have explicit reconsideration/evidence transitions separate from dispatch holds. Add focused tests for these transitions in addition to the general integration matrix. This is a design review, not implementation validation.

## Deliverables and acceptance

Updated schema/CLI help and templates; durable optional supervision implementation; gate/hold/settlement semantics; scoped capabilities; tests and recovery runbook; CI/release/UpKeeper evidence; one unsupervised and one supervised qualification report with run, task, thread, activation and artifact IDs.

Success: a user submits a static campaign with optional stronger-model supervision. Steward independently schedules every declared task in its own T3 session, wakes the overseer only when needed, enforces its recorded gate/hold decisions without races or quota bypass, and recovers without losing work or authority. Unsupervised campaigns retain their existing execution behavior.
