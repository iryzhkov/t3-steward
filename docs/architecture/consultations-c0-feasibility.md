# C0 consultation feasibility audit

Baseline: `f9f81b18195aa05c7cb186c754bf09af8252b76a`  
Plan: `jocasta:288638cf804a0571f75daa2df06e0cfd@5`, especially sections 4–8 and 15.

This audit is intentionally pre-schema. It records hypotheses, acceptance criteria, source evidence, capability gaps, and the smallest seams needed before consultation contracts are frozen.

## Findings at a glance

| Area | Feasibility result | Contract implication |
| --- | --- | --- |
| Task caller authority | Existing transaction fences prove a named attempt/thread is live, but the caller presents forgeable identifiers through a same-UID admin transport. This is not a task-scoped authenticated principal. | Add an execution-scoped, secret capability bound to attempt, assignment epoch, thread, allowed consultation operations, and expiry; validate it before the store transaction. Keep the existing attempt/thread/revision fences. |
| Exclusive consultation parking | Feasible by specializing the existing task-wait transaction. Existing waits support WakeEach/WakeAll groups, so exclusivity is not an existing invariant. | Consultation ask-and-await must check for any live wait for the attempt and insert the request plus exclusive subscription plus park transition atomically. Ordinary wait registration must reciprocally refuse while that subscription is live. |
| Wake and delivery paths | Existing task waits have fenced, durable park, settlement, wake, delivery, recovery-required, cancellation, terminal sweep, and operator rewake paths. | Reuse their resumption and delivery machinery. Do not encode the answer as a shell condition or reuse WakeEach/WakeAll group semantics. |
| Parked capacity | Provider and planner-derived executor occupancy are released while assignment/workspace ownership remains. Wake immediately changes the attempt to resuming. **Executor capacity is not transactionally fenced at offer, claim, or wake in the SQLite store.** | No mandatory reserved headroom, but add one authoritative shared capacity fence for new offers/claims and retained resumptions before consultations can qualify. Then add fair priority and one-slot contention tests. |
| Fresh advisor dispatch | Feasible as a new execution-package kind through the existing offer/claim/worker journal/T3 effect path. Supervision activation already demonstrates a fresh scratch thread without repository preparation. | Add a consultation execution kind/capability and an immutable per-request package. Never resume a prior advisor thread. Response commit remains a coordinator operation bound to request/execution epoch. |
| Hard context bounds | **Not supported by the current Steward-to-T3 adapter.** It submits prompt/model/runtime fields and later exports the completed thread. It cannot set or observe an input ceiling, intercept continuations, meter accumulated tool results, or reject before a continuation. | Strict-context consultation mode must remain unqualified until the execution backend exposes enforceable input/transcript/tool budgets with measured usage. A byte-bounded, fresh-thread trusted-agent pilot must be named as weaker and cannot claim the section 15 guarantee. |

## 1. Task-scoped authenticated caller identity

**Hypothesis.** The existing task identity and coordinator transaction fence are sufficient to authorize `task ask`.

**Acceptance criterion.** A caller must prove it is the currently authorized execution for one attempt/assignment/thread, and the proof must be unusable by another same-UID task workspace. The coordinator must independently verify the live attempt, assignment epoch, thread, allowed recipient, and operation.

**Evidence.**

- `cmd/t3-steward/wait_task.go:50-66` resolves six identifiers from environment or `.t3-steward/task.env`: workflow, task, attempt, issued revision, assignment, and thread.
- `cmd/t3-steward/wait_task.go:90-130` requires the fallback file to be a private, owned regular file and rejects symlinks. This protects accidental or cross-user replacement, not copying by another process running as the same user.
- `internal/control/t3/control.go:459-465` explicitly says the environment field is unverified by tested T3 releases; the workspace record is the authoritative discovery fallback.
- `internal/store/sqlite/task_wait.go:77-225` validates authoritative run/task ownership, a live turn, matching thread, non-future issued revision, and a compare-and-set revision transition. It does not authenticate possession of the assignment; `AssignmentID` is not present in `TaskWaitRegistration`.
- The remote coordinator call uses the worker/admin transport identity. The operation body supplies task identifiers. Thus authentication answers “which configured client reached the coordinator,” while the store fences answer “does this named attempt exist and remain live”; neither proves the request came from that attempt’s contained execution.

**Result.** The hypothesis fails for honest task-scoped authentication. The existing fences are valuable and must remain, but identifiers alone are ambient same-UID authority.

**Minimum seam.**

1. Mint a random execution capability when a worker accepts a claimed assignment. Store only its digest with assignment ID/epoch, attempt ID, thread ID, allowed operations/recipient aliases, and expiry.
2. Deliver the secret only to that execution (prefer a protected descriptor or execution-local credential file; environment remains an optimization only if T3 eventually verifies containment). The existing `DispatchToken` is not usable as this secret: `internal/backlog/coordinator.go` derives it deterministically from assignment identity with `stableCoordinatorID("dispatch", assignmentID)`, so it provides replay identity rather than entropy.
3. Present it on `ask`, `await-answer`, own-request inspect, and cancel. Validate the digest and binding before entering the request transaction, then recheck live authoritative state inside the transaction.
4. Rotate/revoke on retry, assignment epoch change, cancellation, thread replacement, and terminality. Never place the capability in durable task identity records, logs, receipts, or question payloads.
5. Document that full-access same-UID processes are not OS-isolated. This capability narrows application authority and prevents accidental/cross-workspace use; it cannot defeat a hostile process able to read another process’s files or memory.

A narrower alternative is a worker-local authenticated proxy that derives the execution from its retained assignment journal and forwards a signed assertion to the coordinator. It is viable only if the invoking process can be bound to the retained execution rather than merely naming it.

## 2. Current waits, wake paths, and exclusive parking

**Hypothesis.** Consultation await can reuse task waits without changing ordinary wait algebra.

**Acceptance criterion.** Ask-and-await atomically accepts the request, binds exactly the current live turn, and parks it only when no unrelated wait is live. Every ordinary wait registration must refuse while consultation owns the park. Answer-before-await, concurrent answer/await, cancellation, timeout, operator forced resumption, terminality, uncertain delivery, restart, and duplicate delivery must each have one explicit outcome.

**Evidence.**

- `Store.RegisterTaskWait` atomically stores the wait and moves the attempt to `waiting-external/waiting-external` under a turn/revision fence.
- Existing waits intentionally allow multiple waits per attempt using WakeEach and WakeAll. `refuseMixedTaskWaitSetTx` only rejects mixed local/coordinator kinds in an all-set. Therefore consultation exclusivity cannot be inferred from current wait registration.
- `Store.SettleTaskWait`, `ExpireTaskWaits`, and `CancelTaskWait` settle content/state; `Store.WakeTaskWaits` groups settled members, resumes a parked live attempt, or records an undelivered/abandoned outcome after terminality.
- `Store.WakeTaskWaits` handles a later WakeEach member after an earlier member already resumed the attempt by generating delivery to the live turn without changing state. That behavior is specifically unsuitable for exclusive consultation delivery into a possibly busy turn.
- `Store.TransitionTaskWake` claims a delivery group atomically and supports pending/held/sending/delivered/recovery-required/abandoned transitions.
- Admin cancellation settles task waits (`internal/backlogadmin/execution.go`), terminal sweep cancels stranded live waits, and `backlog rewake` is the explicit recovery path after a wait was cancelled or settled without reaching the attempt.
- Existing tests cover answer-equivalent races and stale observations: `task_wait_each_park_test.go`, `task_wait_group_recovery_test.go`, `task_wait_terminal_sweep_test.go`, `task_wait_stale_observation_test.go`, `backlogadmin/rewake_test.go`.

**Result.** Reuse is feasible at the transition/delivery level, but a consultation subscription needs its own row/kind and reciprocal exclusivity checks. Modeling it as another WakeEach/WakeAll member would violate the reviewed contract.

**Minimum seam.**

- One store transaction: authenticate caller, check request idempotency, check no live wait/subscription for the attempt, insert consultation request and exclusive subscription, transition attempt to parked, and persist dispatch intent.
- Ordinary `RegisterTaskWait` checks the exclusive-subscription table in the same transaction and returns the async alternative.
- `await-answer` checks terminal response first; otherwise it performs the same exclusivity and live-turn binding transaction.
- Any operator/recovery transition from parked to resuming detaches the subscription in the same transaction. The answer remains readable but cannot be injected into the active turn.
- Answer commit stores the immutable response and either settles the attached subscription or records a durable settlement intent. Delivery uses the existing wake outbox/state machine and contains compact answer plus provenance.
- Consultation cancellation revokes only request/response authority and settles its subscription. Run/task terminal transitions revoke all owned live requests before sink settlement.

Required race matrix: answer before await; answer concurrent with await; ordinary wait concurrent with await; operator rewake concurrent with answer; cancellation concurrent with response; timeout sweep concurrent with response (commit-time deadline wins); caller terminal concurrent with delivery; delivery reply lost; coordinator restart in every delivery state.

## 3. Parked capacity release and reacquisition

**Hypothesis.** Existing parking releases scarce capacity and existing wake paths safely reacquire it.

**Acceptance criterion.** A parked caller holds neither provider concurrency nor executor capacity, retains assignment/workspace/locks, and cannot resume past quota or executor limits. Under contention, queued answers and resumed callers receive bounded fair service without starving ordinary work.

**Evidence.**

- `domain.ControlState.HoldsProviderSlot` counts preparing, running, draining, and resuming, but not waiting-external.
- `capacityOwners` derives executor reservations from durable assignments and explicitly omits a waiting-external attempt. `task_wait_lifecycle_test.go` proves the parked attempt retains `AssignmentID` while releasing executor capacity and provider occupancy.
- `Store.WakeTaskWaits` moves the attempt to active/resuming and states that provider/executor capacity is reacquired through ordinary paths.
- Provider quota reconstruction counts resuming as active. This prevents a wake from bypassing quota concurrency.
- The general planner has a `NewCapacityConstraint` based on executor pools and durable capacity owners. Placement rejects workers with no free slot, but this is a planning snapshot rather than the authoritative concurrent commit boundary.
- `internal/store/sqlite/fleet.go:CommitAssignmentPlan` and `ClaimAssignment` contain no `ExecutorSlots`/capacity predicate. Concurrent plans or claims can therefore pass against the same stale free-slot view.
- A resumed caller is not a new assignment: it retains the claimed assignment, and worker command reconciliation emits dispatch/resume behavior for `ControlResuming`. This makes its executor reacquisition distinct from normal new-offer placement.
- The exact wake commit is `Store.WakeTaskWaits`: in the same transaction it changes `waiting-external/waiting-external` to `active/resuming` and marks the wait group woken. That transaction checks execution-abandonment and attempt revision, but reads no executor pool/reservation state. Because `ControlResuming` immediately holds a provider slot while `capacityOwners` now counts the attempt as executor occupancy, the derived accounting can report a reservation after the wake without having arbitrated it against a concurrent offer/claim.
- No fair arbitration policy was found between retained resumption, newly offered work, and consultation-answer assignments.

**Result.** Release is source-supported. Provider accounting on resumption is source-supported. Authoritative executor enforcement is absent at offer, claim, and wake, so safe reacquisition is not currently established. This is a C0 prerequisite, not merely a qualification gap.

**Minimum seam and tests.**

- Keep waiting-external excluded from capacity ownership and keep workspace/locks attached.
- Add one store-owned capacity reservation/fence shared by `CommitAssignmentPlan`, `ClaimAssignment`, and the `WakeTaskWaits` transition to resuming. Its transaction input must identify the same worker pool and demand used by planning, fence the worker/catalog epoch, and make concurrent contenders produce at most the configured reservations. Offered assignments must retain a reservation or claim must atomically reacquire it; choose one contract and apply it consistently so offer-to-claim does not double count or create a gap.
- A wake that cannot reserve capacity must leave the attempt parked with its settled outcome pending delivery/resumption, or enter an explicit capacity-wait state that does not hold provider/executor occupancy. It must not mark the attempt resuming first and hope the worker refuses later.
- Route fresh answer work through the same authoritative fence.
- Introduce a deterministic runnable ordering class within the existing planner for: required finalization/gates, resumptions with settled outcomes, consultation answers, and ordinary work. Freeze exact priority only after mixed-load measurements; use aging/fair per-run shares rather than permanent absolute priority.
- Test one-slot sequences: caller parks → answer runs → caller resumes; ordinary task occupies slot when answer queues; answer occupies slot when caller wake commits; two projects share advisor route; closed quota; worker disconnect/restart; stale parked observation; cancellation at each boundary.
- Assert no simultaneous running executions above allocatable slots, no quota bypass, retained workspace/lock identity, eventual service under the defined fairness bound, and explicit timeout when service is unavailable.

## 4. Fresh non-overseer answer dispatch

**Hypothesis.** Independent advisor answers can use the current scheduler and worker without a second service.

**Acceptance criterion.** Each accepted question produces at most one deterministic execution identity, is admitted through normal quota/capacity, starts in fresh scratch state from immutable packaged inputs, and can submit only one response for its request/epoch. No prior advisor transcript is resumed or appended.

**Evidence.**

- Supervision activations already travel through the ordinary assignment lifecycle and `ExecutionPackage`, while `workerruntime.prepareActivation` creates a fresh empty workspace with no repository lock.
- `createActivationThread` creates a deterministic T3 thread on the selected route; collection exports the completed transcript and publishes ordinary result custody. This is an existence proof for a fresh non-task execution kind on the existing scheduler/worker path.
- Supervision is the wrong semantic type: it carries overseer credentials/actions and coordinator decision authority. Advisor identity must be separate.
- `CreateAndStartThread` accepts a caller-provided deterministic thread ID and dispatch token, giving persist-before-effect recovery a stable external identity.
- The worker currently learns success/output after thread termination. An advisor response can therefore be collected as a bounded result artifact or submitted during execution through a scoped coordinator operation; either path must bind request ID, execution epoch, question digest, and deadline.

**Result.** Backend scheduling is feasible without another scheduler. The minimum implementation is a new execution-package discriminator and capability, not a reuse of supervisor authority.

**Recommended integration contract.**

- Add `ExecutionKindConsultationAnswer` and worker capability `consultation-answer-v1`.
- Package contains immutable request ID/digest, recipient definition/context versions, selected evidence manifest, route, deadline, byte/token budgets, deterministic execution/dispatch identity, and a response capability scoped to exactly one request epoch.
- The package gets a scratch workspace and no source checkout, caller locks, supervisor credential, or task capability. Optional reads are only packaged/custodied objects.
- Worker creates a brand-new T3 thread per request. Reconciliation reuses the same deterministic identity after ambiguous create/start; semantic retry is absent.
- Coordinator response commit validates custody, response capability, request/execution epoch, nonterminal state, and coordinator time strictly before deadline. An identical already-accepted replay succeeds after the deadline; any new/changed response does not.
- Cancellation stops the execution only when it is exclusively owned. The later shared-overseer batch mode tracks per-request membership separately and does not reuse this exclusive-stop rule.

## 5. Hard adapter input, tool, and transcript budgets

**Hypothesis.** Current T3 control can enforce section 15’s 32k input / 4k output target and cumulative within-execution tool/transcript ceiling.

**Acceptance criterion.** Before initial dispatch and every model continuation, a controlled adapter computes or conservatively bounds all model-visible input (system/instructions/schema/question/attachments/history/tool results), refuses a continuation that would cross the configured ceiling, bounds each tool result before materialization while returning a pageable reference, caps output, reports measured peak usage, and never relies on provider compaction or agent obedience.

**Evidence.**

- `internal/control/t3/control.go:447-465` exposes only identity/project/title/model selection/runtime/interaction/worktree/prompt/environment.
- `CreateAndStartThread` serializes `thread.create` plus `thread.turn.start` with prompt, attachments (always empty), model selection, and runtime modes. It sends no context window, input token, output token, transcript, tool-result, or continuation budget.
- Worker code can observe terminal state, final assistant message, exported transcript, and provider usage logs after execution. Those are retrospective measurements and cannot reject a pre-continuation breach.
- `ExecutionLimits.MaxTurns` is carried in packages, but the T3 create/start call does not transmit it as an enforceable backend limit; the prompt states the turn allowance for supervision. A turn count is not a context budget.
- No Steward adapter interface intercepts model continuations or tool-result insertion. No exact tokenizer/capacity declaration is available through this control contract.
- Provider-log input/output token parsing is accounting after the fact. It does not establish which context bytes were materialized at each continuation or prevent excess.

**Result.** The hypothesis fails. The current backend lacks the strict-context-budget capability. Steward can bound the initial package bytes and force a fresh thread, but cannot honestly guarantee the total working context or accumulated tool transcript.

A follow-up audit inspected the owning T3 Code source at the exact installed tag `v0.0.38` / `c0995d2eaf8ec787b3318ed1169ae266ed1529f8` (installed npm package `t3@0.0.38`) and current upstream `1de563c1491c7d82563e4553bf5bf689ce6adbb9`:

- The v0.0.38 `ProviderAdapter` contract exposes session start/send/interrupt/read/stop operations, with no bounded-generation operation or strict-budget capability.
- Its Codex adapter passes input, model, effort, interaction mode, and attachments to Codex app-server `turn/start`; it has no tools-disabled flag, complete model-envelope accounting, pre-continuation gate, or maximum output-token field.
- T3's existing worker-owned `CodexTextGeneration` is a useful structural precedent: it invokes `codex exec --ephemeral --sandbox read-only --output-schema --output-last-message`. However, read-only is not tool-disabled, Codex's hidden system/tool envelope is not counted by T3, and the invocation has no hard output-token cap.
- `OpenCodeTextGeneration` creates a fresh session with deny-all permissions and performs one SDK prompt. This is closer to the desired shape, but deny-all can still permit attempted tool/denial continuations, the inspected prompt has no hard maximum output or complete-envelope count, and the fleet's advertised `codex/gpt-*` routes do not use the OpenCode credential/quota identity.
- A direct OpenAI Responses call could expose `tools=[]` and `max_output_tokens`, but it would be a different credential/provider interface unless T3 defines and admits such a route. It cannot be silently treated as the existing authenticated Codex route.

Therefore there is no narrow T3-only wrapper over the currently deployed Codex app-server/CLI path that makes the section 15 claim true. Strict mode requires a backend/provider primitive whose complete model-visible envelope and output limit are controllable and auditable.

**Minimum necessary backend seam.** T3 (or its controlled provider adapter) must expose:

1. A declared per-route model context capacity and tokenizer identity/version, or a verified conservative byte-to-token bound.
2. Per-execution limits: maximum assembled input tokens/bytes, reserved output tokens, cumulative model-visible tool-result bytes/tokens, continuation count, and optional total generated tokens.
3. A pre-continuation hook that returns the exact/conservative materialized count and rejects before provider submission.
4. A tool-result ingestion boundary that stores the full object out of context, inserts only a bounded excerpt plus immutable reference/size/digest, and charges every page to the same cumulative execution budget.
5. Terminal reason `context-too-large` / `budget-exhausted`, measured peak counters, and an auditable coverage flag proving all continuations/tool channels passed through the guard.
6. Capability negotiation in worker inventory. Steward must refuse strict consultation packages on workers/routes that do not advertise and attest this version.

The exact installed Codex CLI was then traced to official tag `rust-v0.155.1`, commit `be2951ea34f0d295ed0becf97079f92fa5f6950e`:

- `core/src/session/turn.rs:build_prompt` copies all model-visible tool specifications from the `ToolRouter`; `run_turn` rebuilds input from session history and can issue another sampling request when a tool call or pending input requires follow-up. Disabling the sandbox or denying tool execution does not make this a single provider request.
- `core/src/client.rs:ModelClient::build_responses_request` assembles instructions, formatted history, tools, reasoning controls, output schema, routing metadata, and access programs. `ModelClientSession::stream_responses_api` performs the last mutations and then calls the transport.
- `codex-api/src/endpoint/responses.rs:ResponsesClient::stream_request` serializes that complete request with `EncodedJsonBody::encode` immediately before posting to `/responses`. This is an enforceable pre-provider-request boundary below app-server.
- `codex-api/src/common.rs:ResponsesApiRequest` currently has no `max_output_tokens` member. The public Responses API defines that request field as an upper bound including visible and reasoning tokens, but compatibility with the ChatGPT-authenticated Codex backend must be proven by an integration probe; client-side stream truncation is insufficient.
- A conservative strict input check can serialize the final request and treat every UTF-8 byte as one token. This overcounts BPE tokens and JSON framing, so it safely rejects early without depending on a tokenizer. The contract must state that the limit covers the client-assembled model envelope; any provider-added hidden input requires a declared fixed reserve or provider attestation.
- Strict single-response mode also needs a core flag that builds an empty tool router, asserts the final serialized request contains no tools, rejects any tool-call response, disables compaction/retry inference, and rejects a second sampling submission. Applying only a T3/app-server option cannot close these internal loop paths.

This is a conditional implementation path rather than an immutable adapter blocker. Admission remains blocked until a live authenticated-backend probe proves `max_output_tokens` is accepted and reported usage never exceeds it. The fork can preserve the existing Codex authentication and quota route: build the `codex-cli` package (whose `codex` binary embeds app-server and depends on `codex-core`) and point T3's Codex adapter at that worker-owned binary. The installed stripped static PIE is 257 MiB; the sparse exact-tag source checkout is 45 MiB (6.08 MiB Git pack).

## 6. Contract freeze recommendations

Freeze these integration contracts before feature/schema work:

1. **Caller principal:** execution capability fields, issuance/rotation/revocation, and same-UID limitation.
2. **Exclusive subscription:** exact reciprocal conflict rules with every existing wait kind and operator resumption.
3. **Execution kind:** fresh advisor package, worker capability, deterministic identity, scratch/read boundary, and one-response authority.
4. **Scheduling:** one shared executor registry for new claims and retained resumptions; fair ordering and measurable latency bounds.
5. **Budget capability:** strict mode is capability-gated and refused when any continuation/tool path is not covered. Byte-bounded trusted mode is a separately named pilot.
6. **Response commit:** coordinator clock deadline, exact replay semantics, custody, request/execution epoch, delivery separation, and terminal outcome vocabulary.

Stop/replan condition reached: strict section 15 enforcement cannot be implemented solely in Steward against the current T3 control surface. This does not block schema-independent work on identity, request state, exclusive parking, or fresh scheduler packages, but it blocks claiming or enabling strict-context consultation mode.

## Validation performed

At the baseline, targeted existing tests passed:

```text
go test ./internal/backlog ./internal/store/sqlite ./internal/workerruntime \
  -run 'Test(TaskIdentity|WorkerParked|AnEachWake|WaitingExternal|CreateAndStartThread|MatchWorkersCapacity|Executor)' -count=1
```

This confirms the selected existing invariants compiled and passed; it is not the new race/mixed-load qualification described above.
