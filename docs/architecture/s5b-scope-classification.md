# S5b: scope classification for the Track S core closeout

Status: active. Supersedes the implicit assumption that every mechanism named in the Track S
closeout plan is built during this campaign.
Date: 2026-09-13
Authority: the user's scope correction of 2026-09-13, which directs that the closeout plan be read
as a design inventory rather than as a mandate, and that the campaign optimize for one proven use
case.

## Why this document exists

The Track S core closeout plan describes a complete orchestration platform: portable ephemeral
execution environments, least-privilege credential grants, generalized external-effect recovery,
storage reservations with retention and garbage collection, priority preemption, mixed-version
rolling upgrades, backup and restore, attention aggregation, and empirical quota accounting with
flexible model routing. Each of those is a defensible design. Built together and now, they are a
platform without a workload.

The steward has one workload that is actually proven and actually waiting: Huyang-scale backlog
batches. The correction therefore changes what S5b produces. S5b no longer freezes every record in
the plan. S5b classifies every proposed capability into one of three buckets and commits the
campaign to a thin vertical slice that serves that workload end to end.

The closeout plan is not withdrawn. It remains the inventory this document classifies, and a
deferred entry here keeps its design intent available for the day a real workload asks for it.

## The three buckets

**Implement now.** Part of the thin vertical slice. Designed, built, tested and exercised by the
end-to-end evidence batch that closes this campaign.

**Seam only.** A clean interface or boundary is preserved so the capability can be added later
without reworking what the slice builds. No mechanism, no policy engine and no state machine is
built now. A seam earns its place only if omitting it would force a later rewrite of slice code;
a seam is not a cheap way to keep a feature nominally alive.

**Defer.** Not designed in detail now. The intent is recorded together with the trigger that would
justify revisiting it. Deferring is not rejecting, and it is not a promise either.

## The thin vertical slice

The slice is exactly these seven capabilities. Nothing is added to it without user direction.

1. Reliable worker and catalog interoperability across homelab and omarchy-pc.
2. Capability and CPU-class placement, with higher executor concurrency.
3. YAML batch submission.
4. Minimal prompts carrying preflight and context results.
5. Basic portable plan, file and test-result artifacts.
6. Correct quota observation and admission.
7. The existing asynchronous wait behavior, preserved rather than rebuilt.

Item seven is a deliberate non-goal for new work. The universal-wait rework described in S5f and
S6b is not part of the slice; today's wait behavior is kept working and is not extended.

## Classification

### Worker interoperability and execution environments (closeout S5c)

| Capability | Bucket | Note |
| --- | --- | --- |
| `WorkerConformance` and `worker doctor` | Implement now | Two hosts cannot interoperate reliably without an observable statement of what each can do. Scope it to the capabilities the slice actually consults. |
| `ProtocolVersion`, `CapabilitySet` negotiation | Implement now | Needed so an incompatible worker is excluded before assignment rather than failing mid-attempt. The existing recovery for a worker whose retained catalog this build cannot activate is the concrete case. |
| Catalog activation and recovery across two hosts | Implement now | Slice item one. |
| `ExecutionEnvironmentSpec`; attested host profile versus ephemeral environment | Seam only | Keep the distinction nameable and keep the assignment path from hard-coding the host profile. Build no ephemeral-environment mechanism. |
| `EnvironmentArtifact`, `EnvironmentAttestation`; digest-pinned custom toolchains | Defer | Trigger: a batch that genuinely needs a tool version the host baseline cannot provide. The plan's custom-Huyang acceptance case is a real future need, not a present one. |
| `CredentialGrant`, `NetworkPolicy` | Defer | Deferred by the user as comprehensive credential and egress management. The default-deny intent is recorded; no grant lifecycle is built. |
| `WorkerDrain`, mixed-version rollout | Defer | Deferred by the user as rolling-version machinery. Trigger: a fleet upgrade that cannot take a maintenance window. |
| `StateSchemaVersion` | Seam only | A version stamp that refuses an unknown future schema is cheap and prevents silent corruption. No migration engine. |
| `BackupManifest`, restore from clean coordinator state | Defer | Deferred by the user as disaster recovery. |

### Placement, capacity and executors (closeout S5d)

| Capability | Bucket | Note |
| --- | --- | --- |
| `ResourceDemand`, `WorkerCapacitySnapshot` | Implement now | Slice item two. |
| `ExecutorPool`, `ExecutorSlot` | Implement now | Replacing the singleton execution assumption is what raises concurrency. |
| `ResourceReservation` with exactly-once release | Implement now | A leaked reservation silently lowers achieved concurrency, which is one of the numbers the evidence batch must report. |
| `PlacementDecision` and its explanation | Implement now | Constraint rejection, preference score, reservation and selected worker must be recoverable from durable state. |
| CPU class separate from allocatable capacity separate from pressure | Implement now | normandy low, homelab medium, omarchy-pc high remains a user decision, not a benchmark output. |
| 2/4/8 concurrency floors | Implement now | The qualification target the evidence batch measures per host. |
| `QueueEntry`, `PriorityClass` | Seam only | Enough shape that a priority policy can be added later without reworking admission. No policy engine. |
| `PreemptionRequest`, safe-boundary preemption | Defer | Deferred by the user as priority preemption. |
| Queue aging and anti-starvation | Defer | Trigger: an observed batch where background work measurably starves. |

### Artifacts, conditions and effects (closeout S5e)

| Capability | Bucket | Note |
| --- | --- | --- |
| `ArtifactDeclaration`, `ArtifactInstance` | Implement now | Slice item five, scoped to plan, file and test-result artifacts. |
| `InputBinding`, `OutputContract` | Implement now | Readiness is an input-contract decision and success is an output-contract decision; the slice needs both to pass a plan from one task to the next. |
| Portable artifact custody across hosts | Implement now | A plan produced on one worker must be consumable on another with matching digest. |
| `WorkflowBundle` | Implement now | Slice item three; a batch submits a manifest plus referenced prompt files. |
| `Condition` | Seam only | The slice needs dependency state and artifact-verified only. Keep the evaluation typed and bounded so richer conditions do not later require arbitrary shell. |
| `ExternalEffect`, `EffectReceipt` | Seam only | The existing worker command journal already gives durable effect identity and observe-before-create for the T3 path. Keep that boundary and keep observe-before-retry for the one Git-push case. Build no general effect engine. |
| Generalized effect sagas | Defer | Deferred by the user. |
| `StorageReservation`, `RetentionPolicy`, garbage collection | Defer | Deferred by the user as artifact retention and GC. Trigger: artifact bytes becoming a capacity problem on any host. |
| Confidential-artifact encryption and tombstones | Defer | Follows the credential deferral. |

### Temporal orchestration and interaction (closeout S5f)

| Capability | Bucket | Note |
| --- | --- | --- |
| Existing asynchronous wait behavior | Implement now | Preserve it. This is explicitly a keep-working item, not a rework. |
| `ScheduleSpec` | Seam only | Existing schedules keep working unchanged; no new schedule semantics. |
| `WaitSpec` as a universal session primitive | Defer | The rework that lets a wait target any interactive T3 thread is not needed by a backlog batch. Trigger: a user-facing wait that today has to invent a backlog item. |
| `UserDecisionRequest`, `UserDecisionResponse`, `InteractionPolicy` | Seam only | The campaign's own blocking protocol uses T3's structured user-input tool. Keep the boundary where a blocked attempt can become a durable request; build no suspension-and-resume machinery. |
| `CancellationRequest`, `DeadlinePolicy` | Seam only | Cancellation already exists; do not extend it. |
| `ClockPolicy` | Seam only | Record that coordinator time is lease authority and worker wall clocks are observations. No skew-tolerance engine. |
| `Suspension`, `WakeEvent` | Seam only | Existing behavior only. |
| `AttentionEvent`, `AttentionPolicy` | Defer | Deferred by the user as attention aggregation. |

### Quota measurement and control (closeout S6 and S6a)

| Capability | Bucket | Note |
| --- | --- | --- |
| `ProviderUsageObservation`, `QuotaDelta` | Implement now | Slice item six. Correct observation is the prerequisite for every quota claim the evidence batch makes. |
| Restart-safe, deduplicated ingestion | Implement now | Without it the reported quota consumed is not trustworthy. |
| Admission against observed buckets | Implement now | Slice item six. |
| `UsageAttribution` | Seam only | Keep exact, inferred and unattributed distinguishable. Build no inference model. |
| `BudgetReservation` | Seam only | Enough to stop admitting work when a bucket is exhausted. |
| `QuotaAccountingModel`, multiplier and drift detection, `BudgetForecast` confidence | Defer | Trigger: a measured forecast error that actually misleads admission. |
| Bucket policy states beyond open and stop | Defer | Conserve, drain and rearm are policy the slice does not exercise. |
| `CheckpointRequest`, `CheckpointEvidence` | Seam only | Keep the boundary; build no checkpoint negotiation. |
| `ModelPolicy` flexible and constrained routing | Defer | Deferred by the user as sophisticated model routing. Fixed routing only. |

### CLI and API (closeout S6b)

| Capability | Bucket | Note |
| --- | --- | --- |
| `workflow validate`, `workflow plan`, `workflow submit` | Implement now | Slice item three, including canonicalization, digest and an idempotency key. |
| Diagnostics with source line, column and JSON Pointer | Implement now | A batch author cannot fix a hundred-task manifest without them. |
| `preflight show`, `context show` | Implement now | The evidence for slice item four has to be inspectable without agent context. |
| Stable JSON for slice reads, documented exit codes | Implement now | Automation never parses human output. |
| The remaining canonical namespaces | Seam only | Name them; do not build them ahead of a caller. |
| Compatibility alias removal plan | Defer | Follows the legacy-deletion deferral below. |

### Campaign tail (closeout S7, S8 and S9)

| Capability | Bucket | Note |
| --- | --- | --- |
| The frozen end-to-end scenario matrix | Defer | Replaced for this campaign by one realistic Huyang-scale end-to-end batch. |
| Legacy deletion and v2 client migration | Defer | The slice does not require removing the legacy intake. Trigger: the evidence batch showing the legacy path causing divergence. |
| 24-hour soak, UpKeeper publication, release 0.12.0 | Defer | The campaign now ends at the evidence gate, not at S9. |

## What the campaign now ends with

The campaign no longer runs to S9. It ends at an evidence gate: one realistic Huyang-scale
end-to-end batch, reporting operator intervention required, successful task percentage,
concurrency achieved per host, quota consumed, prompt and token cost, failures attributable to
steward, and which deferred capability the evidence actually justifies next.

The platform is not expanded past that gate without user direction. That last report is the input
to the next scope decision, and choosing what to unfreeze is the user's call, not a continuation
the campaign makes on its own.

## Consequences for the checklist

S5b closes on this classification plus the two area decision records it points to. S5c through S6b
are no longer completed as written; they are completed only to the extent their implement-now rows
above require. S7, S8 and S9 remain unticked and are explicitly out of this campaign's scope.

Related records: [placement and capacity](adr-s5b-placement-and-capacity.md),
[execution environment and worker interoperability](adr-s5b-execution-environment.md).
