# M16 explicit immutable reviewer execution profiles

Controlling plan jocasta:b694668a6fba8a4e1329a658fcf6e0e5@1. This is an internal local implementation atop accepted 49b99fd2e2763ead3f68babd37c41af2783bf9c0; independent Sol and Astra medium reviews remain lead-owned.

Version 2 review_requirements requires each member's execution block:

```yaml
execution:
  effort: medium
  quota_pool: review-pool
  max_turns: 9
  resources:
    preset: build
    cpu_units: 1.5
    memory_mb: 512
    scratch_mb: 64
```

Effort is exactly medium or high; max/above, whitespace, controls, unknown and missing values refuse. The producing campaign stays medium. Quota pool is a canonical identifier matching [A-Za-z0-9][A-Za-z0-9._:-]{0,127}. Turns are exactly 1..32; zero/missing values neither default nor clamp. Resources must be explicit, expand existing build/light presets and resolve a valid nonempty minimum CPU class. Existing class/preference/preset validation applies. Each explicitly declared numeric scalar must be positive; CPU units must also be finite. Optional absent scalars remain zero in normalized domain.ResourceDemand and confer no CPU/memory reservation.

The static catalog must authorize the exact project, provider instance/model and requested quota pool. Selected grants are filtered to that pool before policy hashing, so a pool from another project, instance or model grants nothing. Catalog authorization supports its existing sole-wildcard model grant semantics; the reviewer route remains concrete. Authored worker availability, drain status, live quota, pressure and connection observations do not enter permanent validation. The configured catalog still owns pool-definition eligibility. Original frozen admission provenance/policy/issued revision remains authoritative after catalog change.

Domain ReviewExecutionProfile has scalar effort, pool, turns and normalized resources. TaskReviewMember, AdmissionMember and MemberRequirement hold pointers with omitempty JSON. Nil pointers preserve the existing legacy field order/serialized bytes and canonical admission/requirements/declaration digests. Version 1 accepts historical absent profiles, refuses execution declarations (including explicit YAML null), and keeps MaxTurns12/empty Options/pool/zero resources in its internal Build/replay. Version 2 requires every profile. Frozen requirements refuse mixed or malformed profiles. Neither version 1 compatibility nor synthetic two-family metadata authorizes public invocation or production diversity.

Custody: manifest defaults detach member/execution/resource pointers; compilation normalizes into fresh scalar profiles; domain CloneTaskReview, graph/planner cloning, policy conversion, Requirements construction/Snapshot, FrozenAuthority canonicalization and child Build isolate pointers/maps. Each profile scalar affects declaration, admission policy and requirements identity. Child Build derives exactly MaxTurns, ResourceDemand, QuotaPoolID and Options containing only effort from the frozen profile, with no parent/worker inheritance or caller option override.

SQLite retains immutable JSON authority/creation receipts and validates full task definitions, original graph/run graph and marker receipt by existing deep equality. No schema or transactional redesign is needed. The owning validateDeclaredAuthorityTx guard additionally accepts validated v1/v2 and compares full execution profiles against the saved declaration; this is necessary to permit v2 while refusing rehashed declaration/profile replacement inside the writer. Existing MaterializeReviewChild/replay full-definition comparisons are unchanged. Tests prove all eight fields covered across template, graph, run graph and receipt tamper, and prove authority changes refuse before/after materialization with every logical SQL/native audit record unchanged. Disposable corruption tests first prove immutable graph/marker update triggers refuse, then remove the specific trigger only in their temporary test DB to simulate corrupted persisted bytes. Valid terminal evolution and read-only reopened replay remain supported.

Tests named TestReviewExecutionProfile cover strict parser/compiler and presets, unsafe/missing/mixed settings, per-field digests, legacy golden layouts and Build/replay, static exact grants and zero SQL mutation, configured drain behavior, original-authority catalog-change replay, pointer/map isolation, actual configured clone/rerun custody, fresh/reopened materialization and every-field tamper/refusal. Complete accepted Sol/Astra review artifacts are preserved byte-for-byte under docs/m16-execution-profiles; their complete Go fences are permanent independent_fix3_* overlays, changed only by external gofmt. Existing accepted portable tests/staging implementation are unchanged.

Limits: no public invocation/auth transport, parking/early completion/restart integration, physical worker/final HEAD proof, provider sessions, M16c/M17, fleet/provider diversity qualification, CI or deployment. Reopening SQLite is not OS restart; logical SQL equality is not physical page equality. Existing filesystem and SQL custody guarantees and their stated limitations remain. No nested reviews/delegation/escalation, Claude calls, schedules, config/trust/admin/live service/fleet actions or publication. This work remains provisional pending both separate independent lead-owned reviews.
