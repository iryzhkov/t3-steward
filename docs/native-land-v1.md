# Native landing v1 (BL-t0)

Design contract at base `cbf89522f13ed4e441cb87f95bc7fc29f51ba328`; this document adds no executable behaviour. A campaign may opt into coordinator-owned publication with `land: {branch: main}`. The coordinator owns admission, serialization, durable state and the push; a deterministic worker performs Git integration and verification. Tasks never receive push credentials. The coordinator never runs manifest verification commands.

## Manifest and acceptance

Add optional top-level `land` to the version 2 manifest, with exactly one required string `branch`; omission preserves today's behaviour. Reject null/empty objects, unknown keys, whitespace, full refs and values failing Git's branch-name validation; construct only `refs/heads/<branch>`. Require a Git environment and a catalog project resolving to one canonical GitHub repository accessible to the configured installation. Submission pins that repository's numeric ID and target ref; aliases share the same repository lock. Require an existing target branch, never create it implicitly.

For v1, resolve exactly one final local producer/declaration and its local review through the declared dependency graph; reject ambiguous multiple tips/declarations, missing review or missing producer verification commands. Chained inputs remain supported as ancestry. Fix loops resolve to their final accepted producer attempt, never an earlier round. A landing is eligible only after that producer succeeds, all campaign prerequisites succeed, and a succeeded review returns structured ACCEPT bound to that exact attempt and declared commit SHA, with the producer's verification evidence passing. Existing `needs_verdict: ... accept` dependency guards still apply, but do not alone prove which SHA was reviewed. A replacement head needs its own ACCEPT.

## Execution and state

| State | Owner and transition |
| --- | --- |
| accepted | Coordinator atomically records eligible accepted SHA, recorded base, producer/review IDs, immutable gate definition digest, repository/ref and FIFO sequence. |
| landing | Coordinator takes the repository slot, records target SHA and generation, and dispatches a built-in landing job. Worker imports the accepted bundle, integrates and verifies, then declares `landed` with retained bundle and receipt. Coordinator validates them and performs the push. |
| landed | Coordinator observes the proposed SHA at the remote ref (or as its ancestor after later advancement), records the receipt and releases the slot. This is landing success, separate from campaign execution success. |
| conflict | Worker reports a failed clean rebase, unsupported history, or invalid ancestry/provenance; coordinator records attention and stops this landing. No push. |
| gate-failed | Worker reports nonzero exit, timeout, workspace mutation or missing verification evidence; coordinator records attention and stops. No push. |
| push-failed | Coordinator reports credential, transport, authorization or branch-policy failure; retain the exact candidate and effect intent for reconciliation/retry. No success inferred from a local exit alone. |

The worker receives immutable accepted SHA A, recorded producer base B, current target T, bundle digest and the producer's effective verification definition (ordered `verify` commands and, if declared, `gate.commands`, with their existing timeout semantics). Its built-in runner uses a fresh detached workspace, no agent session, hooks or inherited signing configuration. It imports verified objects and checks B is an ancestor of A.

If T is an ancestor of A, fast-forward: candidate L=A, preserving reviewed bytes and the original successful verification receipt; no rebase gate rerun is required. If A is already an ancestor of T, mark landed as an already-integrated no-op after proving ancestry. Otherwise require B to be an ancestor of T and replay exactly B..A onto T using fixed Git arguments and recorded committer identity/time per generation. A clean rebase means a linear commit range, every commit applies without conflict or manual edits, no skipped/empty commits, and a clean index/worktree. Merge commits or an unrelated/missing base stop as conflict; v1 does not invent a merge strategy.

After rebase, run every original producer verification command, in original order and environment contract, against L on the worker; do not substitute affected tests for this gate. The gate must leave HEAD=L and the tracked tree clean. Record command-definition digest, per-command exit codes, timeout and logs bound to L, then export the declared `landed` commit. Rebase requires no new agent-authored edits; a conflict or gate failure must return to implementation and exact-head review. Coordinator validates the bundle digest, declaration, repository, A/B/T/L binding and successful gate receipt before any credential resolution. It may run only built-in Git object/ancestry operations with hooks disabled, never scripts from the bundle.

## Ordering, push credentials and recovery

Persist a FIFO queue by canonical repository ID (across branches), ordered by the acceptance transaction's monotonic sequence. One landing owns that repository from target observation through reconciliation. A stopped failure releases the slot; an explicitly retried entry joins the tail. A push of uncertain outcome retains the slot until reconciled. Generation/epoch fences prevent stale worker results and stale coordinator owners from publishing.

Immediately before pushing, reread the target. If it advanced, invalidate the old generation and recompute integration from A against the new target, including another gate on any rebased candidate. Persist a push intent containing repository/ref, expected T, exact L and generation before the network effect. Publish only that explicit SHA/ref with an atomic expected-old-ref check (Git receive-pack compare-and-swap), after verifying T is an ancestor of L. Never authorize a non-fast-forward update, delete, wildcard refspec or implicit branch creation. A concurrent external writer causes reconciliation/reintegration, never overwrite.

For each push effect the coordinator alone resolves `secretref:github-app/iryzhkov-steward.pem`, App `iryzhkov-steward` (ID 5243126), installation 169398973. Reuse the owner-only secret-store/reference pattern; the existing admin resolver is not a PEM/App-token implementation. Sign an RS256 JWT with `iss=5243126`, `iat=now-60s`, `exp=now+600s`. Call `POST /app/installations/169398973/access_tokens` with `repository_ids: [pinned_repository_id]` and `permissions: {contents: write}`; do not omit either restriction. Check the returned repository, permissions and expiry (implicit metadata read is allowed). Installation tokens expire after one hour; mint immediately before the effect, keep only in coordinator memory, revoke after use where possible, and mint a new token for a retry. Never log key/JWT/token, embed them in a remote URL/argv/config, retain them in state, or pass them to a task. Supply authentication through a coordinator-owned transient credential channel with Git tracing disabled. Branch policy or workflow-file restrictions fail as push-failed; never widen permissions or bypass protection. API details: [JWT](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-json-web-token-jwt-for-a-github-app), [installation tokens](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app).

Idempotency key: repository/ref + run + producer attempt + declaration + accepted SHA; a unique durable record deduplicates repeated ACCEPT delivery. Each generation also pins T, L, bundle and gate receipt digests. On restart, reclaim the durable repository slot under the current epoch; resume accepted entries, reattach/reconcile an existing landing job rather than duplicate it, and reuse a verified candidate only while its target generation remains valid. Before retrying any persisted push intent, read the remote: ref=L or descendant of L proves success; ref=T permits retry of exact L; another ref requires reintegration or attention if ancestry cannot be established. An unreachable remote keeps the outcome unresolved. Missing/corrupt retained evidence stops with attention rather than trusting a workspace. Tokens are never recovered from disk.

Conflict, gate-failed and push-failed emit deduplicated attention events keyed by landing/generation/reason, carrying run/task/attempt, repository/ref, A/T/L where available, state, sanitized reason and retained diagnostic/gate artifact references. Surface the event through existing attention/notify handling. Operator retry reconciles first; source repairs require a new accepted head.

## Implementation split

Each unit targets about 400 changed lines including focused tests; split further before exceeding that budget.

| Unit | Change and verification | Depends on |
| --- | --- | --- |
| BL-t1 | Manifest `land`, strict validation, unique producer/review binding; reject invalid/ambiguous manifests. | BL-t0 |
| BL-t2 | Durable landing records, FIFO repository slot, generations and ACCEPT deduplication; migration/restart tests. | BL-t1 |
| BL-t3 | Built-in worker job, fast-forward/rebase and gate receipt; local Git fixtures for clean/conflicting replay and failed/mutating gate. | BL-t1 |
| BL-t4 | Exact `landed` bundle export/import and receipt checks; reject swapped SHA, digest or generation. | BL-t2, BL-t3 |
| BL-t5 | Coordinator App signer/token transport and restricted push; mocked scope/expiry/redaction and local compare-and-swap tests. | BL-t4 |
| BL-t6 | Controller wiring, restart/push reconciliation and attention; lost-reply, external advancement and serialization tests. | BL-t2, BL-t4, BL-t5 |

Pinned code map: `internal/backlog/manifest.go` defines manifest/task/commit fields and calls `validateNeedsVerdict` (implemented in `manifest_fix_loop.go`); `docs/campaign-commit-export.md` and `internal/backlogadmin/commit_export.go` supply declared-commit provenance and verified export. `remote_credential_store.go` supplies the secret-reference/private-file pattern; `internal/backlog/git_ls_remote.go` supplies validated built-in repository probing and credential-reference-aware evidence. None currently provides native landing or App-token push.

## FOLLOW-UPS

Multiple independently reviewed campaign tips and merge-preserving integration are outside this v1 contract. Release/tag/CI orchestration and broader credential enforcement remain separate units.
