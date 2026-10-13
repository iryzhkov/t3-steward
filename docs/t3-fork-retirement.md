# T3 fork retirement: branch handoff and operator gates

This handoff targets official stable T3 `0.0.45` on branch
`agent-b/t3-fork-retirement`. It accompanies the source work in this branch;
it does not authorize a main merge, release publication, fleet convergence,
credential changes, or a campaign. The owning Steward session must finish
integration and the gates below. The final independent Astra reviewer must
not be an author of this branch.

The approved inputs are kickoff
`jocasta:c9e870d209f3c376f85790dfb4ce52ef@1` and fork plan
`jocasta:377471863e2e07992b4bb5111d71b1c5@1`, with the interactive
instructions to complete retirement on a source branch and hand it off.
V2 migration, nightly rollout, and public SDK adoption are outside this
retirement. The nightly idle-wire experiment does not qualify those changes.

## Initial proofs and their limits

Official stable package provenance points to upstream source
`6c8fed35dded9ff71c5b46807125457acbb76be6`. Proofs used private installation
prefixes and owned daemon state, credentials, workspaces, and threads.

| Proof | Retained evidence | Established boundary |
| --- | --- | --- |
| Linux x64 package | agent-b evidence `official-0045-package-provenance.json`, `official-0045-signatures.json` | Wrapper/platform archive bytes, executable modes, absence of unexpected files, registry signatures and attestations. Native SHA-256 `6e2511f8a9ee9c031faacdbf34118e9ed6f403d6fedbfbab9d02a08f12e4feba`. |
| Linux x64 authentication | `auth-restart-3.json` | Owned cwd/state; discovery, unauthorized access, restart, revocation, replacement bearer, and owned cleanup. Earlier helper failures remain in custody. |
| Linux x64 execution | `execution-c275a564-c2e0-41e9-b553-62de1c639743.json` | 20 observations: native Codex cwd/session/turn linkage, completed result, replay of create sequence 8 and send sequence 10 across restart, interrupt, and session stop. Lost acknowledgment was simulated after receiving a reply; real transport fault injection and caller-process relaunch were not proved. |
| Linux x64 settled cleanup | `execution-cleanup-c275a564-c2e0-41e9-b553-62de1c639743.json` | Dated process and owned-auth checks found zero. |
| Linux ARM64 package/auth | local evidence `official-0045-arm64-handoff.json` | 2,348 regular files, exact bytes/full executable modes, declared npm bin link, no unexpected files or symlinks in archives; 11 registry signatures and 7 attestations, including both target packages. Authentication 19/19 and expiry 7/7; a 3-second token returned 401 after 3.512 seconds. No ARM64 model, catalog, provider refresh, or quota execution was claimed. |

The x64 execution receipt SHA-256 is
`45e32fcfd444ee00c7ba7cf83cb5a2fd029780e1cac7564325c0543b4addb29a`;
its cleanup supplement is
`08351ffd6eb2b9fef1e033570de901fa838ce96f393b691f9bd483e33d9fa4b7`.
The ARM64 handoff SHA-256 is
`723ef132069afd78ce69b34235e7f4aa8767139ad53b0cd541c96e510aefb936`,
and the ARM64 native binary SHA-256 is
`49906baaa37b410d68d029aecee28fa36d0235dfab1fe171d00e156615d44ab7`.
The handoff indexes its individual receipts and executed helpers, including
the retained failed configuration predicate and the source-corrected run.
All six providers were disabled before the ARM64 daemon boot.

Agent-b evidence lives under
`~/.cache/agent-b-t3-modernization/evidence` on agent-b; the ARM64 handoff
lives under that path on the collector laptop. These receipts establish
bounded initial facts. They do not replace final Steward integration tests,
release review, or fresh canary evidence.

## Claude quota source and admission requirements

Agent-b's existing service token has inference permission but its profile
request returned 403 for missing `user:profile`. The user declined a new
login. Supported T3 stable/nightly quota refresh returned unsupported empty
windows, and the guarded SDK attempts returned unavailable. Agent-b local
collection remains blocked; no capacity is inferred from those results.

The selected source is a small Steward-owned collector using the pinned
Claude Code 2.1.296 / packaged SDK 0.3.296 native OAuth profile and usage
contract. This is a private HTTP contract, not a public SDK quota API.
The SDK experimental path returned seeded cache and did not establish fresh
acquisition. Native source pin, request bounds, schema, and status rules are
documented in [claude-quota-collector.md](claude-quota-collector.md).

The laptop's own existing normal login qualified the source with exactly
one profile GET and one usage GET from one in-memory credential snapshot:
both HTTP 200, total 778 ms, authenticated account UUID equal to the
configured UUID, private post-check equality true, and unchanged file
generation. No login, refresh, retry, inference, transfer, or auth mutation
occurred. Receipt
`~/.cache/claude-quota-existing-proof/evidence/quota-joint-account-and-usage-proof-20261013.json`
has SHA-256
`6b293d4a3573127c237d67eba7436978d04eb831f3037d57b3207b1a75ac5b4c`.
Its historical session/weekly/model readings are not current capacity.
The revised external contract fixture passed 167/167 checks; receipt
`quota-contract-fixture-f7b2dd4e-00a9-4407-9156-bfd5f0fb8a6c.json`
has SHA-256
`fa60fd436703105f871e48f6fe25c31366d57de5bad5f571b257958355f322cb`.
That fixture qualified the initial source choice, not the completed adapter.

Before enabling collection, the owner must validate these integration rules:

- The collector uses only its own existing credential store. Workers can
  retain inference-only credentials. Never distribute the collector login,
  fabricate scopes, or substitute an environment-only token.
- Collector, provider instance, quota pool, canonical account UUID, and
  configured exact model/window bindings must agree. Unknown or missing
  required model coverage, invalid populated optional windows, unknown
  returned model labels, conflicting windows, or account mismatch close
  admission. Family aliases do not authorize exact catalog models.
- Acquisition identity and source provenance must survive persistence and
  negotiated transport. The integration requires `SourceID`,
  `AcquisitionID`, `OriginWorkerID`, and acquisition-start `AcquiredAt`;
  required windows must belong to one authenticated batch. Same worker and
  account IDs alone cannot distinguish a collector result from logs/cache.
  This is provenance metadata, not a separate health transport.
- Preserve acquisition timestamps across cache, restart, and worker exchange.
  A new check time cannot refresh old capacity. Durable sanitized status
  records available, partial, unavailable, or stale; a failed/partial batch
  cannot emit healthy admission. Expired evidence closes capacity.
- Extend age only for the explicitly bound collector source and account.
  The existing 60-second policy for anonymous/local observations stays
  unchanged. With 300-second cadence, 15-second jitter, 10-second request
  bound, and 50-second pipeline budget, the minimum budget is 375 seconds;
  an explicit 390-second horizon leaves 15 seconds for measured processing.
  A 360-second horizon is unsafe for this topology. Recompute the budget
  from actual exchange tick, HTTP deadline, scheduler, and processing costs.
- Preserve local restrictive evidence as a conservative veto for its
  configured instance/pool. An empty account ID cannot prove account identity
  or recovery; unknown healthy evidence cannot open a pool. Restrictive
  ties must not be overwritten by equal-time healthy observations.
- A collector-covered Claude route must suppress inference-to-get-quota
  probes during both reset recovery and outages, including credential-free
  remote workers. Fresh authenticated recovery must satisfy existing stop,
  epoch, ownership, and resume gates. Preserve Codex legacy behavior until
  a prompt-free Codex source is independently qualified.

Admission and recovery must agree in planner selection, submission,
quota waits, and ranking. Retain native/CANON/normalized stream parsers,
deduplication, malformed/partial/tailer tests, ownership fences, replay,
stop/wake behavior, workspace isolation, and rollback compatibility.
Shared Codex probe tests and persisted probe state are not obsolete merely
because Claude has a collector. Exact configuration and protocol fields
must follow the completed implementation rather than this planning text.

## UpKeeper change and immutable rollback custody

[upkeeper-t3-fork-retirement.patch](upkeeper-t3-fork-retirement.patch) is a
reviewable companion patch based on read-only UpKeeper commit
`4268515ee8baee48779b603c09d17f537b25f24d`. It removes exactly
`components.t3.artifact` from `desired/release.json`. It keeps the
`npm-package` kind, `t3` package, version `0.0.45`, service
`t3code.service`, compatibility block, and every other manifest byte.
It has not been applied to an UpKeeper candidate.

The earlier authoritative fleet observation reported release commit
`3e9acaad4379720d9e632c253c76d442d82afba1` and canonical digest
`c72f32c640e94e3d1edaf45711b3c2aef40a9ea77fd5416e7d40e75b8be9602a`.
These differ from the read-only source and are historical observations,
not current publication fences. The owner must reconcile them and the
untouched rc.127 candidate before preparing a release. Do not apply this
patch over that candidate or reuse the historical digest.

Preserve the generic controlled-artifact installer, transaction journal,
controlled state, immutable backups, and receipt verification. UpKeeper's
same-version controlled-to-upstream identity transition exists independently
of version comparison; retirement must use that path with fresh idle
checks. Keep this rollback source and both asset/receipt pairs:

- Repository `iryzhkov/t3code`, commit
  `57b900a98bc5482a6d0e6b66742fb8cd3570e264`, tree
  `62f724e2e4f8a0e9966586100d9b29c9f824622c`, tag
  `fleet-v0.0.45-m16-20261005`.
- `t3-fleet-0.0.45-linux-arm64.tar.gz`: SHA-256
  `3d2e460f46d5691977784086118b00ee4bf4a438fe26263f1e135b4cf09a3300`;
  `t3-fleet-0.0.45-linux-arm64.receipt.json`: SHA-256
  `3c668b5aee3032ecab2e96fced3f50f5390ca31c7e4b6143a49410ca7bc34e36`.
- `t3-fleet-0.0.45-linux-x64.tar.gz`: SHA-256
  `aafeb8d19497adb93fd03e1c6384dc96a78fcd140b2f1ee7ea7e552926bf49d5`;
  `t3-fleet-0.0.45-linux-x64.receipt.json`: SHA-256
  `4c3fbd7c30059438b02c0cb77bb728a969a3067e67c6d37b6a553bdc7470fd04`.

## Owning-session qualification and publication procedure

1. Finish source integration and scoped obsolete-code removal on the branch.
   Validate staged configuration with
   `t3-steward config validate --file STAGED_CONFIG --json`.
   Keep the collector disabled in defaults. Resolve actual account/source
   bindings and timing budget without placing secrets or real account
   identifiers in committed examples.

2. Run the complete owning gates from [CONTRIBUTING.md](../CONTRIBUTING.md):
   `make test`, `make lint`, and applicable `make qualification`.
   Targeted/check-fast tests are iteration aids. Cover native acquisition
   cancellation, fixture/schema limits, restart/outage/stale/partition
   behavior, account and source binding, restrictive ordering, required model
   coverage, transport negotiation, and probe suppression. Review all final
   source and tests independently with Astra after these checks.

3. Qualify official stable packages privately on each deployment architecture.
   Pin both `t3@0.0.45` and the appropriate
   `@t3code/t3-linux-x64@0.0.45` or
   `@t3code/t3-linux-arm64@0.0.45`, installing with `--ignore-scripts`.
   Verify archived bytes, modes, declared bin links, registry signatures,
   and upstream attestations before use. Use an owned temporary cwd/state,
   loopback daemon, unique auth label, and owned disposable project/thread.
   Disable providers before boot for auth-only tests; keep sanitized
   receipts and both successful and failed evidence. Revoke only owned
   credentials and check settled process/auth cleanup.

4. After integration, run bounded real runtime qualification using the
   opt-in test described in [t3-protocol.md](t3-protocol.md), with the
   privately pinned T3 binary and newly built Steward binary. Inspect that
   harness's cwd/state isolation before running it. It spends inference.
   Claude inference is prohibited until **2026-10-15 09:00 America/Los_Angeles
   (PDT)**; that time boundary does not itself grant execution permission.
   Use only an explicitly authorized exact catalog model. Keep
   `t3.send_thread_environment: false`; the existing environment sentinel
   was absent, so preserve the workspace identity-file fallback. Synthetic
   quota matrices do not replace fresh authenticated collector proof.

5. Before any cutover, preserve an immutable, coherent V1 backup: SQLite
   through its supported online backup or a cleanly stopped checkpoint,
   configurations, credential references, launcher/unit identity, installed
   binaries, pending-command journal, attempt ownership, and controlled
   transaction receipts. Protect secret-bearing backup bytes and retain
   checksums. Inventory unfinished attempts and pending commands; no V2
   import or migration is part of this handoff.

6. Publish source only after the final branch review and owning merge
   decision, then verify CI for that exact source commit. Prepare the
   UpKeeper release separately in its owning checkout, incorporating the
   reconciled artifact-only delta and accepted Steward source. Verify its
   full tests, lint, source CI, manifest diff, and complete release contents;
   review compatibility/version changes separately from this patch.

7. Before release publication, inspect `upkeeper status --json`,
   `upkeeper hold show`, and read-only `upkeeper fleet-plan --json`.
   Preview **all** target drift, including unrelated components. Inspect
   each timer/startup convergence path: publishing can cause activation
   before a manual pull. Preserve the unpublished candidate if any target
   could activate unsafely. Hold changes or overrides require the owning
   operator's authorization; do not clear holds as part of this recipe.

8. Obtain the canonical `release_sha256` from a fresh read-only fleet plan
   for the final clean release HEAD. After confirming its source/remote
   alignment and the reviewed target disposition, publish using
   `upkeeper push --authored --expect-head HEAD --expect-release-sha256 DIGEST`.
   Replace both placeholders with those fresh reviewed values. Do not
   recapture the host with ordinary push. Confirm exact release CI before
   later convergence.

9. Start with an explicitly approved idle canary. Immediately before
   activation, prove zero active tasks/threads and no unresolved ownership,
   using the actual target runtime and fresh process/unit identity. Recheck
   at the mutation boundary; an old inventory or a zero cached count is
   insufficient. Preview the selected target with the owning UpKeeper
   dry-run flow, then perform only the approved convergence. Check official
   served native identity, restart/auth behavior, bounded execution/stop
   behavior when authorized, collector freshness, and quota veto/recovery.
   Preserve the controlled transaction's backups. Expand hosts only after
   the canary gates and owner approval.

10. If a gate fails, stop further convergence and preserve evidence.
    Use the reviewed UpKeeper rollback flow for the retained release and
    architecture-specific artifacts; restore matching V1 state/config only
    after stopping and reconciling affected attempts. Verify launcher/unit,
    native identity, authentication, and attempt ownership before resuming.
    Reconcile pending command acknowledgments and unfinished attempts
    before redispatch; never automatically replay uncertain work or erase
    transaction backups. Preserve scanner retirement and existing holds.

Report source publication, release publication, and verified deployment
as separate outcomes, with exact commits/digests and dated target receipts.
No step above waives the owning review or release gates.
