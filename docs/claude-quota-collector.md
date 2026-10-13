# Claude quota collector

The collector reads quota without running a model or depending on a patched T3.
It uses one existing local subscription credential snapshot for a profile GET and
a usage GET, then checks that the private credential bytes and file generation
are unchanged. A profile UUID must equal the configured account UUID. An env-only
inference token is not a substitute for a stored profile-capable login.

The collector is disabled by default. This branch's first slice implements
acquisition, configuration, source composition and local status. Do not enable it
until the remaining admission and transport work below has passed review.

## Pinned contract

The HTTP contract was read from the published Claude Code 2.1.296 native executable
included in `@anthropic-ai/claude-agent-sdk` 0.3.296. The arm64 executable SHA-256
is `f1f6e96e0d8342b9dbf41d7e88255397a6a52ce3d8736ad6a4c6b59c9b62fefa`.
SDK packaging is source provenance; the collector does not invoke the SDK or CLI.

Both calls target `https://api.anthropic.com`. Native function `X6e` at embedded
offset 209830610 reads `/api/oauth/profile` with Bearer authorization, JSON content
type, `Cache-Control: no-cache` and `User-Agent: claude-code/2.1.296`.
Native `km` at 211080959 reads `/api/oauth/usage`; its OAuth header builder adds
`anthropic-beta: oauth-2025-04-20`. These private endpoints can change. Version
updates require source inspection and a new acquisition qualification.

Usage requires typed, finite utilization from 0 through 100 and a future RFC3339
reset with a timezone for `five_hour` and `seven_day`. Optional legacy
`seven_day_opus`, `seven_day_sonnet` and `seven_day_oauth_apps` may be null.
A populated optional window requires an explicit binding.

Modern model windows come from `limits[]` rows with `kind: weekly_scoped`,
`scope.model.display_name`, numeric `percent` and `resets_at`. Native `Pm`
at 211079432 and `Gxt` at 211079755 define this projection.
`seven_day_overage_included` is a legacy internal Fable label; it is not treated
as an invented raw endpoint field. Bind an observed display name to an existing
window key and exact catalog model IDs. A family label never authorizes a model.

A single acquisition shares a ten-second context and a 64 KiB limit per response.
There are no redirects, retries, credential refreshes, logins, subprocesses,
hooks, MCP connections, behavior scans or inference requests. HTTP/1 with fresh
connections prevents Go transport retries of reused connections or HTTP/2 streams.
Credentials remain in host memory and are sent only to the pinned endpoint.

## Configuration and timing

Enable only on a collector host whose own existing login can read the selected
account. Workers may retain inference-only credentials. Configure
`claude_quota_collector.account_id` to equal the selected
`backlog_v2.quota_pools[quota_pool].account_id`; never infer an account from a
pool name, an env token or another worker's account.

`provider_instance_id` must already be bound to that pool. Model bindings use
either `display_name` for modern rows or `source_key` for a proved legacy field,
plus `window`, `label`, `selector`, `models` and optional `optional`.
Each exact model must already be in that pool's configured catalog.
All required model windows must be present and valid before any snapshot is
emitted. Optional missing windows are omitted, never recorded as zero.
Unknown populated model windows fail the acquisition rather than disappearing.

Collection starts every 300 seconds with at most 15 seconds of jitter.
The acquisition deadline is 10 seconds. The declared `pipeline_budget` must
cover a worker observation exchange tick, its transport deadline, the next
coordinator scheduler tick, and measured local processing. Configuration checks
the first three against two scheduling intervals plus the transport timeout.

The pool's explicit `observation_max_age` must cover interval, maximum jitter,
acquisition and that pipeline budget. The current 10-second scheduler and
30-second transport bounds require at least 50 seconds of pipeline budget:
300 + 15 + 10 + 50 = 375 seconds. A scoped 390-second horizon leaves 15 seconds
for qualified processing delay. A 360-second horizon is rejected. If measured
processing needs more time, declare more pipeline budget and recalculate the
pool horizon. Do not shrink existing transport settings to fit an old proposal.
Zero pool age preserves the unrelated global `quota_max_age: 60s` default;
it cannot substitute for an enabled collector's explicit scoped horizon.

## Failure and restart

A successful acquisition emits ordinary account-bound `QuotaSnapshot` records
through the existing source and `HandleSnapshot` pipeline. Their observation
time is acquisition start, not a later scheduler timestamp. One random event
identity groups the windows. Provider-log collection remains in place for
Claude, Codex and other providers.

Failure or partial data emits no snapshot and cannot claim healthy capacity.
The collector writes only sanitized local diagnostic status, including attempt
and last-success intervals, static failure classification, HTTP status, window
count, account fingerprint and server-directed cooldown. It never writes a
credential digest, token, profile, email, response body or raw transport error.
Atomic mode-0600 writes sync the file and directory. Status persistence failure
suppresses a valid batch. Restart preserves source times and honors Retry-After
before issuing another request. Status is not account evidence or a health wire
upgrade. Outages must close admission when existing observations become stale.

## Remaining integration before activation

The first slice adds pool AccountID and observation age configuration but does
not yet propagate scoped age through all coordinator, worker, wait and policy
consumers. Their existing age limits remain conservative and unchanged.
Snapshot model selectors still use the existing representation. Exact model
bindings must reach admission without granting additional model authorization.

The integration slice must preserve tied competing observations in
`MergeQuotaObservations` and `ReadQuotaWindows`, keep conservative downstream
reconciliation, and treat fresh unknown-account restrictive evidence as an
overlay on the explicitly bound pool. It must retain raw empty account keys,
never relabel them, count them as required coverage, or use them to open a pool.

Qualify account-bound transport, outage/partition and mixed-window behavior
before removing Claude collector-covered inference-to-get-quota probe paths.
Do not remove Codex probes without a qualified prompt-free Codex source.
Keep normalized stable T3 0.0.45 logs, native/unified event parsing and their tests.

The complete fork-retirement branch also needs official stable compatibility and
runtime qualification, replacement tests for removed paths, full make test and
make lint, independent review and a handoff to the owning Steward session.
No production enablement, main merge, fleet release or service replacement is
part of this isolated source slice.
