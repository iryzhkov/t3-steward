# T3 fork retirement: monitoring integration handoff

This branch integrates the existing native coordinator producer from PR73,
commit `89eb9fb49bfbfe7b8981d152366a00dbd2f21710`, with the fork-retirement
quota work. That producer was cherry-picked as `5de9a378593acba7e7841f41e78f38cd49e76710` after M3 candidate
`5f034585d983c4c29cdd6c4f229a8bdfa018be51`; its owner branch remains separate. The five published CI jobs
passed at the original producer commit and do not qualify this combined
candidate. Producer reconciliation and focused checks are branch work;
complete source tests, lint, exact CI, and independent Astra review in this
root session remain required before handoff. No runtime was deployed.

The approved monitoring plan is
`jocasta:9bb096dcbc9f16d032e7c7e1854aa22a@2` (122 lines; SHA-256
`f2e6d1750930796b569bddac257efcfa3a920e97409afbec76fc15b8e8ce10aa`).
The homelab context is
`jocasta:bea8000b05abede0fd4d1b2e9fa3fd56@8`; its newer revision 9 was
also read. Source preparation does not prove release or runtime deployment.
No new alerts, second exporter, token panels, raw usage aggregation, or
production UI changes are included in this companion.

## Coordinator placement and companion configuration

The recorded coordinator is `normandy-coordinator` on inventory host
`normandy`. This placement comes from pinned primary homelab source:
`docs/transition/phase6/2026-10-03-agent-vms.md:11` records its identity
and SSH carrier; `docs/transition/phase6/2026-10-03-agent-vlan.md:66`
places the coordinator on Normandy. Reconfirm actual placement and runtime
identity before deployment; these source records are not fresh live clearance.

[homelab-steward-monitoring.patch](homelab-steward-monitoring.patch) targets
homelab-infra commit `73a6e441ee8c49be9c7b5ba4471c32cc754e5034`.
It has not been applied to that repository or its active PR3 owner branch.
The owner branch advanced during preparation; reconcile the patch against
its reviewed current head rather than replacing newer hardware/UI work.

The patch changes only these two paths:

- `inventory/host_vars/normandy.yml`: override the inherited extra-scrape
  list with the identical Gatus entry plus exactly one
  `{name: steward, job: steward, address: '127.0.0.1:9469', interval: '30s'}`
  entry. Ansible replaces lists; retaining Gatus explicitly prevents losing
  the existing group-level target. No other host receives a Steward scrape.
- `roles/observability_grafana/files/dashboards/agents.json`: change only
  the five existing native query expressions. Match `up == 1` and
  `steward_metrics_snapshot_fresh == 1` on `job,instance,host` before
  aggregation. Preserve the panel definitions, descriptions, layouts,
  datasource, and all other expressions. Empty or failed producer evidence
  remains unavailable; no healthy-zero fallback is added.

Reuse the existing generic loop in
`roles/observability_collector/templates/config.alloy.j2` and the
`observability_extra_scrapes` default. Alloy supplies the host identity
and uses its existing authenticated remote-write path. No remote scrape,
firewall opening, credential, or new collector service is required.
If the inherited scrape list changes before owner integration, reconcile
the Normandy override so it retains every accepted existing entry.

The producer opt-in is the existing staged configuration:
`backlog_v2.coordinator.metrics.bind: "127.0.0.1:9469"`.
Its default remains disabled. Config lifecycle and leader/attempt continuity
belong to the owning Steward integration; this document does not enable it.

## Native producer reconciliation with quota integration

Retain the single literal-loopback listener and separate existing-file
SQLite `mode=ro/query_only` connection. Snapshot refresh uses bounded
SELECTs in one read transaction, a 5-second load deadline, and an immutable
30-second cache. HTTP performs no durable reads or admission reconciliation.
Failed refresh retains the previous complete snapshot and signals failure;
after 90 seconds the endpoint returns 503 without operational gauges.
Close the listener and refresh loop before closing the query connection.

The reconciled rendering uses the native conservative merge,
`NewestQuotaStates`, `QuotaObservationMaxAge`, and `QuotaReadingFresh`.
Configured coverage reuses `backlog.QuotaBridgePools` with
`coordinatorQuotaPoolBindings` and is copied for the cache lifecycle.
The bounded query projection preserves acquisition provenance and selects
the authenticated worker_id column internally. Never export account, source,
acquisition, worker, task, run, or thread IDs as labels. An account UUID or
worker ID alone cannot qualify a collector source.

Qualified collector observations use their explicitly budgeted scope age;
anonymous, local, unqualified, and legacy observations retain the existing
60-second age. Keep quota acquisition time unchanged across refresh/cache/
forwarding. The 90-second exporter-cache age is a different limit and must
not widen quota freshness. Count each fresh bucket key once and retain the worst valid tied percentage.
Diagnostic stale/invalid key counts may overlap usable competitors. Partial
model/batch coverage cannot be described as healthy admission; individual
fresh ratios remain observed readings and admission counts remain authoritative
records. Configurable model windows, including weekly_fable, use the fixed
other label. Metrics are not pool capacity or an admission authorization signal.

Preserve the 17 bounded gauge families and their retained-record semantics.
`steward_token_aggregation_supported` remains zero: bounded evidence
coverage is not consumed tokens. Normalized fleet consumption/performance
is the separate plan milestone 2 and requires an agreed native aggregation
contract. Preserve current hardware, HA, retained Influx/history, log
redaction/forwarding, relay, other dashboards, and aggregate capability links.

## Validation and remaining operating acceptance

Preparation validated YAML/JSON structure, retained inherited Gatus, exactly
one Steward target, and rendering of the exact existing generic Jinja scrape
loop with synthetic local variables. It checked that only five native query
expressions change and that task health/freshness gating precedes `sum`.
Read-only `git apply --check --whitespace=error-all` passed against both
`73a6e441ee8c49be9c7b5ba4471c32cc754e5034` and the separately checked
owner snapshot `20ef4e9e292a49dddc0d36915165ba79eac0c07c`. Both source
checkouts remained unchanged. The delta is six added Normandy variable lines
and five query-expression replacements. This is offline source/template
validation, not an Ansible host check, PromQL engine evaluation, ingestion
proof, or Grafana visual acceptance.

After source integration, run the plan's targeted race checks and the full
owning test/lint/qualification gates. Add scoped-age, provenance round-trip,
conservative tie, required coverage, outage/stale, and old-peer fixtures to
the reconciled producer tests. Preserve SQLite write rejection, live WAL
visibility, bounded read/cache behavior, lifecycle/disable cleanup,
privacy, and snapshot-bound failure tests. Require exact final source CI
and independent Astra review in this root session.

Owner-approved deployment remains subject to
[t3-fork-retirement.md](t3-fork-retirement.md)'s separate source/release/
deployment gates, full target drift and holds/timers review, immutable
rollback custody, and fresh idle canary proof. No release, hold change,
campaign, binary replacement, or Ansible convergence occurred here.

In the accepted deployment window, stage and validate the loopback opt-in;
preview only the owning coordinator's collector configuration through the
existing Ansible playbook. Preserve all other targets and services. Deploy
only after owner review of that preview. Then retain dated evidence for:

1. `/metrics` 200, unknown routes 404, non-GET 405, load failure signal,
   90-second stale 503, read-only/WAL behavior, and disable cleanup.
2. Exactly one healthy `job="steward",host="normandy"` target, unchanged
   other scrapes, and advancing snapshots through existing authenticated
   ingestion. Test failure/unavailable handling without ordinary work loss.
3. Representative task/assignment/admission/worker observations compared
   with authoritative native records; quota observations consistent with
   source-qualified freshness and restrictive evidence.
4. The existing private dashboard's populated desktop/mobile panels and
   explicit stale/unavailable behavior. Preserve other dashboards/history
   and public aggregate boundaries; record sanitized evidence.

Live Grafana and monitoring acceptance remain blocked until the accepted
producer, configuration, and release are actually deployed. Claude model
inference remains prohibited until 2026-10-15 09:00 America/Los_Angeles;
monitoring acceptance itself does not require model inference.
