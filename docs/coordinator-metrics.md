# Native coordinator metrics

Disabled by default. A reviewed coordinator configuration may opt in with:

```yaml
backlog_v2:
  coordinator:
    metrics:
      bind: "127.0.0.1:9469"
```

Only a literal loopback IP is accepted; there is no public listener, authentication
credential, task/run/thread/account/host label, or CLI parsing. An Alloy instance
on the same coordinator host can scrape /metrics. Binding is part of coordinator
configuration lifecycle; this source change does not enable or deploy it.

A separate native mode=ro/query_only SQLite connection avoids the coordinator's
writer connection and sees live WAL commits; SQLite itself refuses writes.
A SELECT-only read transaction refreshes an immutable cache every30s with
a5s deadline. HTTP requests read only this cache. A failed refresh preserves the
last successful snapshot and marks refresh_success=0; after90s /metrics returns
503 with snapshot_fresh=0 rather than serving stale operational values. Unknown
routes return404 and non-GET metrics requests return405. Snapshot load bounds
are10000 workers and buckets; an exceeded bound fails the entire refresh.

All series are gauges. Tasks count the latest retained declared attempt per
run/task, excluding taskless supervision activations; retries are not extra
tasks. Assignments and admission states count retained records. Worker freshness
requires connected=true, unexpired validUntil and configured observation age.
Quota uses domain.MergeQuotaObservations and NewestQuotaStates, retaining
equally timed competitors. A key counts once among fresh observations, with
the worst valid percentage among its fresh competitors. Stale and invalid
diagnostics count unique keys with excluded competitors; these diagnostics
can overlap a fresh key when another competitor remains usable.

The query projection retains the authenticated worker_id column internally.
Native merge replaces claimed origins with that identity and limits remote
origin validity. Immutable configured coverage comes from the same pure
QuotaBridgePools projection as admission. QuotaObservationMaxAge extends age
only for a unique source/account/window/origin-qualified collector pool.
Provider instance IDs alone never select coverage; ambiguous qualified scopes
receive no extension, and an unrelated known account cannot clamp legacy age;
unqualified readings in collector pools retain at most60s. QuotaReadingFresh
also excludes future/zero readings, expired origin validity, and ended resets.
The existing fallback governs legacy pools. Acquisition time is never restamped
by metrics refresh, and the90s exporter cache limit is independent of source age.

Ratios report observed windows, including valid windows in a partial acquisition.
They do not claim that required coverage is complete or that admission is healthy;
admission gauges remain retained authoritative records. The largest observed
used percentage per fixed window never adds percentages across accounts.
Configured model windows, including weekly_fable, map to the fixed other label
along with other unrecognized windows; qualified observations are retained,
and exact model/source/account names never become labels. Unknown states also
collapse to other. The ratio is a conservative fleet summary, not pool capacity.
No scrape or refresh runs admission reconciliation.

Token aggregation is explicitly unsupported (steward_token_aggregation_supported
is0). usage_evidence_rows counts only the latest10000 native evidence rows,
ordered by observation time, after excluding a local copy when its authenticated
forwarded copy is present in that same bounded window. A copy outside the window
cannot be deduplicated against it; the explicit truncated gauge marks excluded
history, and oldest_timestamp identifies the window. Database worker/event keys
deduplicate forwarding retries. Call and turn evidence may overlap; these counts
must not be converted to consumed tokens. usage_field_coverage_rows separates
measured-field presence from absent numeric evidence. No token values are loaded.
The bound prevents metadata-only usage scans from delaying operational snapshots.

Follow-up before exposing token totals: add a bounded fleet-native aggregation
contract to the existing measured-usage store, preserving the rules implemented
by domain.NormalizeUsageReport: exact causal call/turn supersession, cumulative
incarnation/sequence reset handling, field presence, diagnostic coverage,
authoritative dispatch attribution and local/forwarded deduplication. Existing
AttributedUsage is per-run; an empty run returns unattributed coverage only.
Expose measured retained/window totals as gauges alongside partial/stale/
unsupported coverage, and omit unknown values. Never raw-sum usage_samples,
estimate token counts from quota percentages, or expose task/thread identities.

Publication and runtime deployment are separate. No current coordinator,
worker, campaign, configuration or UpKeeper hold is modified by this preparation.
