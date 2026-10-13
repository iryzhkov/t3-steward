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
Quota uses the native freshest-observation merge, admits only fresh workers and
buckets, and reports the largest observed used percentage per fixed window
category. It never adds percentages across accounts or runs admission
reconciliation. Unrecognized states/windows collapse to other, bounding labels.
The ratio is a conservative fleet summary, not a particular pool's capacity.

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
