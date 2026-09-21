# Consultations pre-feature numeric baseline receipt

Source under measurement: `3c42e45746618cdaf1970161e43fd7d40dc21eb9`.
The measurement harness is the next commit. Host and toolchain:
`Linux 7.2.3-arch1-3 x86_64`, `linux/amd64`, CGO enabled,
`go1.27.0-X:nodwarf5`, `GOMAXPROCS=12`.

Commands:

```text
go test ./internal/store/sqlite -run 'TestConsultations(PrefeatureParkWakeMeasurements|RegressionBaseline)$' -count=1 -v
go test ./cmd/t3-steward -run '^TestConsultationsPrefeature(CoordinatorMeasurements|IdleCoordinatorRSS)$' -count=1 -v
```

The durable-path test prepares 40 independently migrated SQLite stores before
timing. Each sample performs one task-wait registration, one settlement, and one
wake, using fixed request identities. The 40 samples contain 120 timed store
operations.

```json
{"fixture_creation_timed":false,"go_version":"go1.27.0-X:nodwarf5","gomaxprocs":12,"goroutines_after":42,"host_loadavg":"3.59 1.46 0.89 3/2347 824205","mallocs_delta":13575,"median_ns":318903,"operations_per_sample":3,"p95_ns":437100,"samples":40,"schema":"consultations-prefeature-park-wake-v1","throughput_operations_per_second":9280.444595165738,"timed_operations":120,"total_alloc_bytes_delta":1063432}
```

The coordinator test prepares 30 independently migrated SQLite stores before
timing. Every sample runs one existing coordinator planning/supervision boundary
with one ordinary ready attempt on a disjoint worker and quota pool, plus one
supervised run ready to dispatch an overseer. It verifies that both assignments
are offered. The measured cycle uses the real SQLite planner and supervision
stores; fixture materialization, worker exchange, schedule/admin/legacy fakes,
and workflow projection are outside this bounded measurement.

The instrumentation wraps modernc's connector only in this test. It counts
driver statement executions, measures Exec calls and the complete lifetime of
Query rows, and counts calls to Begin or BeginTx. The counters reset after
migration and fixture construction and immediately before each timed Tick.
All 30 cycles executed exactly 162 statements and began exactly 21
transactions; this stability makes both exact-count comparators.

```json
{"coordinator_passes_per_sample":1,"fixture_creation_sql_counted":false,"fixture_creation_timed":false,"go_version":"go1.27.0-X:nodwarf5","gomaxprocs":12,"host_loadavg":"0.55 2.56 2.83 3/2603 914073","mallocs_delta":169762,"median_ns":2256491,"median_sql_execution_ns":1569698,"ordinary_ready_per_sample":1,"p95_ns":2813718,"p95_sql_execution_ns":2012444,"samples":30,"schema":"consultations-prefeature-coordinator-v2","sql_statement_count_max":162,"sql_statement_count_median":162,"sql_statement_count_min":162,"sql_timing_scope":"driver Exec plus complete Query rows lifetime","supervised_runs_per_sample":1,"throughput_passes_per_second":436.53517722687945,"total_alloc_bytes_delta":17716240,"transaction_count_max":21,"transaction_count_median":21,"transaction_count_min":21,"transaction_counting_scope":"successful or failed driver Begin/BeginTx calls"}
```

The idle RSS comparator launches five independent dedicated test processes. In
each child the same ordinary-ready plus supervision fixture completes one warm
coordinator pass. The child forces garbage collection, releases free OS memory,
waits 250 ms, reports readiness, and then blocks without an in-flight Tick while
the coordinator cycle remains reachable. The parent reads that child's
`/proc/<pid>/status` `VmRSS`. Thus the number is the RSS of the dedicated
quiescent coordinator harness process, including its Go runtime and one SQLite
store; it is not an active multi-fixture test-process delta.

```json
{"fixture":"one warmed ordinary-ready plus supervision coordinator cycle","go_version":"go1.27.0-X:nodwarf5","gomaxprocs":12,"host_loadavg":"0.55 2.56 2.83 1/2602 914181","maximum_rss_kb":27576,"measurement_source":"/proc/<child-pid>/status VmRSS","measurement_state":"dedicated child process, coordinator quiescent, no in-flight Tick","median_rss_kb":27076,"rss_kb_samples":[26932,26956,27076,27344,27576],"samples":5,"schema":"consultations-prefeature-idle-coordinator-rss-v1"}
```

The allocation fields are Go process allocation deltas over the timed
coordinator region. Compare feature revisions by running the identical commands
on the same host and toolchain. The pre-feature path contains no consultation
answer records or advisor sessions; the feature revision must additionally
measure its mixed-work integration seam.
