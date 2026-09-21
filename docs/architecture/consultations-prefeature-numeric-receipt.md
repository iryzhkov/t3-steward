# Consultations pre-feature numeric baseline receipt

Source under measurement: `e22eb6fed87dbb67521be48c8818b2d4c2f8d828`.
The measurement harness is the next commit. Host and toolchain:
`Linux 7.2.3-arch1-3 x86_64`, `linux/amd64`, CGO enabled,
`go1.27.0-X:nodwarf5`, `GOMAXPROCS=12`.

Commands:

```text
go test ./internal/store/sqlite -run 'TestConsultations(PrefeatureParkWakeMeasurements|RegressionBaseline)$' -count=1 -v
go test ./cmd/t3-steward -run TestConsultationsPrefeatureCoordinatorMeasurements -count=1 -v
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

```json
{"coordinator_passes_per_sample":1,"fixture_creation_timed":false,"go_version":"go1.27.0-X:nodwarf5","gomaxprocs":12,"host_loadavg":"11.00 6.87 3.21 6/2363 875446","mallocs_delta":165079,"median_ns":2257310,"ordinary_ready_per_sample":1,"p95_ns":2828228,"process_rss_after_kb":76200,"process_rss_before_kb":73296,"samples":30,"schema":"consultations-prefeature-coordinator-v1","sql_statement_count":null,"supervised_runs_per_sample":1,"throughput_passes_per_second":428.37024142775465,"total_alloc_bytes_delta":17380504,"transaction_count":null}
```

Both receipts are in-process measurements and record contemporaneous host load.
The allocation fields are Go process allocation deltas over the timed region.
The RSS values are whole test-process snapshots around the 30 coordinator
passes, not retained heap or idle coordinator RSS. The SQLite wrapper exposes no
statement or transaction trace hook, so those counts are explicitly `null`;
adding instrumentation would change the source under measurement.

These values establish the pre-feature ordinary-ready plus supervision path and
the durable park/settle/wake path. There are no consultation answer records or
advisor sessions in this source, so no consultation work is represented. A
feature revision should run these identical tests on the same host and toolchain
and add its mixed-work measurement at the new integration seam.
