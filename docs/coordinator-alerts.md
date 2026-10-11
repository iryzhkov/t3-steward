# Coordinator alert endpoint

Steward exposes monitoring signals independently of homelab monitoring. The HTTP
listener is off by default. Enable it on the coordinator:

```yaml
backlog_v2:
  coordinator:
    alerts:
      bind: "127.0.0.1:9080"
      min_free_mb: 5120
```

Traefik fronts this endpoint for Gatus or Grafana polling. Bind to the interface
Traefik can reach. The endpoint has no authentication and needs no repository
secret. Changing these coordinator settings requires a restart.

Only GET and HEAD are served; other methods return 405 with Allow: GET, HEAD.
HEAD has the same status as GET with no response body.

- /healthz returns 200 when a store read succeeds within two seconds, otherwise 503.
- /alerts always returns 200 with the steward-alerts/v1 JSON report and all five
  signals. The report and each signal include observedAt in UTC.
- /alerts/NAME returns one signal, with 200 for ok or unavailable and 503 for
  firing or unknown. Unknown signal names and paths return 404.

Each signal has state, detail and observedAt. Coordinator is ok when its bounded
schema-version read succeeds and firing on a failure or timeout. Disk checks
available space on the filesystem holding the configured coordinator state
directory and the latest workspace_free_mb and temp_free_mb reported by every
enrolled worker. The default minimum is 5120 MiB; zero selects that default and a
negative value is invalid. Space below the minimum fires; exactly the minimum
is ok. A known low reading fires even if another reading is missing. Otherwise
missing local or enrolled-worker readings, or a failed telemetry read, are
unknown. Worker observations are the latest durable reports, without an
additional freshness cutoff. Signal observedAt is the evaluation time.

Litestream is unavailable with detail "Litestream is not deployed (unit 1.16)".
Stuck_attempt and quota are unavailable with detail "not implemented yet" until
unit 1.19a-2. This endpoint does not configure Traefik, Gatus, Grafana or Litestream.

Responses contain only monitoring status and disk counts and sizes. Store errors,
worker inventory, prompts, outputs, credentials and filesystem paths are never
included. Requests cannot change coordinator state.
