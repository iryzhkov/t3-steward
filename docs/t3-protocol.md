# T3 Code protocol notes

These notes record what the watchdog relies on inside T3 Code, verified
against the source of the tested version. T3's control protocol is internal
and undocumented; every item here can change in a future T3 release, which
is why the watchdog gates control actions on a tested version range
(`internal/compat`).

Tested T3 versions: **0.0.38** (server package `t3` on npm, repository
`pingdotgg/t3code`).

## What the watchdog uses

| Need | Mechanism | Where in T3 |
| --- | --- | --- |
| Quota events | Provider event log files | `apps/server/src/provider/Layers/EventNdjsonLogger.ts` |
| Server version and capabilities | `GET /.well-known/t3/environment` (no auth) | `packages/contracts/src/environmentHttp.ts` |
| Bearer token | `t3 auth session issue --token-only` | `apps/server/src/cli/auth.ts` |
| Thread list and state | `GET /api/orchestration/shell` | `packages/contracts/src/orchestration.ts` (`OrchestrationShellSnapshot`) |
| Thread detail | `GET /api/orchestration/threads/:id?turnLimit=N` | same |
| Warn, drain, resume | `POST /api/orchestration/dispatch` with `thread.turn.start` | same |
| Stop | `POST /api/orchestration/dispatch` with `thread.turn.interrupt` or `thread.session.stop` | same |

The WebSocket RPC (`/ws`, Effect RPC over JSON text frames) is not used.
The rate-limit event is not delivered over it: T3 declares
`account.rate-limits.updated` in its runtime-event vocabulary and both the
Claude and Codex adapters emit it, but the orchestration ingestion ignores
it. It is neither projected into any snapshot nor exposed by any RPC. Its
only durable landing place is the provider event log. Polling the HTTP shell
snapshot every few seconds is enough for thread state, and it avoids a
dependency on the RPC wire format.

## Authentication

- `t3 auth session issue --token-only --ttl 60m --label t3-steward`
  writes a bearer session directly into T3's own database
  (`userdata/state.sqlite`) and prints the token. The session carries the
  administrative scope set, including `orchestration:read` and
  `orchestration:operate`. Default TTL when unspecified is 30 days. The
  watchdog requests one hour and refreshes at 80% of the TTL, so nothing
  long-lived is stored.
- Requests carry `Authorization: Bearer <token>`. A 401 triggers one token
  refresh and retry.
- `GET /api/auth/session` describes the current session and its scopes.
- The alternative flows (one-time pairing token exchanged at `POST
  /oauth/token`, form-urlencoded RFC 8693 token exchange; WebSocket tickets
  from `POST /api/auth/websocket-ticket`) are not needed for a local
  watchdog. A static token can be configured for remote servers.

## Server discovery

The server writes `<data_dir>/userdata/server-runtime.json` on start and
removes it on a clean shutdown:

```json
{"version":1,"pid":736866,"host":"0.0.0.0","port":7391,"origin":"http://127.0.0.1:7391","startedAt":"..."}
```

`origin` is the base URL; a wildcard bind is recorded as `127.0.0.1`. The
data directory is `--base-dir`, else `T3CODE_HOME`, else `~/.t3` on every
operating system (no XDG, no `Library/Application Support`, no `%APPDATA%`).

## Provider event log

Location: `<data_dir>/userdata/logs/provider/events.<thread-id>.log`, one
file per thread, plus `events._global.log` for events without a thread.

Line format:

```text
[<ISO timestamp>] CANON: <json>
[<ISO timestamp>] NTIVE: <json>
```

`CANON` lines are the normalized `ProviderRuntimeEvent`; `NTIVE` lines are
the raw provider-protocol messages. The watchdog only decodes `CANON` lines
whose JSON contains `"type":"account.rate-limits.updated"`.

Rotation is by rename: when a file exceeds 10 MiB it becomes
`events.<id>.log.1` (older backups shift to `.2` ... `.10`) and a fresh file
is created. Retention deletes files older than 14 days and keeps the
directory under 512 MiB. Records are batched (about one flush per second)
but a record is never split across two writes, so complete lines can be
relied upon. The tailer keys its position on inode plus offset and drains
the renamed file before starting on the new one.

### Codex payload

`payload.rateLimits` is the app-server notification envelope, which is
itself `{rateLimits: snapshot}`, so the snapshot sits at
`payload.rateLimits.rateLimits`:

```json
{"limitId":"codex","limitName":null,"planType":"plus",
 "primary":{"resetsAt":1788898977,"usedPercent":47,"windowDurationMins":300},
 "secondary":{"resetsAt":1789485777,"usedPercent":7,"windowDurationMins":10080},
 "credits":{"balance":"0","hasCredits":false,"unlimited":false},
 "individualLimit":null,"rateLimitReachedType":null,"spendControlReached":null}
```

Every field is optional: updates are sparse and an absent window means "no
change". `usedPercent` is 0..100, `resetsAt` is unix seconds. The watchdog
maps `primary` and `secondary` to buckets of the same name and
`individualLimit` (a spend control with `remainingPercent`) to a `spending`
bucket. `limitId` is the bucket's limit identity, falling back to
`limitName`, then `codex`.

### Claude payload

`payload.rateLimits` is the whole Claude Agent SDK `rate_limit_event`
message; the data is under `rate_limit_info`:

```json
{"status":"allowed","resetsAt":1788547200,"rateLimitType":"five_hour",
 "overageStatus":"rejected","isUsingOverage":false,
 "unifiedWindows":{"five_hour":{"utilization":0.88,"resetsAt":1788547200},
                   "seven_day":{"utilization":0.15,"resetsAt":1789056000},
                   "seven_day_overage_included":{"utilization":0.02,"resetsAt":1789056000}}}
```

Current Claude Code builds report every window in `unifiedWindows` with
`utilization` as a 0..1 fraction (observed values such as `0.88`). The SDK
type definition that ships with T3 0.0.38 predates `unifiedWindows` and
describes only the flat `rateLimitType` / `utilization` / `resetsAt`
fields, with `utilization` documented as 0..100. The watchdog handles both:

- With `unifiedWindows`, each key becomes a bucket window. Values are
  treated as fractions unless any value exceeds 1, in which case the whole
  map is read as percentages.
- Without it, the single flat window named by `rateLimitType` is used with
  `utilization` as a percentage.
- `status: "rejected"` forces the named window to 100%.

Window names seen: `five_hour`, `seven_day`, `seven_day_overage_included`.
The SDK enumerates `seven_day_opus`, `seven_day_sonnet` and `overage` as
well. A suffix after `five_hour_` or `seven_day_` that is not
`overage_included` or `oauth_apps` is treated as a model selector: a
`seven_day_opus` bucket affects only threads whose selected model id
contains `opus`. Windows containing `overage` are recorded but never act
(configurable through `policy.ignore_windows`).

Claude reports no account identifier, so all Claude windows of one provider
instance are one account. See the README's single-account note.

## Thread state

The shell snapshot (`GET /api/orchestration/shell`) lists every thread
without message bodies. Fields used:

- `modelSelection.instanceId` (provider instance id, the routing key) and
  `modelSelection.model`
- `latestTurn.turnId` and `latestTurn.state` in `running | interrupted |
  completed | error`
- `session.status` in `idle | starting | running | ready | interrupted |
  stopped | error`
- `backgroundLiveness` in `working | monitoring | null`: native background
  work (subagents, workflows) alive after the turn settled
- `latestUserMessageAt`, `hasPendingApprovals`, `hasPendingUserInput`,
  `archivedAt`, `updatedAt`

A thread is "running" for the watchdog when the latest turn is running or
`backgroundLiveness` is `working`. Session status is consulted only before a
latest turn is available; a terminal latest turn is authoritative over a
reusable provider session that remains `starting` or `running`.

T3 has no parent/child thread relation. Subagents and workflows exist only
inside one thread, as provider-runtime tasks reported through thread
activities. There is no command to stop one subagent; the drain message
asks the coordinating agent to wind them down, and the hard stop interrupts
the whole thread, which ends its subagents with it.

## Commands

All commands go to `POST /api/orchestration/dispatch` as a JSON body with a
`commandId` (UUID) and ISO `createdAt`.

Create a thread in a steward-prepared checkout:

```json
{"type":"thread.create","commandId":"...","threadId":"...","projectId":"...",
 "title":"...","modelSelection":{...},"runtimeMode":"full-access",
 "interactionMode":"default","branch":"main",
 "worktreePath":"/absolute/path/to/prepared/workspace","createdAt":"..."}
```

In the tested contract, `branch` and `worktreePath` are nullable strings. The
control adapter sends legacy calls as explicit `null` values and sends supplied
values unchanged. A hermetic HTTP integration test verifies that an isolated
endpoint receives the absolute prepared checkout path, branch, and the same
caller-selected `threadId` in both `thread.create` and the following
`thread.turn.start`. A rejected create or lost HTTP response returns that stable
thread ID to the scheduler and does not remove the prepared checkout, so it can
be reconciled instead of dispatched under a new identity. The adapter does not
own workspace cleanup.

This test exercises request serialization and failure ownership without
contacting a live T3 daemon. Acceptance by a newly supported T3 release must
still be checked against that release's contracts as part of the version
verification checklist.

Send a message (warn, drain, resume):

```json
{"type":"thread.turn.start","commandId":"...","threadId":"...",
 "message":{"messageId":"...","role":"user","text":"...","attachments":[]},
 "modelSelection":{...thread's own...},"runtimeMode":"full-access",
 "interactionMode":"default","createdAt":"..."}
```

For a running Claude thread the server steers the live turn: the message is
queued into the SDK agent loop and the turn continues. For a running Codex
thread the server issues a `turn/start` to the app server. In the live test
the Codex agent received both the warn and the drain message mid-turn and
stopped at a checkpoint on its own.

Interrupt the running turn:

```json
{"type":"thread.turn.interrupt","commandId":"...","threadId":"...","turnId":"...","createdAt":"..."}
```

Stop the provider session:

```json
{"type":"thread.session.stop","commandId":"...","threadId":"...","createdAt":"..."}
```

After an interrupt the thread's `latestTurn.state` becomes `interrupted`
and `session.status` returns to `ready`; the turn id does not change. A
later user message starts a new turn with a new id, which is how the
watchdog detects manual interaction after its own stop.

## Verification checklist for a new T3 version

1. `t3-steward check` passes (descriptor, token, shell snapshot).
2. A provider log line for each provider parses (`replay` on a copied log).
3. On a disposable thread with `dry_run: false`: inject 86% and confirm the
   warning arrives as a user message; inject 91% and confirm the drain
   message; inject 96% (or let the grace period expire) and confirm
   `latestTurn.state` becomes `interrupted`; inject a reset snapshot below
   50% and confirm the resume prompt starts a new turn.
4. Update `MinServerVersion` / `MaxServerVersion` in `internal/compat` and
   the README table.

Injecting events for a test: point `t3.data_dir` at a scratch directory
with `userdata/logs/provider/`, set `t3.url` explicitly, and append lines in
the format above.
