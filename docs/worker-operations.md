# Persistent worker operations

The persistent worker connection is opt-in. Existing workers without a
`connection` setting retain the restricted one-shot protocol.

A worker reads `~/.config/t3-steward/worker-bootstrap.json`, distributed by
UpKeeper's worker-configuration component. It must implement schema 1 of the
U0 distribution contract. The bootstrap contains worker/coordinator identity,
Git/Huyang capabilities, allowed provider instance names, SSH transport, and
`secretref:f02-protocol/<worker-id>`. Its reported bootstrap digest matches
UpKeeper's sorted, indented JSON plus trailing newline encoding.

Provision the reference as a private 0600 regular file under
`~/.config/upkeeper/secrets/f02-protocol/<worker-id>`. It contains the same
protocol credential JSON used by the restricted environment resolver. The
coordinator also needs the corresponding reference. Secret values are never
part of the release manifest, catalog, enrollment response, or audit event.

Install the released binary through UpKeeper. Install the supplied
`packaging/systemd/t3-steward-worker.service` as a user unit and enable it.
The unit reads a dedicated `~/.config/t3-steward/persistent-worker.yaml`:

```yaml
policy:
  dry_run: false
backlog_v2:
  mode: disabled
log_level: info
```

The `worker serve` command owns execution; `backlog_v2.mode: disabled` prevents
that file from also starting a coordinator if used with `run`. Add host-specific
`t3` connection settings when automatic discovery is insufficient. This separate
file lets enrolled workers execute while the normal host watchdog retains its
own dry-run policy. Keep it private (0600). The unit includes mise shims on PATH.
The worker independently reconciles retained execution every five seconds. An exclusive
socket lock prevents two daemons from owning the runtime; a crash leaves a
socket that the next lock owner can replace.

Configure `backlog_v2.workers.<id>.connection: persistent-ssh` and use the
verified SSH alias as its address. The fixed remote command is
`.local/bin/t3-steward worker bridge`; it forwards opaque frames to the local
worker socket. `connection: unix` uses the address as a local socket path,
with the same signed protocol. Neither transport grants coordinator admin
authority.

Persistent connections require message limits at most 8 MiB, artifact transfers
at most 16 MiB, request timeouts at most two minutes, and snapshot freshness at
most five minutes. The coordinator supplies a scoped execution catalog over
the authenticated connection. Catalog and execution sessions have independent
replay sequences. Lost responses retry the identical signed intent.

Inspect `t3-steward worker list --json` for configured hosts, accepted catalog
digests, snapshots and enrollment state. Enroll through the coordinator's local
admin socket:

```text
t3-steward worker enroll <worker-id> --request-id <stable-id> \
  --catalog-revision <digest> --expected-revision 0 --reason "<reason>"
```

Enrollment observes current worker readiness and records the actor, worker
epoch, principal, credential reference, connection and accepted snapshot.
A configured worker cannot receive new assignment offers until enrollment
matches the effective requirement. Exact request replay is idempotent; changing
its actor or body is rejected.

## Resource-aware placement

Workers include live telemetry in each inventory: one- and five-minute load,
CPU count, available memory, swap usage, free space on the workspace and temp
filesystems, and active (preparing, running or resuming) attempts. Linux reads
/proc and statfs; missing fields on other platforms are unknown. The temp
filesystem is the worker service's own TMPDIR, falling back to /tmp, and it
must hold a task's scratch need plus the disk reserve. A worker whose /tmp is
small therefore never receives `build` tasks until its service sets TMPDIR to a
larger filesystem, even if the tasks themselves write temporary files elsewhere.

Placement keeps enrollment, capabilities, CPU class, executor capacity and slots
as hard constraints. Fresh telemetry additionally rejects a worker when memory
available is below task memory plus reserve, either filesystem is below task
scratch plus reserve, or swap exceeds the configured limit. Remaining workers
rank by normalized CPU and memory headroom before existing preference scores.
CPU headroom is `(cores - max(load1, load5) - task CPU units) / cores`,
clamped to [-1, 1]. Memory headroom is
`(available - task memory - reserve) / (available + task memory + reserve)`.
The configured weights combine these measures; existing preference scores break
headroom ties.
Missing, partial and stale telemetry rank after complete fresh telemetry;
unknown readings alone do not exclude a worker. Telemetry is fresh while its
timestamp is within `telemetry_max_age` of the coordinator's clock in either
direction, so a worker clock that leads the coordinator by less than that bound
does not make its readings stale. Because any fresh, complete worker ranks ahead
of any worker without such telemetry regardless of load, and CPU load never
excludes a worker, an overloaded upgraded worker can be preferred over an idle
worker that does not yet report telemetry until that worker is upgraded.

Each offer cycle adds the expected needs of assignments already proposed in
that cycle to the proposed worker's load, memory and disk readings, preventing
a burst from repeatedly using the same idle snapshot. This holds for unsized
tasks too: a task that declares no size and no CPU class floor is expected to
use the coordinator's nominal unsized needs.

Coordinator defaults can be adjusted under
`backlog_v2.coordinator.resource_placement`:

```yaml
telemetry_max_age: 2m
memory_reserve_mb: 1024
disk_reserve_mb: 2048
max_swap_used_mb: 4096
cpu_weight: 1
memory_weight: 1
unsized_task_cpu_units: 1
unsized_task_memory_mb: 1024
unsized_task_scratch_mb: 0
```

Task resource presets supply expected CPU share, memory and scratch needs for
the live telemetry floors, ranking and in-cycle reservation: `light` expects
0.25 CPU units, 256 MiB memory and 512 MiB scratch; `build` expects 2 CPU
units, 4096 MiB memory and 8192 MiB scratch. Build is intended for race tests
and full review gates. Ingestion records the declared preset name on the task,
and the expected needs follow that name, not the CPU classes. A build with
`min_cpu_class: high` or `preferred_cpu_class: medium` therefore keeps build's
memory and scratch needs. A task that declares classes without a preset, such
as a bare `min_cpu_class: medium` or even build's own medium-and-high pair,
uses the nominal unsized needs. Explicit `cpu_units`, `memory_mb` and
`scratch_mb` override the expected needs per field. `campaign check` sends the
preset name with each task so its live floors match placement. A review member's execution profile
records its preset name too, and the member task inherits it. Tasks and review
profiles stored before the preset name was recorded have none, so they use the
nominal unsized needs unless they declare explicit sizes.

With the defaults an unsized task therefore needs 2048 MiB of available memory
(1024 MiB nominal need plus the 1024 MiB reserve). Setting all three
`unsized_task_*` values to 0 removes the in-cycle reservation for unsized tasks,
so a burst of them again fills the worker that looked idlest.

A preset never reserves configured executor capacity. Only explicit
`cpu_units`, `memory_mb` and `scratch_mb` are checked against a worker's
configured `executors.cpu_units`, `memory_mb` and `scratch_mb`, so a
fleet-managed worker that configures executor slots alone keeps receiving
`light` and `build` tasks. A task with explicit sizes still needs a worker that
configures those capacity dimensions.

`backlog explain` and `campaign check` report the telemetry used, resource
rejections, headroom score, ranking and selected worker. Explain preserves the
assignment's recorded decision after dispatch; queued work and campaign check
evaluate current snapshots, so a later report may change as load changes.
Check is read-only and does not reserve resources: it evaluates each task
independently, so it can name the same worker for every task of a burst that
the planner will spread across workers within one cycle.

## Advertised capabilities and campaign supervision

A worker advertises two kinds of capability in one list. The configured kind
describes the host, such as `git`, `huyang` and `preflight`, and comes from the
bootstrap file. The build kind describes the binary running on that host, and is
appended by the worker itself, because a coordinator reading its own
configuration cannot know which release the far end is running.

`campaign-supervision-v1` is a build capability. It says that this worker
understands a campaign supervision activation: that it can run an overseer
activation as ordinary assigned work and report the resulting turn without the
coordinator treating that turn as a task result. Every worker built from this
release advertises it automatically; nothing has to be configured, and it cannot
be configured on for a build that does not implement it.

The practical consequence is a deployment ordering rule. A supervised campaign
is only admitted when at least one eligible worker advertises the capability, so
upgrade the workers before submitting supervised work. A fleet one release
behind advertises `git`, `huyang`, `preflight` and
`task-wait-collection-fence-v1` and nothing else, and
`t3-steward campaign check` reports `impossible` for a supervised campaign
against it, which is a refusal at admission rather than a stall after it. A
worker that is downgraded while a supervised run is live is not sent the
activation either: the worker exchange withholds the offer and leaves the
assignment outstanding, so the same activation is offered again once the worker
is back on a capable release. Unsupervised campaigns are unaffected and require
no capability at all.

## Turns that end while their commands still run

Ending a turn completes a task, so a turn that ends while a command the task
started is still running (typically a long gate started with `nohup ... &`)
would be collected with uncommitted work and missing outputs. When an attempt's
turn ends with no task-bound wait, the worker first looks for such commands.

On Linux it reads `/proc`. For a process whose working directory is inside the
attempt workspace, the worker follows its parents while they are inside too, and
the process outside the workspace that holds the topmost of them decides:

- pid 1 or an ancestor of the worker, which is where the kernel puts a command
  whose starting shell has exited (`systemd --user` on a fleet host): the
  command is detached and counts;
- a command shell (`sh -c`, `bash -c` and the like): a command started from
  outside the workspace counts;
- any other live process, such as the T3 server: the topmost process is the
  provider session. The provider, its MCP servers and their language servers
  run in the workspace for the whole session and do not count. A tool execution
  the provider itself started does, because that is a command-tool call the
  provider still tracks, such as a Claude Code `run_in_background` command or a
  Codex command. A child of the provider is a tool execution when it leads a
  session of its own, which Claude Code and Codex give every command they run
  while MCP servers stay in the provider's session; the session survives a
  `bash -lc '<command>'` shell replacing itself with the command. A command
  shell or a sandbox wrapper (`bwrap`, `codex-linux-sandbox`) the provider
  started counts too. The command below the shells and wrappers is reported.

The worker's own processes, such as verification commands, are excluded. A
provider started through a command shell would make its whole session look like
a command; T3 starts providers directly. A command a provider ran directly in
the provider's own session, without a shell or a sandbox wrapper, would look
like an MCP server; neither provider runs commands that way. Conversely, a
provider that started infrastructure in a session of its own (Node's `detached`
spawn) would have it reported; neither provider does today, and the follow-up
turn budget bounds the cost.

If such commands are found, the attempt is not collected. The worker sends one
follow-up turn to the same session naming the commands and telling it to wait
for them in the foreground and then finish. At most two follow-up turns are sent
per attempt; each is claimed in the worker journal before it is sent and carries
T3 identities derived from the dispatch token and the ended turn, so a worker
restart never sends a second one for the same turn. A third turn that ends the
same way fails the attempt with the reason `live-children-at-turn-end`, followed
by the process list, which declared outputs are present or missing, and what was
retained.

Whenever an attempt of a task that declares a commit fails with uncommitted
changes in its tree (the runtime's `.t3` directory aside), whether for live
commands, a missing declared output, a failed verification or a worker-side
failure, the worker snapshots them as a commit on the private ref
`refs/steward/wip/<attempt>` and uploads a bundle of it from the workspace base
as the result artifact `wip.bundle`, naming it in the failure text; the bundle
stays in the attempt directory on the worker too. A failed attempt does not
publish its declared commit, so this is how its work is recovered. A clean tree
is not snapshotted.

The snapshot runs Git on the worker host, outside any containment the attempt
had, so it never reads the task's repository configuration: it runs in a
private Git directory the worker writes, with no hooks, attributes, system or
global configuration, and a minimal environment. HEAD, the index, the ignore
rules and the workspace's own objects are copied in as data; symbolic links
in the object store are skipped, so no object from outside the workspace
reaches the bundle. Filters the task's attributes name are therefore not
applied: a file the task's index records as unchanged is kept as the index
has it, and a changed one as it is on disk (a changed Git LFS file is kept
whole rather than as a pointer). The private ref lives only in the bundle,
not in the workspace. A workspace whose `.git` or object store is not its own
directory, whose objects come from another repository through alternates, or
whose ignore rules cannot be read is not snapshotted; the failure says so.
Recover a bundle in a clone that has the base commit with
`git fetch wip.bundle refs/steward/wip/<attempt>:refs/heads/<name>`.

`backlog explain` (as a `turn end:` detail) and `backlog task show` (as a
`turn end:` line) show the state, for example `waiting for 2 background
commands: sh -c make check-review ..., sleep 300 (nudge 1 of 2)`, once the worker
advertises the build capability `turn-end-commands-v1` and the coordinator asks
for it. A host without `/proc` (macOS) collects the turn as before and reports
that the check did not run, rather than failing a task it cannot judge; explain
keeps showing that warning after the attempt is terminal.
Contained executions are not inspected: their provider runs in its own pid and
mount namespaces, and the supervisor stops the whole sandbox at collection.

Upgrading the worker binary is a lifecycle operation of its own: install the new
release, then restart `t3-steward-worker.service`, or the host keeps serving the
previous release against the new coordinator. `t3-steward worker inspect-journal`
reports, read-only, how many attempts the durable journal still owns and how many
of them are dispatched, so a restart can wait for dispatched work to settle.

A release may derive catalog digests differently from the release that wrote the
worker's retained catalog. The worker then starts, serves no execution, logs
"retained catalog is unusable; awaiting republication", and waits for the
coordinator to publish a catalog again; the coordinator's expected revision does
not fence that republication. The drain guard still refuses a catalog change
while the journal owns unsettled execution, so recovery never discards custody.

To drain a persistent worker, set its coordinator `accept_backlog: false` and
send SIGHUP to the coordinator process. Draining leaves execution identity and
retained packages unchanged while closing new admission. The worker continues
reconciling existing custody. Once assignments are settled, change the catalog,
reload, inspect its new digest, and enroll the new revision before reopening
admission.

SIGHUP validates the complete configuration before replacing configuration-bound
services under the existing coordinator epoch. Only backlog_v2 catalog and policy
settings are reloadable. Identity, epochs, storage, and host watchdog/T3 settings
are lifecycle operations. Invalid configuration retains the prior effective
configuration. Changing an execution catalog while its worker has nonterminal
assignments is rejected; drain and settle first, or cancel the task the receipt
names. A failed activation restores the prior configuration. Every signal is
answered with a receipt at
`~/.local/state/t3-steward/coordinator/reload-receipt.json` (`accepted`,
`unchanged` or `rejected` with the blockers), carried by the status query as
`lastReloadReceipt` and by `t3-steward coordinator identity`; on the coordinator host
`t3-steward coordinator reload` sends the signal and prints the receipt. Status
also exposes the effective configuration digest, release, and activation time;
successful activation has a native audit event. See "Reloading the coordinator"
in [backlog-v2-operations.md](backlog-v2-operations.md).

Rotate credential values in their private files on both ends, then reconnect.
A same-catalog handshake refreshes authentication without changing worker epoch
or execution identity. Old keys fail authentication. Changing a credential
reference changes enrollment identity and requires bootstrap convergence and
new enrollment.

The Track S4 stage remains incomplete until the Citadel stage document contains
release CI, remote enrollment and live task/restart/disconnection/lease evidence.

S5a directory execution is still guarded during integration. Worker construction
now supplies a contained T3 attachment adapter backed by the worker journal's
`contained` subdirectory. Preparation must record the exact assignment, launch,
supervisor invocation and authenticated T3 environment before attachment is
possible. Receipt publication is durable and refuses replacement. Recovery only
observes that receipt and service; it never launches a replacement. Every API
request checks the supervisor invocation, and Linux connections and token reads
reopen the recorded control directory identity. Socket connections pin the socket
inode rather than following a provider-controlled pathname. Unsupported platforms
fail closed. Missing, incomplete, stopped or changed identities cannot fall back
to shared T3. Lease expiry is not a stop condition.

This adapter does not yet enable directory tasks: production preparation still
needs provider settings, contained verification, output custody and confirmed
supervisor stop integration. No normal directory task is qualified by the adapter
unit tests alone.
