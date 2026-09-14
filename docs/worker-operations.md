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
assignments is rejected; drain and settle first. A failed activation restores
the prior configuration. Status exposes the effective configuration digest,
release, and last activation time; successful activation has a native audit event.

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
