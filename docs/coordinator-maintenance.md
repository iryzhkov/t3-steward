# Coordinator maintenance

Create an online backup while the coordinator and its workers continue running:

```sh
t3-steward coordinator backup --config /path/to/operator-config.yaml --out ./snapshots/before-upgrade
t3-steward coordinator backup verify ./snapshots/before-upgrade
t3-steward coordinator backup verify ./snapshots/before-upgrade --restore-drill
```

Creation is an operator-only local filesystem command. It requires an explicit
operator-controlled configuration in coordinator mode, refuses a configured
remote coordinator client, and exposes no remote admin mutation. The restricted
coordinator-exchange endpoint cannot create or restore backups. The snapshot
destination must be absent and must not overlap the database or artifact roots.
Relative paths resolve against the current working directory. Quote `"~/backup"`
when needed; `~` and `~/` expand to the invoking account's home. Other users'
tilde forms are refused with an absolute-path remedy.

SQLite `VACUUM INTO` captures one consistent committed database image, including
committed WAL pages, without coordinator ownership or schema migration. The
snapshot contains that image and the immutable artifact objects referenced by
its metadata, with sizes and SHA-256 digests in `manifest.json`. Uncommitted
uploads and unrelated objects are excluded. Submission objects from the configured
bundle root are included under the same content-addressed artifact tree. This
flattening makes drills independent of the original roots. A production restore
that uses separate submission and artifact roots must also place submission
objects in its configured bundle root; the older `backlog backup restore`
command restores the one artifact tree only. Online maintenance does not automate
production rollback or copy the operator's configuration and credentials.

A copy fails without publishing if retained bytes are missing, their content
differs from their metadata, or concurrent pruning removes a needed object before
it can be opened. Retry after pruning settles. Limits protect against runaway
snapshots (one million files, one TiB); ensure the destination filesystem has
space for the database and retained objects. Staging is private in the destination
parent. The command validates the staged image and digests before a single rename
publishes the snapshot, and refuses an existing destination.

Verification reads no production configuration. A restore drill copies into an
owner-only temporary directory, accepts the backup's recorded coordinator identity,
opens only the scratch database read-only, verifies schema and integrity, and
reports `coordinatorId`, `schemaVersion` and `counts` (runs, tasks, attempts,
assignments, worker snapshots, artifacts, schedules and audit events). It never
starts a coordinator, advances an epoch, migrates schema, or contacts a worker.
Scratch state is removed before the command returns. A successful drill verifies
the persisted image; it does not prove that external work can be undone.

After a planned restart, use a readiness deadline instead of an immediate
single health check:

```sh
t3-steward coordinator health --wait-ready --timeout 90s --json
```

The read-only command uses the same local or remote admin transport as
`coordinator identity`. JSON reports epoch, release, health, connected, expected
and missing worker IDs, and `ready`. A worker counts as connected only with a
fresh connected snapshot from the reported epoch. Expected workers come from
the coordinator's worker view, including configured workers that have never
connected; removed workers are excluded, draining workers still count.
Without `--wait-ready` it prints one observation. With the flag it retries
temporary unavailability once per second until healthy and all expected workers
are connected. Authentication, configuration and protocol errors fail immediately.
The default timeout is 60 seconds. Exit 6 means readiness timed out; JSON then
contains the last observation with `ready: false`, or unavailable if no status
was obtained. Re-run after correcting the reported fault.

Coordinator startup serves local administration first, then contacts workers
after one second. Failed worker exchanges retry with exponential backoff capped
at five seconds, independently of the ordinary scheduling interval. Each retry
still passes the existing coordinator boundary gates. A slow exchange or boundary
can add its own execution time; the cap is on retry delay.

On clean stop, the coordinator truncates its WAL and closes the database before
releasing its ownership fence. With no other readers, SQLite removes the WAL and
shared-memory sidecars. An unrelated process holding the database open can retain
empty sidecars or block checkpointing; shutdown returns a checkpoint error in
that case. A persistent worker on the coordinator host skips both local watchdog
quota and usage database reads whenever its configuration is in coordinator mode,
even while drained. A worker-mode configuration pointing at the same database
also skips these reads when the persistent `.coordinator.lock` ownership marker
exists, including while the coordinator is stopped. Coordinator quota admission
still applies to its work.

The older stopped `backlog backup create|verify|restore` workflow remains
available for operator rollback procedures and retains its stopped-state guards.
