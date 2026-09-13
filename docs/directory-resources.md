# Directory identity diagnostics

Existing-directory execution is not enabled yet. Identity inspection, operator
catalog configuration, capsule resource resolution and retained planner ownership
are implemented. Mounting data and containing actual T3 provider processes remain
required; bound packages are refused before provider or workspace effects.

On Linux, create an operator candidate registration JSON:

```json
{"workerId":"homelab","resourceId":"datasets/photos","revision":"1","path":"/srv/photos","writable":false}
```

Inspect it without modifying the source:

```sh
t3-steward worker inspect-directory --registration candidate.json > identity.json
t3-steward worker inspect-directory --registration candidate.json --expected identity.json
```

Paths must be absolute and canonical; symlinks in any component are refused.
Missing directories are never created. Inspection opens each component relative
to a pinned parent descriptor and records device, inode, birth time, mount ID,
and ancestor identities. Kernels/filesystems without required statx evidence
and non-Linux platforms fail closed. A mount change, source replacement,
registration revision change, or replaced ancestor fails revalidation.
Normal writes inside the directory do not invalidate its identity.

Runtime integration must retain the validated descriptor until a descriptor
bind mount has been created. A path recheck followed by a path-based mount would
reintroduce a replacement race. Snapshot JSON alone cannot pin an object.

Access defaults to read-only; writes require both an explicit request and the
operator writable flag. Conflict comparison excludes overlapping readers when
either access is writable, including canonical ancestors and inode aliases.
Unmanaged external writers and mutations to mount topology by privileged host
operators are outside this primitive. The backend now persists resolved bindings on tasks, authorizes them against
project catalogs, includes them in execution-package hashes and reconstructs
ownership for the production planner from assignment records. Reader/writer
conflicts also reserve within a scheduling pass. Cancelled tasks and expired
leases retain directory ownership until the assignment confirms completion or
release; missing assignment evidence retains uncertainty. Empty bindings preserve
legacy package hashes. Provider containment and writer lifecycle qualification
remain required before directory tasks run.
The local driver explicitly refuses bound directory packages before effects.

A local namespace probe demonstrated denied direct/symlink/child writes and a
separate writable output directory. It did not run an AI provider. T3 owns the
provider processes; wrapping only Steward's preparation process is insufficient.
A contained provider lane must also handle network access and prevent host-side
brokers from granting write access outside the namespace. No production service
isolation is changed by this increment.

## Operator catalog and capsule requests

Under `backlog_v2.projects.<project>.directory_resources`, the operator supplies
a list of bindings: `{identity: <complete inspect-directory JSON>, access: read-only}`.
JSON is valid YAML; identity keys retain the diagnostic JSON spelling. Use the
complete inspected identity, including ancestor evidence. An approved write binding
requires both `access: read-write` and `registration.writable: true`. Its worker
must appear in the project's eligible workers. Configuration never inspects or
creates the source on the coordinator.

Each worker catalog includes only that worker's approved resources. Changes to
identity, access or registration revision change its catalog fence. Changes to a
different worker's resources do not invalidate this worker's catalog.

A task in a fresh, task-scoped workflow can name resources:

```yaml
directories:
  - worker: homelab
    resource: datasets/photos
    revision: '1'
    # access defaults to read-only
```

Requests cannot contain host paths or inode evidence. All requests for one task
must name the same worker, consistent with task placement. Submission resolves
each name and revision against that project's operator catalog before creating
bundle storage. Missing registration, stale revision and access escalation are
rejected. The resolved evidence is persisted with the task and reauthorized
against the worker catalog during package construction. Accepted idempotent
replays retain their original evidence; they do not gain a new authorization.

This is a configuration/submission interface for staged integration, not a live
execution qualification. Do not submit production directory tasks yet. Attachment
mount destinations, direct existing-directory cwd, owned output handling and
provider containment still need runtime integration and live validation.
