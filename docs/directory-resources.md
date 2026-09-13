# Directory identity diagnostics

Existing-directory execution is not enabled yet. This increment provides local
identity inspection and access-conflict primitives for its implementation.
It does not register a resource, authorize a capsule, mount data, or isolate T3.

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
operators are outside this primitive. Scheduler persistence, package binding,
resource enrollment, containment and writer lifecycle integration are still
required before these checks can authorize existing-directory tasks.

A local namespace probe demonstrated denied direct/symlink/child writes and a
separate writable output directory. It did not run an AI provider. T3 owns the
provider processes; wrapping only Steward's preparation process is insufficient.
A contained provider lane must also handle network access and prevent host-side
brokers from granting write access outside the namespace. No production service
isolation is changed by this increment.
