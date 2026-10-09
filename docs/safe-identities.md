# Path-safe coordinator identities

New reruns, clones, graph additions, scheduled attempts and recovery retries use
a kind prefix and 32 lowercase hexadecimal characters. Each is deterministic;
the coordinator preserves replayed results rather than deriving them again.

| Family | Previous identity | New identity |
| --- | --- | --- |
| Rerun run | `run:rerun:<key>` | `run-<32 hex>` |
| Rerun task | `task:rerun:<key>:<index>` | `task-<32 hex>` |
| Rerun input | `input:rerun:<key>:<index>` | `input-<32 hex>` |
| Corrected rerun prompt | `input:rerun:<key>:prompt` | `input-<32 hex>` |
| Clone run | `run:clone:<key>` | `run-<32 hex>` |
| Clone task | `task:clone:<key>:<index>` | `task-<32 hex>` |
| Clone input | `input:clone:<key>:<index>` | `input-<32 hex>` |
| Added graph task | `task:graph:<key>` | `task-<32 hex>` |
| Graph prompt | `input:graph:<key>` | `input-<32 hex>` |
| First rerun, clone or graph attempt | `attempt:<task>:1` | `attempt-<32 hex>` |
| Scheduled attempt (timer or manual) | `attempt:<run>:<task>:1` | `attempt-<32 hex>` |
| Recovery retry | `attempt:recovery:<24 hex>` | `attempt-<32 hex>` |

The shared `domain.DerivedID(namespace, kind, parts...)` helper hashes
`t3-steward/derived-id/v1\0<namespace>\0<kind>\0<parts joined by \0>`
with SHA-256, takes the first 16 bytes, encodes them as lowercase hex and adds
`<kind>-`. The prefix separates this scheme from submitted runs and other
coordinator identity schemes. The result has 128 bits of digest and matches
`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`.

Rerun wrappers use namespace `rerun`; clone wrappers use `clone`; graph task
and prompt wrappers use `graph`. Run parts are the request key; task and input
parts are the key and decimal index. The corrected rerun prompt uses key and
`prompt`; a graph task or prompt uses the request key alone. First attempts use
their family's namespace with kind `attempt` and parts task ID and `1`.
Scheduled attempts use namespace `schedule`, kind `attempt`, and parts run
ID, task ID and `1`. Recovery retries use namespace `recovery`, kind
`attempt`, and the full payload digest. Run/task/attempt collisions still fail
the transaction; inserts do not silently replace existing rows.

To map a rerun key to its run, read the command's `runId` JSON field or its
first `run <id>` output line. The graph's `rerunOf.idempotencyKey` preserves
the key; clone and graph amendment request IDs preserve their corresponding
keys. Consumers should read stored IDs rather than identify runs by a prefix.

Existing records keep their IDs, including colons. No migration or backfill is
performed. Quote old IDs in shell commands. Replaying an old request returns
the original IDs unchanged, and old runs remain readable and usable as rerun
or clone sources. Their new descendants receive safe identities; references
to the source keep the source identity.

Coordinator-only sink IDs (`sink:<run>`) remain derived on read and are never
worker paths. Pin owners (`rerun:<run>`, `clone:<run>`, `wait:<id>`,
`edge:<run>`) and audit/event/lock identities remain colon-shaped database
keys or lock names. Changing these would alter existing ownership or sink
relationships.

## Where the rules are enforced

`domain.PathSafeID` is enforced, not only tested: a rerun or clone refuses to
store a run, task, input or first-attempt ID that does not match it, before
anything is written, and an authored campaign commit name must match it too.

A cross-run reference `<run>/<task>` is parsed by `domain.ParseNodeRef`, which
holds the run part to the storage-component rule
(`domain.ValidateStorageComponent`): not empty, `.` or `..`, no `/`, `\` or
control character, not starting with `-`, at most 255 bytes. The rule is
looser than `PathSafeID` on purpose, so that old colon run IDs remain valid
references. Output paths and `inputs_from` artifact names in a manifest or a
`--output` flag must have no control character and no component longer than
255 bytes. Submission idempotency keys must be 1 to 256 trimmed bytes with no
control characters (`domain.ValidateIdempotencyKey`); the client and the
coordinator refuse with the same message.

New run and task IDs pass wake-summary identity checks. Old colon run IDs still
report `unsafe-identity` in wake summaries; this change does not modify those
validators. Workers and older clients consume the new IDs as ordinary strings.
