# H1 ADR: one coordinator-admin transport, two carriers

Status: accepted. Freezes the contract before implementation.
Date: 2026-09-14
Authority: the dogfood failures of the UpKeeper Go-migration campaign, where an agent on
omarchy-pc could not submit a campaign to the coordinator on normandy without opening an SSH
shell and running `t3-steward` there by hand.

## Scenario

Every coordinator-admin operation reaches the coordinator through one owner-only Unix socket
at `<state path>.admin.sock`. A CLI on a non-coordinator host dials its own nonexistent copy
of that path and fails with `dial unix ...: connect: no such file or directory`, which reads
like a broken installation rather than like "this host is not the coordinator". The documented
workaround was `ssh normandy t3-steward ...`, which teaches agents to run an arbitrary remote
shell command as the coordinator's owner. That is a worse authority story than the one the
worker protocol already solved.

The worker side does not have this problem. `t3-steward worker-exchange` is a restricted SSH
endpoint: one operation word as its only argument, identity taken from the worker's own
configuration file, a signed and bounded envelope, durable replay protection. This ADR gives
the admin side the same shape rather than inventing a second one.

## Decision

Introduce `CoordinatorAdminTransport`: one interface that carries every coordinator-admin
operation, with two implementations behind it.

The interface is the union of the six narrow service interfaces the CLI already depends on
(`adminQueryService`, `adminMutationService`, `adminArtifactService`, `adminSubmissionService`,
`adminScheduleDefinitionService`, `adminRecoveryService`) plus the three operations that are
currently string literals reached through their own inline clients: node wait, graph amendment
and worker enrollment. `backlogadmin.LocalClient` satisfies it as it stands.

The local carrier is unchanged. It stays owner-only, keeps its peer-UID principal, keeps the
`local-admin` role, and remains the only transport a coordinator-local client uses.

The remote carrier is SSH invoking a new restricted forced command, `t3-steward
coordinator-exchange <operation>`, built as a sibling of `worker-exchange`: one operation word,
no other arguments, `--config` required and explicitly operator-controlled, `SSH_ORIGINAL_COMMAND`
never read and never forwarded to a shell.

### Selection

Transport selection happens in exactly one place, the client constructor in `runCoordinatorAdmin`.
The three inline `LocalClient` builders (campaign submission, node wait, worker enrollment) are
folded into it. A host selects the remote carrier when its configuration declares a
`backlog_v2.coordinator_client` block and the local coordinator role is absent; a coordinator-local
client continues to select the socket. Selection is reported by every command that talks to the
coordinator, so an agent never has to guess which coordinator answered.

## Authority

Admin authority and worker authority stay separate, in credentials and in roles.

| Concern | Local carrier | Remote carrier |
| --- | --- | --- |
| Principal source | kernel peer UID (`SO_PEERCRED`) | verified envelope signature over the client's admin credential |
| Role granted | `local-admin` | `remote-admin` |
| Credential | none; file mode 0600 is the gate | `secretref:` reference resolved on both ends, never inline |
| Claimed principal in the request | overwritten by the server | overwritten by the server |
| Worker credentials | never accepted | never accepted |

A worker credential must never authorize an admin operation and an admin credential must never
authorize worker execution. The credential namespaces are disjoint: workers keep
`secretref:f02-protocol/<host>`, admin clients get `secretref:f03-admin/<client>`. The
`remote-admin` role exists so that the authorizer is not a rubber stamp: it is granted the
operations an agent needs (query, submission, schedule definition, node wait, artifact read,
and the mutations that were already admin commands) and it is refused anything that would let a
remote client rewrite the coordinator's own identity or epoch.

The server overwrites the principal in both carriers. The request's `Principal` field is
advisory and is never trusted, exactly as it is not trusted today.

## Envelope, bounds and replay

The remote carrier reuses the existing `localRequest`/`localResponse` operation envelope, so
both carriers speak one operation vocabulary and the artifact and submission byte streams keep
their current shape (declared size in the frame, raw bytes after it). What it adds around that
envelope is the `workerproto` security frame: version, session, request ID, sequence, sent-at,
deadline, payload digest and HMAC-SHA256 authentication.

Bounds come from configuration on both ends and are stated in the refusal when they disagree,
rather than surfacing as a decode error. Replay protection is durable, reusing the worker
protocol's replay store shape: a request ID is answered once, a repeated request ID with a
matching digest returns the first answer, and a repeated request ID with a different digest is
refused. The digest covers the operation envelope and, for a submission, a digest of the archive
bytes, so the same idempotency key carrying different content is refused rather than answered
from the cache.

The transport store is a shield, not the guarantee. It closes the window in which a lost
response would be re-executed, and it is defeated by a handler killed between the effect and the
cache write: no answer was stored, the abandoned identity is reclaimed after two minutes, and
the operation runs again. What makes that safe is the service, where the idempotency key, the
revision fence and the audit record live. "Lose the response and retry with the same idempotency
key produces exactly one workflow run" is a property of the submission service; the transport
makes the common case cheap and the uncommon case visible.

Idempotency keys, revision fences and audit records therefore stay where they are, in the
service. The transport does not get its own copy of them.

## Result taxonomy and exit codes

Today every failure prints `error: ...` and exits 1. Both carriers now classify, and the
classification is shared, so an agent can branch on it without parsing prose:

| Class | Meaning | Exit |
| --- | --- | --- |
| `ok` | the coordinator answered | 0 |
| `client-configuration` | this host cannot form a request (missing or invalid client block, bad socket path, non-positive limits) | 3 |
| `authentication` | the coordinator refused the principal or the signature | 4 |
| `unavailable` | no coordinator answered (socket absent, SSH connect failure, DNS) | 5 |
| `timeout` | a request deadline expired with no answer | 6 |
| `protocol` | version or limit mismatch, malformed frame | 7 |
| `rejected` | the coordinator answered and refused the request | 8 |

Exit 1 remains the generic failure for everything that is not transport-classified, so existing
automation that only checks non-zero keeps working. `--json` output carries the same class in a
versioned envelope.

A read-only `t3-steward coordinator identity` command reports which coordinator answered, its
owner, release, configuration digest, epoch, health and the carrier used. It needs no new request
type: `Query{Kind: QueryStatus}` already returns all of it. It exists so that an agent can prove
where its next submission will go before it submits.

## Help is part of the contract

The normal path an agent is taught is `t3-steward campaign submit <dir>`. No help text, example
or error message may teach `ssh <coordinator> t3-steward ...`. When a command fails because this
host has no coordinator client configured, the error names the configuration block to add, not a
remote shell to run.

## Alternatives rejected

Forwarding `SSH_ORIGINAL_COMMAND` to a shell was rejected for the reason `worker-exchange`
rejected it: it makes the coordinator's owner account a remote shell for anyone holding the key,
and no bound on the request can fix that.

A TCP or HTTP admin listener was rejected. It would need its own transport security, its own
listener lifecycle and its own firewall story, while SSH already gives host verification, key
management and a working restricted-command mechanism the fleet has deployed.

Trusting the kernel UID on the remote side, by having the forced command run as the coordinator
owner and skipping the signature, was rejected: then possession of any key in that account's
`authorized_keys` is full admin authority, and the credential rotation story disappears.

A long-lived multiplexed admin connection was rejected for now. One SSH session per request is
the cost `worker-exchange` already pays; a persistent admin channel is an optimization that can
be added behind the same interface once the security properties are settled.

## Explicitly not in this work

No second scheduler, no quota policy change, no artifact retention change, no new manifest
schema, no credential brokerage beyond resolving an existing reference, and no automatic
generation of `authorized_keys` entries: packaging documents the forced-command line, the
operator installs it.
