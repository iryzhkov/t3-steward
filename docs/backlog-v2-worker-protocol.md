# Backlog-v2 worker exchange protocol

Status: S15 production-binding contract. The protocol and SSH transport foundation
are implemented and tested locally, but they are not composed into the production
coordinator or a worker runtime.

## Transport decision

Retain coordinator-initiated SSH for the first production worker runtime. The
bounded local multi-process spike demonstrates the properties required by the
production-binding plan:

- OpenSSH is invoked without a shell, with a validated destination and fixed
  remote command, BatchMode enabled, strict host-key checking, and a bounded
  connect timeout.
- A single request and response use stdin/stdout. Both directions are byte
  bounded; stderr has its own smaller bound.
- The request context and the envelope deadline bound the child process.
  Cancellation terminates the local SSH process and therefore the SSH channel.
- SSH authenticates the host and login identity. The versioned envelope
  additionally binds the authenticated principal, key ID, sender, recipient,
  coordinator and worker epochs, session, sequence, request ID, deadline, payload
  checksum,
  and an HMAC-SHA-256 signature.
- Content-addressed execution packages and artifact manifests travel as protocol
  payloads. Artifact bytes may use a separately bounded SSH stream in the worker
  runtime without granting the worker coordinator database access.
- Lost responses retry the same immutable signed request. The receiver returns
  the cached response for an exact duplicate and never reruns the handler.

This is evidence for the transport foundation, not authorization to contact a
fleet worker. The spike uses only child copies of the Go test binary and
disposable in-memory state. S16 must bind the authenticated SSH principal from
the restricted worker command, persist worker-side execution state, and keep the
remote command incapable of arbitrary shell execution. If that binding cannot
preserve these constraints, the implementation must stop and replace the
foundation with mutually authenticated HTTP rather than weakening the contract.

## Envelope

Protocol version 1 uses one strict JSON object per exchange. Unknown JSON fields,
multiple values, malformed JSON, unsupported versions, and messages above the
configured byte limit fail closed.

Every envelope contains:

- a message type and protocol version;
- sender and recipient identities;
- coordinator and worker epochs;
- session ID and monotonically increasing sequence;
- request ID and, for responses, inReplyTo;
- UTC send time and deadline;
- SHA-256 of the exact payload bytes;
- authenticated principal, key ID, and HMAC-SHA-256 signature;
- the typed JSON payload.

The signature covers the complete envelope with only the signature value cleared.
Payload mutation, identity mutation, or deadline mutation therefore invalidates
authentication. Clock skew is bounded. An expired request is never handled.

## Messages and authority

The stable message kinds are:

| Kind | Direction | Meaning |
| --- | --- | --- |
| capabilities | both | Negotiate supported versions, capabilities, and byte limits. |
| snapshot / observations | worker to coordinator | Publish worker inventory and assignment observations. |
| offers | coordinator to worker | Offer an assignment with its immutable execution package. |
| claims | worker to coordinator | Claim offers using assignment and lease identities. |
| lease-renewals | worker to coordinator | Renew exact epoch-bound leases. |
| commands | coordinator to worker | Deliver durable prepare, dispatch, stop, or collect intent. |
| acknowledgements | worker to coordinator | Acknowledge command acceptance idempotently. |
| artifact-upload | worker to coordinator | Request transfer into coordinator custody. |
| artifact-download | coordinator to worker | Authorize selected immutable inputs. |
| error | both | Return a stable structured failure. |

Authorization is an allowlist per authenticated principal. A valid signature does
not grant access to an unlisted message kind. Workers report observations and
execute intent; they do not perform coordinator state transitions. An
acknowledgement proves command acceptance, not completion of an external effect.

Stable error codes distinguish unsupported version, stale epoch,
authentication, authorization, replay, reordering, malformed input, limits,
timeout, cancellation, backpressure, and internal failure. Retryability and a
bounded retry delay are explicit.

## Ordering, replay, retry, and pressure

A new session begins at sequence 1. The receiver reserves the next sequence
before invoking the handler, so a later request cannot overtake accepted work.
A lower, skipped, or otherwise unexpected sequence is rejected as reordered.

Request IDs are scoped to the authenticated principal:

- an exact completed duplicate returns the cached signed response;
- an exact duplicate while the first handler is active receives retryable
  backpressure;
- reuse with different signed content is a replay error;
- the response cache is bounded;
- transport retry uses the same request ID, sequence, payload, and signature.

The receiver has a bounded in-flight limit. Excess work receives retryable
backpressure and is not accepted. Transport backoff is bounded exponential
backoff and stops on non-retryable protocol errors or context cancellation.

## Immutable execution package

Execution package version 1 carries everything the assigned worker needs without
reading coordinator SQLite:

- coordinator, worker, workflow, run, task, attempt, assignment, dispatch, and
  deterministic thread identities plus their epochs;
- task class and the exact content-addressed prompt;
- static inputs and dependency artifacts with safe materialization paths;
- the fixed provider route, model, quota pool, and options;
- the catalog revision and resolved project, repository, ref, workspace scope,
  setup profile, T3 project, resource locks, and credential names;
- verification commands and declared output names/media types;
- not-before, deadline, expiry, creation time, turn, preparation, verification,
  per-artifact, and aggregate byte limits.

Credential values are forbidden from the package. The package is canonical JSON
inside a media-typed manifest containing exact size and SHA-256. Every custody
change validates the package again and verifies its content address. The JSON
golden is the compatibility contract for field names and encoding.

## Artifact transfer and custody

An artifact object has an immutable ID, safe relative materialization path, kind,
media type, exact size, SHA-256, and optional tar archive declaration. A
versioned transfer manifest binds its direction, coordinator/worker/assignment
epochs, objects, aggregate size, and expiry.

Validation rejects absolute paths, traversal, backslashes, duplicate IDs or
paths, unsupported archive formats, size overflow, and checksum mismatch.
Tar inspection is bounded by entry count and expanded bytes and permits only
regular files and directories. Symlinks, hard links, devices, unsafe paths,
duplicates, malformed headers, and truncation fail before extraction.

A custody record binds manifest/object, source, destination, sequence, verified
size/hash/time, and the prior custody-record hash. Its own SHA-256 makes
tampering detectable. Coordinator publication remains the authoritative custody
transition; transfer alone does not release dependencies.

## Compatibility and next binding

Version negotiation is explicit and version 1 accepts no unknown fields.
Changing field meaning or encoding requires a new version; adding an optional
field still requires updating the JSON goldens and compatibility review.

S16 owns the restart-safe worker implementation behind this contract. S17 owns
coordinator composition. Until then the production daemon remains closed and
constructs no SSH transport, worker process, or T3 client.
