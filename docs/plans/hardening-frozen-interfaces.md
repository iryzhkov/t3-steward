# Hardening: frozen interfaces and fixtures

This file is the lead's contract freeze for the H1-H5 work. Parallel implementations must use
exactly these names, shapes and strings so that the three branches integrate without a rename
pass. Anything not named here is the implementer's choice.

The ADRs are the reasoning: `adr-h1-coordinator-admin-transport.md`,
`adr-h3-live-campaign-readiness.md`, `adr-h4-wait-aware-task-lifecycle.md`,
`adr-h5-recovery-provenance-and-evidence.md`, and UpKeeper's
`adr-h2-steward-fleet-configuration.md`.

## H1 transport (owner: A)

Correction, 2026-09-14: the sketch below was written from a reconnaissance summary and names
several types that do not exist (`Action`, `ArtifactRequest`, `ArtifactStream`,
`ScheduleDefinitionRequest`, `NodeWaitRequest`, `AmendmentRequest`, `WorkerEnrollmentRequest`),
with return types that contradict the real methods. The binding rule is the prose, not the
sketch: the interface is the union of the existing client methods with their signatures preserved
verbatim, so that `LocalClient` satisfies it unchanged and no caller is rewritten.

```go
// package backlogadmin, illustrative grouping only; real signatures win
type CoordinatorAdminTransport interface {
    Query(ctx context.Context, request Query) (Response, error)
    Mutate(ctx context.Context, action Action) (Response, error)
    RecoverUnknown(ctx context.Context, request UnknownRecoveryRequest) (Response, error)
    PutSchedule(ctx context.Context, request ScheduleDefinitionRequest) (Response, error)
    SubmitArchive(ctx context.Context, request LocalSubmissionRequest, body io.Reader, size int64) (LocalSubmissionResponse, error)
    OpenArtifact(ctx context.Context, request ArtifactRequest) (ArtifactStream, error)
    NodeWait(ctx context.Context, request NodeWaitRequest) (Response, error)
    AmendGraph(ctx context.Context, request AmendmentRequest) (Response, error)
    EnrollWorker(ctx context.Context, request WorkerEnrollmentRequest) (Response, error)
    Describe() TransportDescription // carrier, coordinator id, endpoint; no credentials
}
```

`LocalClient` satisfies it. `SSHClient` is the second implementation. Existing method signatures
are preserved verbatim; only the grouping is new.

Classification, shared by both carriers:

```go
type TransportClass string
const (
    ClassOK                  TransportClass = "ok"
    ClassClientConfiguration TransportClass = "client-configuration" // exit 3
    ClassAuthentication      TransportClass = "authentication"       // exit 4
    ClassUnavailable         TransportClass = "unavailable"          // exit 5
    ClassTimeout             TransportClass = "timeout"              // exit 6
    ClassProtocol            TransportClass = "protocol"             // exit 7
    ClassRejected            TransportClass = "rejected"             // exit 8
)
type TransportError struct { Class TransportClass; Operation string; Coordinator string; Err error }
```

Unclassified failures keep exit 1. `--json` errors are `{"version":"backlog.admin/v1",
"kind":"error","class":"...","operation":"...","message":"..."}`.

Forced command: `t3-steward coordinator-exchange <operation>`, operations `query`, `mutation`,
`artifact`, `submission`, `schedule-definition`, `unknown-recovery`, `node-wait`,
`graph-amendment`, `worker-enrollment`. Built as a sibling of `worker-exchange`: one positional
operation, `--config` required and explicit, `SSH_ORIGINAL_COMMAND` never read.

Configuration, new block on `BacklogV2`:

```yaml
backlog_v2:
  coordinator_client:
    coordinator_id: normandy-coordinator
    address: normandy              # ssh destination or alias
    connection: ssh
    remote_command: t3-steward
    credential: secretref:f03-admin/omarchy-pc
    request_timeout: 30s
    message_limits: {max_bytes: 4194304, max_artifact_bytes: 1073741824}
```

Amendment, 2026-09-14: the same settings must also load from an UpKeeper-owned file,
`~/.config/t3-steward/coordinator-client.json`, mode 0600, `schema_version` 1, read the way
`workerruntime.LoadWorkerBootstrap` reads `worker-bootstrap.json`, with unknown fields refused.
An explicit `backlog_v2.coordinator_client` block in `config.yaml` is an operator override and
wins; the file is used when the block is absent. This exists because a single key written into
`config.yaml` would give that file two authors, which is the drift the fleet-configuration
component exists to remove.

The client bootstrap document is canonical JSON with the closed field set `schema_version`,
`coordinator_id`, `address`, `connection`, `remote_command`, `credential_ref`, `request_timeout`,
`message_limits`. `message_limits` uses t3-steward's own vocabulary, `max_bytes` (default
4194304), `max_files` (default 1000) and `max_artifact_bytes` (default 1073741824), because the
document is consumed as `V2MessageLimits`. Absent means the defaults, never unbounded.

Remote principal role is `remote-admin`; the local peer-UID role stays `local-admin`. The server
overwrites any claimed principal on both carriers.

Read-only command: `t3-steward coordinator identity [--json]`, implemented over
`Query{Kind: QueryStatus}`, reporting coordinator id, owner, release, configuration digest,
epoch, health and the carrier used.

## H3 readiness (owner: C)

New verb `campaign check <dir> [--json] [--task NAME]`. New admin query kind
`QueryViability`. Request carries the projected plan's requirements, never the bundle.

```go
type ViabilityRequest struct { Tasks []ViabilityTask } // project, routes, resources, capabilities,
                                                        // directories, locks, repository, ref, timing
type ViabilityMatrix struct {
    SchemaVersion int                 // 1
    Outcome       ViabilityOutcome    // ready | accepted_waiting | impossible
    Tasks         []ViabilityTaskResult
}
type ViabilityTaskResult struct { Task string; Outcome ViabilityOutcome; Candidates []ViabilityCandidate }
type ViabilityCandidate struct {
    Worker   string
    Outcome  ViabilityOutcome
    Reasons  []ViabilityReason
}
type ViabilityReason struct {
    Code      string // see below
    Permanent bool
    Detail    string
    Desired   string // e.g. desired catalog digest
    Observed  string // e.g. observed catalog digest
    Revision  uint64 // expected revision where relevant
}
```

Reason codes, permanent: `unknown-project`, `unknown-setup-profile`, `unknown-provider-instance`,
`unknown-model`, `unknown-quota-pool`, `worker-not-eligible`, `capability-missing`,
`cpu-class-impossible`, `resources-impossible`, `directory-impossible`, `credential-missing`,
`repository-syntax-invalid`, `repository-authentication-failed`, `repository-not-found`,
`ref-not-found`, `no-configured-route`.

Reason codes, temporary: `quota-closed`, `worker-at-capacity`, `worker-offline`, `worker-stale`,
`network-unavailable`, `dns-failure`, `probe-timeout`, `snapshot-stale`, `lock-held`.

Drift is its own code, `catalog-digest-mismatch`, temporary, and always carries `Desired`,
`Observed` and `Revision`. It must never be reported as `worker-not-eligible`.

Probe: built-in name `git_ls_remote`, argv `git ls-remote --exit-code -- <repository> <ref>`,
no shell, repository validated by `catalog.validateGitRepository`, ref by `catalog.validateGitRef`,
output bounded during accumulation. Evidence key is
`(worker, catalogDigest, repository, ref, credentialRefs)`, TTL 10 minutes.

`campaign submit` runs the check unless `--allow-unverified` is passed; the coordinator repeats
the permanent checks inside `ingest` before any record is written.

### Probe classification, measured

Measured on omarchy-pc with git 2.x on 2026-09-14, `GIT_TERMINAL_PROMPT=0`. The classifier must
be driven by these, not by guesses:

| Case | Exit | Decisive stderr |
| --- | --- | --- |
| reachable, ref present | 0 | the ref line on stdout |
| ref absent (`--exit-code`) | 2 | empty stdout |
| no credentials for a private repository | 128 | `could not read Username for 'https://github.com': terminal prompts disabled` |
| repository absent on Forgejo | 128 | `Forgejo: Cannot find repository: <owner>/<name>` then `Could not read from remote repository.` |
| DNS failure | 128 | `Could not resolve host: <host>` |
| option-shaped repository value | 128 | `fatal: strange pathname '--upload-pack=...' blocked` |

Git's own refusal of an option-shaped pathname is a backstop, not the defence: the value is
rejected by validation before `git` is executed, and the `--` separator is always present.

One host fact worth knowing while testing: HTTPS to GitHub succeeds on these machines because
`~/.config/git/config` delegates `credential.https://github.com.helper` to `gh auth
git-credential`. Remove that helper from the probe's environment when testing the
unauthenticated case, or the test proves nothing.

## H4 lifecycle (owner: C)

```go
// package domain
ProgressWaitingExternal ProgressState = "waiting-external"
ControlWaitingExternal  ControlState  = "waiting-external" // HoldsProviderSlot() == false
TurnOutcomeWaiting      TurnOutcomeMarker = "waiting"
```

`RunExecutionsQuiescent` must treat `ControlWaitingExternal` as **not** quiescent.

Task-bound wait record (coordinator-owned, alongside `coordinator_node_waits`):

```go
type TaskWait struct {
    ID               string
    WorkflowRunID    string
    TaskID           string
    AttemptID        string
    ExpectedRevision uint64   // attempt revision fence
    ThreadID         string   // canonical T3 thread
    Wake             WakeMode // each | all
    MaxDuration      time.Duration
    RequestID        string   // idempotency
}
```

Amendments, 2026-09-14, from the first implementation pass:

- `ExpectedRevision` is `int64`, matching `Attempt.Revision`.
- The record additionally carries `RegisteredRevision`, `RegisteredAt`, `Deadline`, `Result`,
  `SettledAt`, `WokenAt`, `Delivery`, `DeliveredAt`, `Name` and `Condition`. The frozen fields
  above are present verbatim; a record without settlement and delivery state cannot be the
  durable record this contract describes.
- `all` is scoped to the attempt. Mixing `each` and `all` on one attempt is defined as: any
  `each` that settles wakes the attempt.
- Settlement ownership: the steward's existing wait runner reports check outcomes; the
  coordinator owns expiry and wake.
- `wait add --task current` is routed on the literal `current` before native-wait dispatch, so it
  does not collide with `--task <run>/<task>`.
- The worker learns that an assignment is parked through a first-class `workerproto` field the
  coordinator sets in the exchange the worker already makes, carrying the fenced attempt
  revision. The worker never queries coordinator state directly, and the coordinator refusal
  remains the authority.

Registration + transition to `waiting-external` commit in one fenced store call. A terminal
attempt refuses registration with `attempt is terminal (<progress>); task-bound waits are refused`.
A `done` marker observed while a live task wait exists is refused and recorded as a
reconciliation event; it never verifies.

Environment injected into the task process, and added to the contained allowlist, exactly:

```
T3_STEWARD_WORKFLOW_RUN_ID, T3_STEWARD_TASK_ID, T3_STEWARD_ATTEMPT_ID,
T3_STEWARD_ATTEMPT_REVISION, T3_STEWARD_ASSIGNMENT_ID, T3_STEWARD_THREAD_ID
```

Amendment, 2026-09-14, after the T3 protocol was checked: the primary channel for these six
variables is a worker-written `.t3-steward/task.env` in the prepared workspace, mode 0600,
created before dispatch. `resolveTaskIdentity` reads the process environment first and that file
second. The contained path keeps its sandbox environment injection.

`environment` on `thread.create` is not in the documented T3 contract, the compat range is pinned
at 0.0.38..0.0.38, and `DispatchResult` carries no per-field acknowledgement, so the adapter
cannot tell an honoured field from an ignored one. An ignored field would make identity injection
inert, which is tolerable; a rejected field would fail every uncontained dispatch identically on
retry, which is not. The field is therefore off by default behind an explicit setting, to be
enabled only after verification against a disposable server of the deployed version.

The file carries identity only: no dispatch token, no credential, and it must not travel with
collected outputs or an archived workspace.

`wait add --task current` uses them. Released while waiting: executor slot, CPU/memory/scratch
reservation, provider slot, quota tally. Held: attempt, thread, workspace, artifacts, dependency
mounts, assignment ownership, resource locks, directory bindings.

## H5 recovery (owner: A after H1, or D in wave 4)

- Preparation evidence path: `<attempt>.preparation.<ordinal>.log`, ordinal from 1.
  First causal failure preserved in a durable attempt field and quoted in the terminal reason as
  `preparation failed N times; first error: <first>; last error: <last>`.
- Legacy intake quarantine: submission state `quarantined` in `coordinator_submissions`,
  one durable report, skipped until the content digest changes.
- `campaign rerun <run> --from <task> --idempotency-key KEY [--reason TEXT]`.
- Campaign-scoped durable refs: `refs/campaigns/<run>/<task>/<name>`, never pruned for the
  campaign lifetime.
- `campaign submit --notify-thread <current|id>`.

## Cross-repository fixtures

One fixture set is shared by both repositories so that a rendering test in UpKeeper and a parsing
test in Steward cannot drift:

- `testdata/fleet/fleet-intent.json` — authored fleet intent covering two hosts, two profiles
  (`standard`, `build`), one per-host exception, two provider instances, explicit model
  allowlists, one project with a canonical repository and default ref.
- `testdata/fleet/projection-worker-<host>.json` — expected worker projection, byte-identical to
  the `worker-configuration` v1 document for an unchanged host.
- `testdata/fleet/projection-coordinator.json`, `testdata/fleet/projection-client-<host>.yaml`.

UpKeeper renders them; Steward parses them. Both repositories carry the same bytes.
