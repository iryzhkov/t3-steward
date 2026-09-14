# Dedicated provider process boundary

S5a runtime integration is unfinished. Normal directory-bound worker packages
remain refused before effects. The operator qualification command is:

```sh
t3-steward worker contained-exec --spec launch.json
```

This command runs a dedicated T3 server (or its qualification command) and its
descendants inside Linux bubblewrap namespaces. It must not wrap the normal
worker while leaving providers attached to the unrestricted shared T3 service.

The operator-owned JSON specification contains:

- `workerId`: worker owning every resource.
- `home`, `workspace`: complete inspected identities for already provisioned,
  separate Steward-owned directories; registrations must allow writing.
- `directories`: approved identity/access bindings, mounted at `/data/0`,
  `/data/1`, etc. Read-only is enforced by the mounts.
- `runtimePaths`: operator-selected, read-only runtime directories exposed at
  `/runtime/0`, etc. Supply runtime distributions, never a whole user home.
- `providerHosts`: exact lowercase provider DNS names permitted on TCP 443.
  Omit for a completely offline launch.
- `command`: absolute executable path and arguments inside the sandbox.
- `cwd`: `/workspace` (default), or exactly one `/data/N` mount for direct cwd.
- `control`, `controlPort`: optional separate owned 0700 directory identity and
  namespace-local T3 API port (1–65535, except the egress port 18080). Supply both.
  The control directory is mounted at `/control`; the contained child publishes
  `api.sock` there, refusing to replace an existing endpoint.

The specification is not capsule input. It requires worker-owned preparation
and approved credentials before runtime use. Use fresh owned storage; do not seed
it with hard links to existing datasets, shared host credentials or host MCP
configuration. Runtime and owned directories may not overlap dataset identities
or each other. Source directories are never created, copied, chmodded or removed
by this launcher. Every source is reopened against its inspected identity and
mounted through the retained descriptor, avoiding path replacement between
validation and mount creation.

The root contains selected read-only OS/runtime paths, fresh proc/dev/tmp/run,
the private home, owned workspace and declared data. Host home, /run, /tmp and
/etc are not mounted wholesale. Environment inheritance is disabled. Namespace
creation, further-user-namespace denial and capability dropping are mandatory;
unsupported bubblewrap/kernel capabilities fail closed.

Networking uses a private network namespace. A loopback bridge reaches only a
single mounted Unix socket belonging to the constrained host-side gateway.
The gateway accepts HTTPS CONNECT for approved names on 443, validates every DNS
answer as public, and dials the checked numeric address without another lookup.
Private, loopback, link-local, translation and special-use destinations are
refused; mixed public/private DNS answers also fail closed. No automatic address
retry occurs. Provider clients must honor HTTP(S)_PROXY; direct network attempts
have no host connectivity. NO_PROXY covers only the sandbox's own loopback.
Provider endpoint/proxy compatibility still needs live qualification.

The supervisor's context stops the dedicated namespace, including descendants.
This context belongs to the separate user systemd service when launched through
the durable supervisor commands:

```sh
t3-steward worker contained-start --spec launch.json --state-dir /absolute/private/state --execution ID
t3-steward worker contained-show --spec launch.json --state-dir /absolute/private/state --execution ID
t3-steward worker contained-stop --spec launch.json --state-dir /absolute/private/state --execution ID
```

Provision the state directory as 0700, outside every sandbox mount. Start reserves
the execution identity durably before its single systemd request. Repeat calls
only observe the same unit; changed specs are refused. A lost response, incomplete
intent or missing unit never authorizes another launch or releases ownership.
The service has no worker-service dependency, no restart policy and no lease
timer. Cancellation of the calling CLI/worker context leaves it running.

Stop is an explicit cancellation/finalization effect. It waits for systemd's
control-group stop and persists a complete stop receipt. An uncertain stop reply
does not release ownership. Even an exited main process is not treated as proof
that every descendant stopped. A stopped receipt describes process custody,
never successful provider completion. Keep these journals across worker restarts;
there is no automatic relaunch or journal cleanup.

The control bridge forwards that Unix socket only to 127.0.0.1 at the approved
port inside the namespace. The host-side `t3api.NewUnix` client dials only this
socket, ignores proxy environment and refuses redirects. Provision a dedicated
T3 server and its execution-specific token; never use the shared host token.
Control storage may not overlap home, output, source data, runtime or supervisor
state. The namespace helper can provision T3 and its local token:

```text
/steward worker contained-t3 --node /runtime/0/bin/node --entry /runtime/0/lib/node_modules/t3/dist/bin.mjs --port 18881
```

The helper creates a fresh private T3 state directory and refuses a prior or
partial startup. Its initial settings disable all providers, browser access and
provider update checks. To prepare the OpenCode lane, supply both
`--opencode-binary /runtime/1/opencode` and `--opencode-model <approved-model>`
with that reviewed runtime mount. Only OpenCode is enabled; its server URL and
password are empty so T3 starts a namespace-local server. Text generation uses
the same explicit model. No shared provider settings or credentials are copied.
Existing settings are never overwritten. These settings do not prove provider
network access, credential readiness, worker preparation or contained verification;
the normal directory execution guard remains closed pending integration.

Use this as the operator spec command with matching controlPort and a reviewed
Node/T3 runtime mount. It issues a one-hour session in /home/agent/t3, atomically
publishes /control/token, and refreshes after 48 minutes. Failed maintenance
stops the dedicated server; it never retries provider execution or reports task
success. Desktop startup mode disables automatic projects and empty threads.
The worker must explicitly ensure its project and dispatch its one execution.

The host SocketTokenFile source observes atomic rotations, refuses escapes
outside control storage, and rejects special files without blocking.
LocalDriver now accepts an ExecutionT3Provider attachment for directory-bound
thread operations. Attach must observe an already prepared, fenced supervisor;
it may not launch or replace one. Creation, observation, cancellation, settlement,
collection, warnings, checkpoints and resume use this scoped control. Attachment
failure never falls back to host T3. Attached dispatch explicitly ensures a
project at /workspace and uses that namespace path for the thread worktree.

Provider account credentials/settings, production supervisor attachment,
contained verification/artifact mapping and preparation integration remain
unimplemented. The normal directory preparation guard stays closed. The normal worker's directory guard stays closed
until those integrations and installed provider recovery qualification pass.

## Evidence and remaining gate

Unit tests cover public destination filtering, pinned DNS dialing, buffered
CONNECT data and bridge cancellation. The Linux kernel test is opt-in because
CI may lack bubblewrap/user namespace permission:

```sh
T3_STEWARD_REQUIRE_CONTAINMENT_TESTS=1 go test ./internal/providercontainment -count=1
```

Run this required test on the intended containment host. It verifies direct and
child-process read-only denial, owned output, unavailable host home/services and
isolated host loopback using disposable directories. A skipped kernel test is not
containment qualification. Unsupported platforms refuse launching.

The real user-service test additionally checks that cancelling the caller and
reconstructing the supervisor manager preserves the same running invocation:

```sh
T3_STEWARD_REQUIRE_SUPERVISOR_TESTS=1 go test ./internal/providercontainment -run TestSupervisorSystemd -count=1
```

This is a bounded process fixture, not installed provider qualification.
Unit tests cover lost launch/stop responses, concurrent claims, missing units,
changed contracts, incomplete intents, foreign unit refusal and durable custody.

Next integrate the supervisor with the worker and scoped T3 API, provider-specific
credentials/proxy settings, directory cwd/output mappings and authoritative
stopped-process evidence. Then qualify an actual provider task on installed
releases on two hosts, including writable data, cancellation, restart and
reconnect. The local launch tests are not a substitute for those live proofs.
