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
This context must belong to a durable, separately managed execution supervisor,
not a worker connection or assignment lease. The launcher itself does not yet
persist supervisor identity, expose a scoped T3 control endpoint, integrate
artifact paths, or establish restart/reconnect recovery. Those are prerequisites
for removing the normal worker's fail-closed directory guard.

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

Next integrate the dedicated supervisor and scoped T3 API, provider-specific
credentials/proxy settings, directory cwd/output mappings and authoritative
stopped-process evidence. Then qualify an actual provider task on installed
releases on two hosts, including writable data, cancellation, restart and
reconnect. The local launch tests are not a substitute for those live proofs.
