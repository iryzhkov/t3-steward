# Contained directory workers

The integrated lane supports a fresh, repo-free output workspace with registered
directory attachments and an explicitly configured OpenCode runtime. It requires
an empty setup profile and no host credential requirements. A project using a
directory as its working directory is not enabled by this integration.

The operator configures the worker's `containment` block:

```yaml
containment:
  runtime_paths:
    - /absolute/reviewed/node/root
    - /absolute/reviewed/opencode/root
  node: /runtime/0/bin/node
  t3_entry: /runtime/0/lib/node_modules/t3/dist/bin.mjs
  opencode_binary: /runtime/1/opencode
  provider_hosts: [opencode.ai, models.dev]
```

Paths are host-specific and must be reviewed against the installed releases.
The selected model comes from the assignment's provider route. No host T3 settings
or provider credentials are copied. Registered dataset bindings still come from
the authorized catalog; this profile does not authorize arbitrary data paths.
Without this profile, new directory preparation remains refused.

Preparation publishes the workspace, then persists an execution launch contract
outside provider mounts before starting one durable systemd service. Reconciliation
reuses the workspace and observes the same invocation. An uncertain start cannot
exhaust preparation retries into an effect-free failure. The worker owns the
bounded readiness request; its timeout does not stop the durable server.

Capsule inputs and dependency artifacts appear through the usual `.t3` paths
using separate read-only mounts. The provider writes outputs in `/workspace`;
registered datasets appear at `/data/N` with their declared access. Every scoped
API connection checks the service invocation and mounted directory identities.

Collection retains the terminal thread archive before stopping the whole service.
Verification then runs in separate durable sandbox invocations, without provider
egress or T3 control access. Each command has its own timeout. Its exit result is
persisted before stopping its complete process group, and only that confirmed
result can enter artifact publication. Recovery can finish collection using the
saved archive after the T3 service has stopped.

Cancellation, task timeout and deterministic failure must confirm custody of both
the provider service and any verification services before terminal release.
Uncertain stop or identity observations retain the assignment for reconciliation.
Lease expiry never stops an existing execution.

Local coverage includes kernel-enforced read-only capsule/data mounts, systemd
collection after server shutdown, durable command-result recovery, and preparation
and cancellation custody fences. This is not a claim that the full S5a live
workspace-mode matrix has passed.
