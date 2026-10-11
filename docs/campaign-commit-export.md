# Chaining campaigns and exporting declared commits

A successor campaign can start at a succeeded task's declared commit without
pushing a WIP branch. In its version 2 manifest:

```yaml
environment:
  project: my-project
  ref: campaign:RUN/implement/implementation
tasks:
  successor:
    prompt_file: prompts/successor.md
    inputs_from_run:
      RUN/implement: [handoff.md]
```

`environment.ref` adds the commit declaration as a cross-run dependency for
every task. `inputs_from_run` maps RUN/TASK to declared artifact names and adds
the corresponding dependencies; explicit `needs` is optional. Existing
`needs: [RUN/TASK]` with `inputs_from` keeps its behavior. Local task names
belong in `inputs_from`.

Validate accepts this syntax offline. Check refuses unknown runs, tasks or commit
declarations with permanent `unknown-node` reasons. Submission requires the
producer to have succeeded and pins exactly one retained artifact and its SHA-256.
Workers verify those bytes before setup or an agent starts. The commit record
must name the bound producer, declaration and the successor project's repository.
The worker obtains the commit through its campaign store or the retained bundle,
checks out exactly that commit, and keeps its `campaign-commit/v1` record in
`.t3/dependencies/<producer namespace>/implementation`. Neither producer nor
consumer pushes a Git ref for chaining.

Upgrade the client and coordinator before using the new manifest fields, and
upgrade consuming workers to advertise `campaign-ref-v1` before using a
`campaign:` ref. Older workers continue to run existing manifests; new checkout
tasks require this capability and are excluded from older workers. Ordinary
`inputs_from_run` files lower to the existing cross-run input protocol.
Cross-worker commits also require `campaign-commit-bundle-v1`; if an older
producer retained no bundle and the consumer's local store lacks the commit,
preparation refuses it. The catalog repository must still supply the recorded
base. Chaining does not remove repository read credentials.

Fetch into a local repository that already holds the base in one command:

```sh
t3-steward campaign commit export RUN/implement/implementation --into . --branch next-unit
```

This verifies the exported bundle and creates a new local branch, printing
commit, base and SHA-256 provenance. Existing branches are refused. No checkout
or remote write is performed.

## Exporting a declared campaign commit

Use an authenticated coordinator client:

```sh
t3-steward campaign commit export RUN/TASK/implementation --bundle implementation.bundle
# Choose the one advertised branch:
t3-steward campaign commit export RUN/TASK/implementation --bundle fix.bundle --branch fix/h3
```

The command prints the full commit, base and SHA-256 digest. The destination must be new. It resolves the provenance on the coordinator and uses a retained M16-0 bundle when available; otherwise it requests the commit from the producing worker over authenticated transport. Unknown runs, tasks or declarations, missing provenance, corrupt objects and unavailable transports fail explicitly. Coordinator and worker refs remain unchanged.

The resulting bundle advertises exactly `refs/heads/NAME`, where NAME defaults to the declared commit name, and requires the recorded campaign base. Import into a repository that already holds that base:

```sh
git bundle verify fix.bundle
git bundle list-heads fix.bundle
git fetch fix.bundle refs/heads/fix/h3:refs/heads/fix/h3
```

Before starting a dependent agent, a worker verifies every declared dependency artifact digest and commit ref through `.t3/dependencies`. It retries materialization once when the view is missing or damaged. A persistent error names the producer and input; the agent does not start with a dangling dependency link.
