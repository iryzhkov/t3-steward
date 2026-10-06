# Exporting a declared campaign commit

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
