# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 and M2.
- Continued M3 through worker placement, project catalogs/setup profiles, and clean per-attempt Git environments with pinned revisions and repository caching.
- Added `LocalRepositoryCache`, which creates one atomic bare mirror per repository and refreshes existing mirrors before every preparation.
- Added `WorkspacePreparer`, which resolves the requested ref to one full Git object ID, clones from the cache with `--no-local`, checks out that exact commit detached, and verifies `HEAD` before setup.
- Task checkouts have independent Git indexes and object stores; they do not use an alternates file or depend on the cache after cloning.
- Prepared attempts publish atomically under `runs/<workflow-run>/<task>/<attempt>/` with `workspace/`, immutable `inputs/`, immutable `dependencies/`, and a read-only `preparation.log`.
- Static input and dependency artifacts are scoped to the workflow run and revalidated by size and SHA-256 while copying from coordinator storage.
- The checkout exposes inputs and dependencies through relative `.t3/inputs` and `.t3/dependencies` symlinks without copying mutable data into the Git worktree.
- Named setup commands run sequentially in the checkout under the profile's overall timeout, with commands, output, and failures captured in the preparation log.
- Clone, checkout, input, dependency, setup, timeout, or publication failure removes the incomplete environment. Once logging has begun, a sibling failure log is retained for diagnosis.
- Added local Git fixture tests for exact commit pinning, cache refresh/reuse, checkout isolation, independent object storage, setup-visible inputs/dependencies, setup failure, timeout, checksum failure, cross-run rejection, retained logs, and staging cleanup.
- No configured/live repository, T3 thread, worker, service, or live database was touched.

## Decisions

- Repository caches are bare mirrors used only as local clone sources. `git clone --no-local` deliberately copies objects instead of sharing them.
- Both 40-character SHA-1 and 64-character SHA-256 Git object IDs are accepted after `rev-parse <ref>^{commit}`.
- Per-attempt preparation supports only `environment.scope: task`; workflow-scoped ownership and serialization remain a separate increment.
- Local repository paths are supported only through the direct preparation seam for deterministic tests. Production catalog validation still accepts remote HTTPS/SSH origins only.
- Setup timeout covers the sequence of setup commands after clone and input preparation. User-systemd/cgroup containment is still required before production.
- Immutable artifacts remain outside the mutable checkout. Relative links provide the documented agent-facing paths.
- Successful preparation logs remain inside the published attempt. Failure logs are retained beside, not inside, the removed attempt directory.
- Cache coordination across processes is not yet implemented; worker protocol/lease ownership must prevent concurrent refresh races or add an explicit cache lock.

## Verification

- `go test ./internal/backlog/... -run 'WorkspacePreparer|LocalRepositoryCache' -count=1`
- `go test ./internal/backlog/... -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- T3 `worktreePath` acceptance has not been prototyped.
- Workflow-scoped checkout ownership/serialization, resource-lock lifecycle, retention cleanup, and user-systemd/cgroup process containment remain unimplemented.
- Setup timeout terminates the direct command but does not yet provide the hard descendant-process guarantee required from a user systemd scope/cgroup.
- Cache refresh lacks an interprocess lock and crash-orphan reconciliation.
- Worker inventory, project catalogs, and prepared-workspace state are not yet loaded/persisted through coordinator/worker protocol.
- Placement establishes worker eligibility only; provider routing, quota, concurrency, reservations, and ordering remain M4.
- No development code has opened live state, contacted workers, dispatched work, or installed/restarted a service.

## Exact next increment

Prototype and verify T3 `worktreePath` behavior without dispatching against the live daemon. Add a hermetic control/API integration harness that submits the prepared checkout path and branch fields to a fake or isolated T3 endpoint, prove request serialization and deterministic thread identity, verify rejection/error handling preserves the prepared environment for reconciliation, and document the confirmed protocol behavior. Do not create a live T3 thread, install binaries, or touch live configuration/state.
