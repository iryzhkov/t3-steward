# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1 and M2.
- Continued M3 by completing worker inventory/capability matching and the logical project catalog with named setup profiles.
- Added immutable `ProjectCatalog`, `ProjectDefinition`, `SetupProfile`, and `ResolvedEnvironment` types.
- Project entries carry a canonical Git repository, default ref, T3 project template, setup profile name, project resource locks, and named credential requirements.
- Catalog construction rejects duplicate/invalid projects and profiles, unknown profile references, insecure or malformed repository URLs, unsafe Git refs, invalid identifiers, untrimmed/control-bearing setup commands, and nonpositive setup timeouts.
- Canonical repositories accept HTTPS, SSH URLs, and SCP-style SSH syntax; local paths, insecure HTTP, embedded HTTPS credentials, URL queries, and fragments are rejected.
- Environment resolution validates workflow/task identity, Git environment type and scope, selects an explicit workflow ref or the catalog default, clones setup metadata, and returns sorted unique project-plus-task locks.
- Credential requirements remain names only. No credential values, environment maps, or secret content exist in the catalog model.
- Catalog inputs and resolved results are deeply detached so callers cannot mutate later resolutions.
- Added tests for default and overridden refs, SSH repository forms, lock merging, credentials, input/result immutability, invalid definitions, unknown projects/profiles, unsafe refs, and workflow/task mismatches.
- No repository was cloned and no setup command was executed.

## Decisions

- Catalog validation accepts only remote HTTPS or SSH Git origins. Local filesystem repository support, if needed for fixtures, must be an explicit test/preparation seam rather than a production catalog shortcut.
- HTTPS user information is rejected to prevent credentials entering configuration or logs. SSH usernames are allowed, but URL passwords are not.
- Setup commands are trusted operator-authored shell commands, but each configured entry must be a single nonempty trimmed line without NUL, CR, or LF.
- Setup profiles require an explicit positive timeout; execution and containment belong to workspace preparation.
- Workflow `environment.ref` overrides the catalog default only after the same safe-ref validation.
- Project and task resource locks are combined as a sorted unique set.
- The catalog is configuration-independent and performs no filesystem, network, Git, worker, or live-state access.

## Verification

- `go test ./internal/backlog/... -run ProjectCatalog -count=1`
- `go test ./internal/backlog/... ./internal/domain -count=1`
- `go test ./...`
- `go vet ./...`
- `git diff --check`

All passed.

## Remaining risks

- Per-attempt Git workspace preparation, pinned commit resolution, repository caching, setup execution/logs, failure cleanup, and process containment remain unimplemented.
- T3 `worktreePath` acceptance has not been prototyped.
- Workflow-scoped checkout ownership/serialization and resource-lock lifecycle remain unimplemented.
- Worker inventory and project catalogs are not yet loaded from new configuration or persisted through a coordinator/worker protocol.
- Placement establishes worker eligibility only; provider routing, quota, concurrency, reservations, and ordering remain M4 work.
- Finalized artifact metadata still lacks dedicated revision-checked coordinator persistence.
- No development code has opened the live state database, contacted workers, cloned a configured repository, or dispatched work.

## Exact next increment

Implement clean per-attempt Git workspace preparation with pinned revisions and a repository-cache seam. Resolve a requested ref to one commit, create the documented run directory layout in temporary test roots, ensure each checkout has an independent Git index and object store, materialize static/dependency inputs without mutating coordinator artifacts, run the named setup profile with a timeout while capturing preparation logs, and remove incomplete environments on clone or setup failure. Add local-fixture tests for pinned commits, checkout isolation, cache use, setup failure, timeout, and cleanup. Do not invoke T3 or use a catalog entry/live repository.
