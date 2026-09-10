# Backlog orchestrator handoff

Updated: 2026-09-10

## Completed checkpoint

- Completed M1, the domain and compatibility foundation.
- Completed the first M2 increment: the strict version 2 workflow manifest definition and validation.
- Added defaults for surplus class, Git task-scoped environments, importance 3, difficulty 3, and three turns. Task class, placement, and ordered routes inherit from the workflow, with task placement narrowing eligible hosts and adding capabilities.
- Covered every manifest field, strict unknown/duplicate YAML rejection, invalid names and values, dependency cycles and missing nodes, undeclared dependency artifacts, and direct-dependency requirements.
- Validated static task and workflow-scoped placement, including host-bound provider routes.
- Added bundle-relative prompt and static-input validation, glob expansion, regular-file checks, and containment checks that reject absolute paths, path escapes, and symlinks escaping the bundle.

## Decisions

- Version 2 manifests use lowercase stable names containing letters, digits, hyphens, and underscores; names must begin with a letter.
- The manifest environment currently supports Git only. Its project is required, its type defaults to `git`, and its scope defaults to `task`.
- Workflow inputs may use glob patterns. Prompt files, declared outputs, and dependency artifact names must be non-glob relative paths.
- `inputs_from` may consume only declared outputs from a direct dependency. This makes artifact staging and dependency provenance unambiguous.
- Host allowlists are intersected across workflow and task placement. Required capabilities are combined. A workflow-scoped environment must retain at least one statically eligible host across every task and host-bound route.
- Safe symlinks resolving inside the submitted bundle are accepted; symlinks resolving outside it are rejected. Atomic copying and checksum capture remain the next increment.
- Legacy version 1 workflows and runner behavior remain unchanged.

## Verification

- `go test ./internal/backlog/... -run Manifest`
- `go test ./internal/backlog/...`
- `go test ./...`
- `go vet ./...`
- `make lint`

All passed.

## Remaining risks

- Bundle validation currently establishes safe paths, but does not copy inputs, calculate checksums, or close time-of-check/time-of-use races. Atomic ingestion must re-open and copy validated content into coordinator-owned storage.
- Version 2 manifests are not yet converted into persistent workflow, task, run, attempt, and artifact records.
- Dependency execution state transitions, failure propagation, retry, cancellation, output capture, verification, and artifact transfer remain unimplemented.
- The coordinator records are not yet the live runner's source of truth.
- No development code has opened the live state database or dispatched work.

## Exact next increment

Implement atomic version 2 bundle ingestion into coordinator-owned storage. Copy the manifest, prompts, and expanded static inputs, calculate checksums and sizes, create immutable domain records, commit metadata atomically, and test rollback on invalid files or persistence failures without touching the live state database.
