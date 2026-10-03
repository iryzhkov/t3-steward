# Contributing

Thanks for helping. This project is small and the rules are short.

## Development

```sh
git clone https://github.com/iryzhkov/t3-steward
cd t3-steward
make check-fast  # build, vet, go test -short, lint, race on changed packages
make test        # go test, go test -race, go vet: the complete gate
make lint        # staticcheck and gofmt
make build       # bin/t3-steward
```

Use `make check-fast` while iterating. It takes a few minutes rather than the
full suite's quarter of an hour, because `-short` makes the tests that write
thousands of SQLite rows cross their bounds with fewer rows, and the race
detector runs only on the packages changed against `origin/main` (set
`FAST_BASE` to compare against another ref). Run `make test` before asking
for review: it runs every test at full size, under the race detector too.

`make qualification` runs the nested-process qualification gates
(`TestBacklogV2ProductionQualification`, `TestCoordinatorLocalMultiProcessWorkflow`),
which build only with the `qualification` tag. They repeat in fresh `go test`
processes tests the ordinary pass already runs, so CI runs them nightly
(`.github/workflows/nightly.yml`, which also runs the race detector on macOS)
rather than on every pull request.

Go 1.24 or newer is required. There is no CGO: the SQLite driver is pure Go
so cross-compiling works with `GOOS`/`GOARCH` alone.

## Layout

- `internal/domain` – shared types (buckets, snapshots, threads, intents).
- `internal/policy` – the pure state machine. No I/O, fully unit-tested.
- `internal/source/providerlog` – log parsing, provider normalizers, tailer.
- `internal/control/t3` – the only package that knows T3 command names.
- `internal/t3api` – HTTP client, token minting, server discovery.
- `internal/daemon` – wiring: snapshots in, actions out, resume scheduling.
- `internal/store/sqlite` – persistence and audit log.
- `internal/platform` – background-process installers.
- `internal/compat` – tested T3 version range.

Keep T3-specific knowledge inside `internal/control/t3`, `internal/t3api`
and `internal/source/providerlog`. The policy engine must never learn about
log files or command names.

## Adding a provider

1. Add a normalizer in `internal/source/providerlog/parse.go` that turns
   the provider's `account.rate-limits.updated` payload into
   `domain.QuotaSnapshot` values (one per window).
2. Add a redacted fixture under `testdata/samples/` and a parser test.
3. Document the payload in `docs/t3-protocol.md`.

## Fixtures

Redact fixtures before committing: replace thread, session and event ids
with zero UUIDs, and never commit message bodies or account identifiers.
`testdata/samples/*.log` show the expected shape.

## Verifying a new T3 version

Follow the checklist at the end of `docs/t3-protocol.md`, then bump the
range in `internal/compat/compat.go` and the README table in one commit.

## Pull requests

- One change per pull request, with tests for policy or parser changes.
- `make test lint` must pass. CI runs the plain tests on Linux and macOS, the
  race detector on Linux, lint and a cross-build; the nightly workflow adds the
  race detector on macOS and the qualification gates.
- Use conventional commit prefixes (`feat:`, `fix:`, `docs:`, `test:`,
  `chore:`); the release changelog is generated from them.
