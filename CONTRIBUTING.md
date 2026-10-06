# Contributing

Thanks for helping. This project is small and the rules are short.

## Development

```sh
git clone https://github.com/iryzhkov/t3-steward
cd t3-steward
make check-fast  # build, vet, go test -short, lint, short race on changed packages
make check-review # same checks, full-size race on changed packages once
make check-review-no-sqlite-checkptr # opt-in: faster, no checkptr in modernc.org
make test        # go test, go test -race, go vet: the complete gate
make lint        # staticcheck and gofmt
make build       # bin/t3-steward
```

Use `make check-fast` while iterating. It takes a few minutes rather than the
full suite's quarter of an hour, because `-short` makes the tests that write
thousands of SQLite rows cross their bounds with fewer rows, and the race
detector runs only on the packages changed against `origin/main` (set
`FAST_BASE` to compare against another ref).

If a task's accepted local gate is `check-fast` plus full-size race tests on
changed packages, use `make check-review` instead. It keeps build, vet, the
short plain suite and lint, then runs changed-package race tests once without
`-short`. This avoids running essentially the same race suite twice. Both
targets use the same changed-package detection and refuse an invalid
`FAST_BASE`; a missing change list never counts as a passing race run. The
short plain pass leaves out the changed packages, because the race pass runs
all of their tests again. The race pass keeps the race detector's checkptr
instrumentation in every package, including the `modernc.org` SQLite
translation, where an invalid unsafe pointer conversion can still return the
expected rows and only checkptr reports it.
`make check-review` does not replace `make test` when the task requires the
complete gate. Run `make test` before asking for review under the repository's
default policy: it runs every test at full size, under the race detector too.

`make check-fast-no-sqlite-checkptr` and `make check-review-no-sqlite-checkptr`
are opt-in faster variants for iterating on SQLite-heavy packages. They turn
checkptr off for `modernc.org` packages alone, which roughly halves the CPU cost
of each SQL statement under the race detector; checkptr still covers every other
package. They lose that one check, so they never replace `make check-review`
before review, and CI keeps checkptr everywhere.

`make qualification` runs the nested-process qualification gates
(`TestBacklogV2ProductionQualification`, `TestCoordinatorLocalMultiProcessWorkflow`),
which build only with the `qualification` tag. They repeat in fresh `go test`
processes tests the ordinary pass already runs, so CI runs them nightly
(`.github/workflows/nightly.yml`, which also runs the race detector on macOS)
rather than on every pull request.

Go 1.24 or newer is required. There is no CGO: the SQLite driver is pure Go
so cross-compiling works with `GOOS`/`GOARCH` alone.

## Keeping tests fast

The gates run with `GOMAXPROCS=2` and under the race detector, where every
SQLite statement runs through instrumented, translated C. A test that is cheap
in a plain run can cost seconds there, so:

- Never sleep for real time. Inject the clock (`SetClock`, a `Now` field) and
  advance it, or wait on a channel. When a test must poll a condition, poll
  every millisecond or so against a deadline, not on a fixed long interval.
- Get a migrated test database from `sqlitetest.OpenMigrated`
  (`internal/store/sqlite/sqlitetest`), or `openMigratedFixture` inside the
  sqlite package. It copies a database migrated once per test process instead
  of running every schema migration again, which takes about a second under
  the race detector. Call `sqlite.OpenMigrated` on a new path only in a test
  whose subject is migration or database creation. Prefer a file under
  `t.TempDir()` to `:memory:`, which cannot use the template.
- Mark a test or subtest `t.Parallel()` when it builds its own fixture and
  touches no package-level state, environment or working directory. Matrix
  tests whose cases each build a store are the usual candidates.
- Share expensive setup through a fixture built once (a `sync.Once` image, a
  template directory to copy) rather than rebuilding it in every case, and
  keep every case the matrix had: speed comes from cheaper setup, not from
  fewer assertions.
- When many independent iterations of a race mostly wait on I/O or locks, run
  them on several goroutines and check each exactly as before.
- To find what is slow, run `go test -race -json -count=1` on the package and
  rank the `pass` events by `Elapsed`; `-cpuprofile` on one test shows whether
  it is computing or waiting.

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
