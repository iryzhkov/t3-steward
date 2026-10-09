BINARY := t3-steward
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

# The lint gate is pinned so that it produces the same result on every host and on
# every day. Staticcheck reads the export data written by the Go toolchain that
# compiles the packages under analysis, so the analyzer version and the toolchain
# version have to be chosen together: an analyzer built against an older Go cannot
# decode a newer toolchain's export data, and staticcheck v0.8.x requires Go 1.26.
# LINT_TOOLCHAIN therefore tracks the `go` directive in go.mod, and
# STATICCHECK_VERSION is the newest release that supports it. Raise both together.
LINT_TOOLCHAIN ?= go1.25.0
STATICCHECK_VERSION ?= v0.7.0
GOFMT ?= $(shell GOTOOLCHAIN=$(LINT_TOOLCHAIN) go env GOROOT)/bin/gofmt

# Explicit timeouts, so that a hung test fails with a goroutine dump instead of
# holding a runner until Go's ten-minute default or the CI job limit.
TEST_TIMEOUT ?= 10m
RACE_TIMEOUT ?= 25m
# check-fast runs the race detector only on packages changed against this ref.
FAST_BASE ?= origin/main
# Extra flags for the race pass of check-fast and check-review. Empty by default,
# so the race detector keeps checkptr, which validates every unsafe pointer
# conversion, in every package, including modernc.org's SQLite translation: an
# invalid conversion there can still return the expected rows and pass every
# assertion, and only checkptr reports it. It is assigned with = rather than ?=
# so that a value left in the environment cannot weaken the gate; only an
# explicit `make check-review RACE_GCFLAGS=...` changes it.
RACE_GCFLAGS =
# check-fast-no-sqlite-checkptr and check-review-no-sqlite-checkptr are opt-in
# faster variants of the two gates. modernc.org/sqlite converts pointers in almost
# every operation, so under -race checkptr about doubles the CPU cost of each SQL
# statement; these targets turn checkptr off for modernc.org packages alone. The
# race detector still instruments them and checkptr still covers every other
# package, but a pointer bug inside modernc.org goes unreported, so they never
# replace check-review as the gate before review.
NO_SQLITE_CHECKPTR_GCFLAGS := -gcflags=modernc.org/...=-d=checkptr=0
check-fast-no-sqlite-checkptr check-review-no-sqlite-checkptr: RACE_GCFLAGS = $(NO_SQLITE_CHECKPTR_GCFLAGS)
# The commit test-affected compares against, and its list-only switch. Like
# RACE_GCFLAGS they are assigned with = so that only the make command line sets
# them: a BASE left in the environment must not choose what gets tested. (GNU
# make also takes command-line variables from MAKEFLAGS, and lets the
# environment win under make -e; list-only runs therefore say so.)
BASE =
TEST_AFFECTED_LIST =

.PHONY: build test check-fast check-review check-fast-no-sqlite-checkptr check-review-no-sqlite-checkptr test-affected qualification lint install clean

build:
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/$(BINARY)

# The complete gate: every test, then every test under the race detector, then
# vet. Run it before asking for review and before a release.
test:
	go test -timeout $(TEST_TIMEOUT) ./...
	go test -race -timeout $(RACE_TIMEOUT) ./...
	go vet ./...

# The fast local gate for iterating: build, vet, the short test suite (tests
# that write thousands of rows to cross a bound cross a lowered one), lint, and
# the race detector on the packages changed against FAST_BASE, including
# uncommitted and untracked Go files. It fails if FAST_BASE is missing or git
# cannot list the changes. It is not a substitute for `make test`, which CI
# runs in full. check-review uses the same checks with a full-size changed-package
# race pass, replacing check-fast followed by a second full-size race invocation.
#
# The short pass leaves out the changed packages, because the race pass runs
# every one of their tests again (all of them for check-review, the same short
# set for check-fast) with the race detector added. Running them first without
# it repeated the slowest packages for no additional assertion.
check-fast check-review check-fast-no-sqlite-checkptr check-review-no-sqlite-checkptr:
	go build ./...
	go vet ./...
	@pkgs=$$(sh scripts/changed-go-packages.sh '$(FAST_BASE)') || exit 1; \
	if [ -n "$$pkgs" ]; then \
		changed=$$(go list $$pkgs) || exit 1; \
		all_pkgs=$$(go list ./...) || exit 1; \
		short_pkgs=$$(printf '%s\n' "$$all_pkgs" | grep -vxF "$$changed") || [ $$? -eq 1 ] || exit 1; \
	else \
		short_pkgs=./...; \
	fi; \
	if [ -n "$$short_pkgs" ]; then \
		echo "go test -short ./... except the packages the race pass runs"; \
		go test -short -timeout $(TEST_TIMEOUT) $$short_pkgs || exit 1; \
	fi; \
	$(MAKE) lint || exit 1; \
	if [ -n "$$pkgs" ]; then \
		race_short=; \
		case "$@" in check-fast*) race_short=-short;; esac; \
		echo "go test -race $(RACE_GCFLAGS)" $$race_short $$pkgs; \
		go test -race $(RACE_GCFLAGS) $$race_short -timeout $(RACE_TIMEOUT) $$pkgs; \
	else \
		echo "no Go packages changed against $(FAST_BASE); skipping the race pass"; \
	fi

# `make test-stress` runs the tests that once failed only on a loaded host
# (the growth-bounded complexity tests and the immutable-tree cleanup tests)
# repeatedly under the race detector while busy loops saturate their CPUs
# (scripts/stress-timing-tests.sh). It is not part of `make test`.
test-stress:
	sh scripts/stress-timing-tests.sh

# The edit-test loop: `make test-affected BASE=<commit>` runs, under the race
# detector with checkptr kept and at full size, the tests of every package
# changed against BASE and of every package whose tests import one of them,
# directly, transitively or only from a _test.go file. Uncommitted and
# untracked files count; a changed template, golden or other file inside a
# package directory selects that package, and a change to go.mod or go.sum
# selects every package (scripts/affected-go-packages.sh). BASE is required and
# never defaults to the whole module. TEST_AFFECTED_LIST=1 prints the selection
# without running it. It does not build, vet or lint, and it replaces neither
# check-review nor `make test` as the gate.
test-affected:
	@if [ -z '$(BASE)' ]; then \
		echo 'test-affected: BASE is required: make test-affected BASE=<commit>' >&2; \
		exit 2; \
	fi; \
	changed=$$(sh scripts/affected-go-packages.sh --changed '$(BASE)') || exit 1; \
	pkgs=$$(sh scripts/affected-go-packages.sh '$(BASE)') || exit 1; \
	if [ -z "$$pkgs" ]; then \
		echo 'test-affected: no Go package changed against $(BASE); nothing to run'; \
		exit 0; \
	fi; \
	echo "test-affected: $$(echo "$$changed" | wc -l | tr -d ' ') packages changed against $(BASE), $$(echo "$$pkgs" | wc -l | tr -d ' ') selected with their importers:"; \
	echo "$$pkgs"; \
	if [ -n '$(TEST_AFFECTED_LIST)' ]; then \
		echo 'test-affected: TEST_AFFECTED_LIST is set; listed only, no test was run'; \
		exit 0; \
	fi; \
	echo "go test -race -count=1 -timeout $(RACE_TIMEOUT)" $$pkgs; \
	go test -race -count=1 -timeout $(RACE_TIMEOUT) $$pkgs

# The nested-process qualification gates, which repeat tests the ordinary pass
# already runs in fresh go test processes. The nightly workflow runs them.
qualification:
	go test -tags qualification -timeout 30m -count=1 \
		-run '^(TestBacklogV2ProductionQualification|TestCoordinatorLocalMultiProcessWorkflow)$$' ./cmd/t3-steward

lint:
	GOTOOLCHAIN=$(LINT_TOOLCHAIN) go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...
	test -z "$$($(GOFMT) -l .)"

install: build
	install -d $(HOME)/.local/bin
	install -m 0755 bin/$(BINARY) $(HOME)/.local/bin/$(BINARY)

clean:
	rm -rf bin dist
