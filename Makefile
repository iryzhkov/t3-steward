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

.PHONY: build test check-fast qualification lint install clean

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
# the race detector on the packages changed against FAST_BASE plus uncommitted
# changes. It is not a substitute for `make test`, which CI runs in full.
check-fast:
	go build ./...
	go vet ./...
	go test -short -timeout $(TEST_TIMEOUT) ./...
	$(MAKE) lint
	@pkgs=$$({ git diff --name-only $(FAST_BASE)...HEAD; git diff --name-only HEAD; } 2>/dev/null \
		| grep '\.go$$' | xargs -r -n1 dirname | sort -u \
		| while read -r dir; do [ -d "$$dir" ] && echo "./$$dir"; done); \
	if [ -n "$$pkgs" ]; then \
		echo "go test -race -short" $$pkgs; \
		go test -race -short -timeout $(RACE_TIMEOUT) $$pkgs; \
	else \
		echo "no Go packages changed against $(FAST_BASE); skipping the race pass"; \
	fi

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
