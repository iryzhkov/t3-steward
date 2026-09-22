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

.PHONY: build test lint install clean

build:
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/$(BINARY) ./cmd/$(BINARY)

test:
	go test ./...
	go test -race ./...
	go vet ./...

lint:
	GOTOOLCHAIN=$(LINT_TOOLCHAIN) go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...
	test -z "$$($(GOFMT) -l .)"

install: build
	install -d $(HOME)/.local/bin
	install -m 0755 bin/$(BINARY) $(HOME)/.local/bin/$(BINARY)

clean:
	rm -rf bin dist
