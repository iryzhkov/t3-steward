#!/bin/sh
# Regression for M17-2 review P1: role-ranking integration must pass the
# gate's unused-code checks in both affected packages. Run from the repo root.
set -eu
GOMAXPROCS=2 GOTOOLCHAIN=go1.25.0 go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./cmd/t3-steward/ ./internal/domain/
