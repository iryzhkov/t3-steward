#!/usr/bin/env bash
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
base=c382eea6a91319ab72246066b5ed4bdfc75bec26
export GOMAXPROCS=4 GOFLAGS=-p=4 MAKEFLAGS=-j4 CARGO_BUILD_JOBS=4
scratch="$(mktemp -d /tmp/rc122-compat.XXXXXX)"
cleanup() {
 git -C "$root" worktree remove --force "$scratch/rc121" >/dev/null 2>&1 || true
 rm -rf "$scratch"
}
trap cleanup EXIT
git -C "$root" worktree add --detach "$scratch/rc121" "$base"
mkdir -p "$scratch/rc121/.t3" "$scratch/home"
cp "$root/scripts/rc122compat-reader.go" "$scratch/rc121/.t3/compat-reader.go"
(cd "$scratch/rc121"; go build -o "$scratch/rc121-cli" ./cmd/t3-steward; go build -o "$scratch/rc121-reader" ./.t3/compat-reader.go)
export RC121_CLI="$scratch/rc121-cli" RC121_READER="$scratch/rc121-reader"
cd "$root"
go test -tags rc122compat -count=1 -v ./cmd/t3-steward -run '^TestRC122DowngradeCompatibility$'
