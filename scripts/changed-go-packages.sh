#!/bin/sh
# changed-go-packages.sh BASE
#
# Prints, one per line as ./dir, the Go package directories that differ from
# BASE in this checkout: commits since the merge base with BASE, uncommitted
# changes to tracked files, and untracked Go files that are not ignored.
# Directories that no longer exist are left out.
#
# Exit status: 0 with no output when no Go package changed; 2 when BASE does
# not name a commit; any other non-zero status when git itself fails. The
# caller can therefore tell "nothing changed" from "could not tell".
set -eu

base=${1:-}
if [ -z "$base" ]; then
	echo "usage: changed-go-packages.sh BASE" >&2
	exit 2
fi
if ! git rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
	echo "changed-go-packages: $base does not name a commit; fetch it or set FAST_BASE" >&2
	exit 2
fi

# Each listing is its own assignment so that set -e stops on a git failure.
committed=$(git diff --name-only "$base"...HEAD)
uncommitted=$(git diff --name-only HEAD)
untracked=$(git ls-files --others --exclude-standard)

printf '%s\n%s\n%s\n' "$committed" "$uncommitted" "$untracked" |
	while IFS= read -r file; do
		case $file in
		*.go)
			dir=$(dirname "$file")
			if [ -d "$dir" ]; then
				printf './%s\n' "$dir"
			fi
			;;
		esac
	done |
	sort -u
