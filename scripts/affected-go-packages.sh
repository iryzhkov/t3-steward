#!/bin/sh
# affected-go-packages.sh [--changed] BASE
#
# Prints, sorted and one import path per line, the packages of this module that
# a change against BASE can affect: every changed package and every package
# whose test binary depends on one, including through an import made only by a
# _test.go file. With --changed it prints the changed packages alone.
#
# Changes are listed as changed-go-packages.sh lists them: commits since the
# merge base with BASE, uncommitted changes to tracked files, and untracked
# files that are not ignored, with both sides of a rename. Every changed
# file, Go or not, changes the nearest module package directory that encloses
# it, so an embedded template or a testdata golden changes its package, and a
# file under no package (docs/, the Makefile) changes none. A change to go.mod
# or go.sum changes every package. Run it from the module root, which must be the top of
# the repository, as the Makefile does.
#
# Exit status: 0 with no output when nothing is selected; 2 when BASE is
# missing or does not name a commit; any other non-zero status when git or go
# fails, including a module that does not load, or when run below the top of
# the repository.
set -eu

mode=affected
if [ "${1:-}" = --changed ]; then
	mode=changed
	shift
fi
base=${1:-}
if [ -z "$base" ]; then
	echo "usage: affected-go-packages.sh [--changed] BASE" >&2
	exit 2
fi

if ! git rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
	echo "affected-go-packages: $base does not name a commit; fetch it or pass another BASE" >&2
	exit 2
fi
# Git names changed files from the top of the repository and go list names
# directories from the module root; anywhere else no change would match a
# package, and the selection would be wrongly empty.
if [ -n "$(git rev-parse --show-prefix)" ]; then
	echo "affected-go-packages: run it from the top of the repository, which must be the module root" >&2
	exit 1
fi

# changed-go-packages.sh would also refuse an unknown BASE with status 2.
go_dirs=$(sh "$(dirname "$0")/changed-go-packages.sh" "$base") || exit $?
# Each listing is its own assignment so that set -e stops on a git failure.
# Both sides of a rename count, so that a file moved out of a package changes
# it. Git still quotes a name holding a tab, a quote, a backslash or a newline;
# awk unquotes those.
committed=$(git -c core.quotePath=false diff --no-renames --name-only "$base"...HEAD)
uncommitted=$(git -c core.quotePath=false diff --no-renames --name-only HEAD)
untracked=$(git -c core.quotePath=false ls-files --others --exclude-standard)
if [ -z "$go_dirs$committed$uncommitted$untracked" ]; then
	exit 0
fi

# The module's packages, with each directory relative to the module root, and
# the dependencies of every package and test binary. Deps holds test variants
# such as "p [q.test]", which contain spaces, so the fields are tab-separated.
# Neither listing uses -e: a module that does not load fails here.
packages=$(go list -f '{{.ImportPath}}{{"\t"}}{{.Dir}}{{"\t"}}{{.Module.Dir}}' ./...)
deps=
if [ "$mode" = affected ]; then
	deps=$(go list -test -f '{{.ImportPath}}{{range .Deps}}{{"\t"}}{{.}}{{end}}' ./...)
fi

# The sections reach awk tagged and in the order it needs them: packages, then
# changes, then dependencies.
tab=$(printf '\t')
{
	printf '%s\n' "$packages" | sed "s/^/package$tab/"
	printf '%s\n' "$go_dirs" | sed -n "s|^\./|dir$tab|p"
	printf '%s\n%s\n%s\n' "$committed" "$uncommitted" "$untracked" | sed -n "/./s/^/file$tab/p"
	if [ -n "$deps" ]; then
		printf '%s\n' "$deps" | sed "s/^/deps$tab/"
	fi
} | awk -F "$tab" '
	# The import path of the package that encloses the relative directory d, or
	# "" when no module package does.
	function enclosing(d) {
		for (;;) {
			if (d in package_at) return package_at[d]
			if (d == ".") return ""
			if (d !~ /\//) d = "."
			else sub(/\/[^\/]*$/, "", d)
		}
	}
	# A name as git lists it, without the C-style quoting git applies to a
	# name holding a tab, a quote, a backslash or a control character.
	function unquoted(name,   out, i, c, n) {
		if (name !~ /^".*"$/) return name
		name = substr(name, 2, length(name) - 2)
		out = ""
		for (i = 1; i <= length(name); i++) {
			c = substr(name, i, 1)
			if (c == "\\" && i < length(name)) {
				c = substr(name, ++i, 1)
				if (c == "t") c = "\t"
				else if (c == "n") c = "\n"
				else if (c ~ /[0-7]/) {
					n = substr(name, i, 3)
					c = sprintf("%c", substr(n, 1, 1) * 64 + substr(n, 2, 1) * 8 + substr(n, 3, 1))
					i += 2
				} else if (c == "a") c = "\a"
				else if (c == "b") c = "\b"
				else if (c == "f") c = "\f"
				else if (c == "r") c = "\r"
				else if (c == "v") c = "\v"
			}
			out = out c
		}
		return out
	}
	function directory(file) {
		if (file !~ /\//) return "."
		sub(/\/[^\/]*$/, "", file)
		return file
	}
	# A dependency or package name without its test variant, so that
	# "p [q.test]" is p.
	function plain(name) {
		sub(/ \[.*\]$/, "", name)
		return name
	}
	$1 == "package" {
		dir = $3
		if (dir == $4) dir = "."
		else if (index(dir, $4 "/") == 1) dir = substr(dir, length($4) + 2)
		package_at[dir] = $2
		module[$2] = 1
		next
	}
	$1 == "dir" {
		p = enclosing($2)
		if (p != "") changed[p] = 1
		next
	}
	# Every changed file, Go or not, also changes its enclosing package, so
	# that a Go file under testdata or in a removed directory counts too.
	$1 == "file" {
		# The whole rest of the line, since a name may hold a tab.
		file = unquoted(substr($0, length("file") + 2))
		if (file == "go.mod" || file == "go.sum") {
			for (p in module) changed[p] = 1
		} else {
			p = enclosing(directory(file))
			if (p != "") changed[p] = 1
		}
		next
	}
	$1 == "deps" {
		# The package a line belongs to: "p", "p [p.test]", the external
		# test package "p_test [p.test]" and the test binary "p.test" all
		# belong to p.
		owner = $2
		if (match(owner, / \[.*\]$/)) owner = substr(owner, RSTART + 2, RLENGTH - 3)
		sub(/\.test$/, "", owner)
		if (!(owner in module) || (owner in selected)) next
		for (i = 2; i <= NF; i++) {
			if (plain($i) in changed) {
				selected[owner] = 1
				break
			}
		}
		next
	}
	END {
		for (p in changed) selected[p] = 1
		for (p in selected) print p
	}
' | LC_ALL=C sort
