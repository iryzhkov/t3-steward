#!/usr/bin/env bash
# Disposable multi-process qualification harness: gate 1 of the hardening
# publication procedure, cases 1 to 4.
#
# It builds this worktree, stands up a disposable fleet under one temporary
# root, and proves four end-to-end properties across real operating-system
# processes and a real OpenSSH server. It touches no live fleet state: no path
# under the real home is read or written, no live coordinator is contacted, no
# worker is enrolled, no provider turn is executed and upkeeper is never run.
#
# Every case either passes with named evidence or fails loudly. There is no
# skip: a case that cannot be attempted is a failure, because a skipped case
# reported as passed is the worst outcome this gate can produce.
#
# Usage: scripts/qualification/run.sh [--keep] [--only CASE]
#
# See docs/qualification-harness.md.

set -euo pipefail

HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$HARNESS_DIR/../.." && pwd)
export HARNESS_DIR REPO_DIR

# shellcheck source=scripts/qualification/fleet.sh
. "$HARNESS_DIR/fleet.sh"
# shellcheck source=scripts/qualification/cases.sh
. "$HARNESS_DIR/cases.sh"
# shellcheck source=scripts/qualification/lifecycle.sh
. "$HARNESS_DIR/lifecycle.sh"
# shellcheck source=scripts/qualification/case11.sh
. "$HARNESS_DIR/case11.sh"
# shellcheck source=scripts/qualification/attacks.sh
. "$HARNESS_DIR/attacks.sh"
# shellcheck source=scripts/qualification/case13.sh
. "$HARNESS_DIR/case13.sh"
# shellcheck source=scripts/qualification/case12.sh
. "$HARNESS_DIR/case12.sh"
# shellcheck source=scripts/qualification/case15.sh
. "$HARNESS_DIR/case15.sh"
# shellcheck source=scripts/qualification/case16.sh
. "$HARNESS_DIR/case16.sh"

KEEP_ROOT=0
ONLY=""
while [ $# -gt 0 ]; do
  case $1 in
    --keep) KEEP_ROOT=1 ;;
    --only) shift; ONLY=${1:-} ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) fleet_fail "unknown argument $1" ;;
  esac
  shift
done

# The private HTTPS repository for case 3. It must be a repository that exists
# and that an anonymous client may not read, because the case is about an
# unauthenticated private repository rather than an absent one. Override it when
# this one stops being private.
QUAL_PRIVATE_HTTPS=${T3_QUAL_PRIVATE_HTTPS:-https://github.com/iryzhkov/citadel.git}
export QUAL_PRIVATE_HTTPS

RESULTS=()
FAILURES=0

# record stores one case verdict for the closing summary. Evidence and verdict
# are stored separately so a reader can tell a proof from an assumption.
record() {
  local name=$1 verdict=$2 detail
  # One verdict is one line. A multi-line detail from a command's error output
  # would otherwise break the summary a reader has to scan.
  detail=$(printf '%s' "$3" | tr '\n' ' ' | tr -s ' ')
  RESULTS+=("$verdict|$name|$detail")
  if [ "$verdict" != PASS ]; then
    FAILURES=$((FAILURES + 1))
  fi
  printf '[case] %-8s %-28s %s\n' "$verdict" "$name" "$detail" >&2
}

# cleanup stops every process the harness started, whether the run succeeded,
# failed or was interrupted.
cleanup() {
  local status=$?
  fleet_stop_all
  if [ -n "${ROOT:-}" ] && [ -d "$ROOT" ]; then
    if [ "$KEEP_ROOT" = 1 ]; then
      printf '[harness] temporary root kept at %s\n' "$ROOT" >&2
    else
      # Coordinator bundles and artifacts are deliberately immutable, so the
      # tree is made writable before it is removed.
      chmod -R u+rwX "$ROOT" 2>/dev/null || true
      rm -rf "$ROOT"
    fi
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

selected() {
  [ -z "$ONLY" ] && return 0
  [ "$ONLY" = "$1" ]
}

fleet_setup "$REPO_DIR"

# The order matters. Cases 1, 2, 4 and the three repository sub-cases of case 3
# need both workers observed, which requires a coordinator whose project catalog
# is clean. The two syntax sub-cases then run against a coordinator restarted
# with the malformed projects, which is the only state in which they exist.
selected 1 && case_restricted_ssh
selected 1 && case_one
selected 1 && case_remote_viability
selected 2 && case_two
selected 3 && case_three
selected 4 && case_four

if selected 11; then
  case_execution_path && case_eleven
fi
selected 14 && case_fourteen
selected 14 && case_attack_replay
selected 16 && case_multi_wait
selected 16 && case_wake_all
selected 14 && case_attack_commanded_collect
selected 14 && case_attack_lease_expiry
selected 15 && case_fifteen
selected 7 && case_seven
selected 13 && case_thirteen
selected 12 && case_twelve
selected 8 && case_eight

if selected 3; then
  fleet_restart_coordinator malformed
  case_three_syntax
fi

# The synthetic provider is asked afterwards what it was made to do. Every turn
# that ran came from a script this harness wrote into its own temporary root, so
# a turn recorded with no script behind it would mean something reached a
# provider the harness does not control.
if [ ! -s "$ROOT/evidence/t3-stub.jsonl" ]; then
  record synthetic-provider FAIL 'the synthetic provider recorded nothing at all'
else
  PROVIDER_SUMMARY=$(python3 - "$ROOT/evidence/t3-stub.jsonl" "$ROOT/turns" <<'PY'
import json, sys
starts = scripts = foreign = 0
for line in open(sys.argv[1], encoding="utf-8"):
    try:
        entry = json.loads(line)
    except Exception:
        continue
    if (entry.get("command") or {}).get("type") == "thread.turn.start":
        starts += 1
    if entry.get("event") == "turn-script":
        scripts += 1
        if not (entry.get("script") or "").startswith(sys.argv[2]):
            foreign += 1
print("%d %d %d" % (starts, scripts, foreign))
PY
)
  # shellcheck disable=SC2086 # three counted fields, split on purpose
  set -- $PROVIDER_SUMMARY
  if [ "${3:-1}" != 0 ]; then
    record synthetic-provider FAIL "$3 turn(s) ran a script outside the harness root"
  else
    record synthetic-provider PASS "$1 turn start(s), $2 scripted turn(s), all from the harness root"
  fi
fi

printf '\n===== qualification summary =====\n'
for entry in "${RESULTS[@]}"; do
  printf '%-6s %-28s %s\n' "${entry%%|*}" "$(printf '%s' "$entry" | cut -d'|' -f2)" \
    "$(printf '%s' "$entry" | cut -d'|' -f3-)"
done
printf '================================\n'

if [ "$KEEP_ROOT" = 1 ]; then
  printf 'evidence: %s/evidence\n' "$ROOT"
fi

if [ "$FAILURES" -ne 0 ]; then
  printf '%d case(s) failed\n' "$FAILURES" >&2
  exit 1
fi
printf 'all cases passed\n'
