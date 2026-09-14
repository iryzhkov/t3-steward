#!/usr/bin/env bash
# Case 15: an interactive wait registered from a session that is not executing a
# backlog or campaign task.
#
# It must wake the thread it names and change no workflow state. The thread is
# created directly on the synthetic provider, so it belongs to no run: anything
# the wait touched in the workflow graph would be visible as a change in the run
# list or in the run the harness had already created.

set -euo pipefail

case_fifteen() {
  local thread=thread-interactive-case15 signal=case15
  rm -f "$ROOT/signals/$signal"
  # A thread with no scripted turn: the provider records the turn and completes
  # it without running anything, which is all this case needs.
  if ! curl -sf -X POST "http://127.0.0.1:$T3_PORT/api/orchestration/dispatch" \
      -H 'Content-Type: application/json' \
      -d "{\"type\":\"thread.create\",\"commandId\":\"case15-create\",\"threadId\":\"$thread\",\"projectId\":\"qual-plain\",\"title\":\"interactive\"}" \
      >"$(evidence_path case15-create.json)" 2>&1; then
    record case15 FAIL 'the synthetic provider refused to create the interactive thread'
    return
  fi

  local runsBefore
  runsBefore=$(fleet_workflow_count)
  local out status=0
  out=$(evidence_path case15-register.txt)
  fleet_coordinator_cli wait add --thread "$thread" --name case15 \
    --every 30s --max-every 30s --timeout 10m \
    -- test -f "$ROOT/signals/$signal" >"$out" 2>&1 || status=$?
  if [ "$status" -ne 0 ]; then
    record case15 FAIL "the interactive wait was refused: $(head -c 200 "$out" | tr '\n' ' ')"
    return
  fi

  : >"$ROOT/signals/$signal"
  local deadline=$((SECONDS + 240)) starts=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    starts=$(turn_starts "$thread")
    [ "${starts:-0}" -ge 1 ] && break
    sleep 3
  done
  if [ "${starts:-0}" -lt 1 ]; then
    record case15 FAIL "the interactive wait settled but no wake reached thread $thread"
    return
  fi
  if [ "$starts" -gt 1 ]; then
    record case15 FAIL "thread $thread received $starts wakes, expected exactly one"
    return
  fi
  local runsAfter
  runsAfter=$(fleet_workflow_count)
  if [ "$runsAfter" != "$runsBefore" ]; then
    record case15 FAIL "the interactive wait changed the run count from $runsBefore to $runsAfter"
    return
  fi
  # The wake must have gone to the named thread and to no other.
  local strays
  strays=$(python3 -c 'import json,sys
named = sys.argv[1]
others = set()
for line in sys.stdin:
    try:
        entry = json.loads(line)
    except Exception:
        continue
    command = entry.get("command") or {}
    if command.get("type") != "thread.turn.start":
        continue
    text = ((command.get("message") or {}).get("text") or "")
    if "case15" in text and command.get("threadId") != named:
        others.add(command.get("threadId"))
print(len(others))' "$thread" <"$ROOT/evidence/t3-stub.jsonl")
  if [ "${strays:-0}" != 0 ]; then
    record case15 FAIL "the wake also reached $strays other thread(s)"
    return
  fi
  record case15 PASS "one wake delivered to $thread, run count unchanged at $runsAfter, no other thread woken"
}
