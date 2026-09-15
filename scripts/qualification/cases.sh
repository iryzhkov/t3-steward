#!/usr/bin/env bash
# The four qualification cases of hardening gate 1.
#
# Each case names the evidence it read and reaches a verdict from it. Nothing
# here treats an absent answer as a passing one: a command that could not run,
# an output that could not be parsed and a fleet that was not ready are all
# failures.

set -euo pipefail

# jq_field reads one value out of a JSON document with python, which is present
# on every host this harness runs on.
json_field() {
  python3 -c 'import json,sys
document = json.load(sys.stdin)
for key in sys.argv[1:]:
    if isinstance(document, list):
        document = document[int(key)]
    else:
        document = document.get(key)
    if document is None:
        print(""); raise SystemExit
print(document)' "$@"
}

# json_after_preamble reads the JSON document that follows any human preamble.
#
# It exists because "campaign submit --allow-unverified --json" prints its
# warning to standard output before the JSON document, so the stream is not a
# JSON document. The first argument is the key to print.
json_after_preamble() {
  python3 -c 'import json,sys
text = sys.stdin.read()
start = text.find("{")
if start < 0:
    print(""); raise SystemExit
try:
    document = json.loads(text[start:])
except Exception:
    print(""); raise SystemExit
value = document
for key in sys.argv[1:]:
    if not isinstance(value, dict):
        print(""); raise SystemExit
    value = value.get(key)
    if value is None:
        print(""); raise SystemExit
print(value)' "$@"
}

# evidence_path names one evidence file for a case.
evidence_path() { printf '%s/evidence/%s' "$ROOT" "$1"; }

# ---------------------------------------------------------------------------
# Case 1: inspect the coordinator and submit one campaign from a host that is
# not the coordinator, using only its local CLI and its configured coordinator
# client.
# ---------------------------------------------------------------------------
case_one() {
  local identity
  identity=$(evidence_path case1-identity.json)
  # The pinned profile: one key narrowed to the query operation.
  if ! fleet_client_cli pinned coordinator identity --json >"$identity" 2>"$identity.err"; then
    record case1-identity FAIL "coordinator identity failed: $(tail -n 3 "$identity.err")"
    return
  fi
  local coordinator carrier
  coordinator=$(json_field coordinatorId <"$identity")
  carrier=$(json_field transport carrier <"$identity")
  if [ "$coordinator" != qual-coordinator ] || [ "$carrier" != ssh ]; then
    record case1-identity FAIL "identity reported coordinator=$coordinator carrier=$carrier"
    return
  fi
  record case1-identity PASS "coordinator=$coordinator carrier=$carrier evidence=$identity"

  local directory before after submission run
  directory=$(fleet_campaign case1 good)
  before=$(fleet_workflow_count)
  submission=$(evidence_path case1-submit.json)
  CASE1_KEY="qual-case1-$$"
  export CASE1_KEY
  # The documented agent path: one client block, no --allow-unverified, so the
  # readiness check and the submission both travel over the same key.
  if ! fleet_client_cli main campaign submit "$directory" \
      --idempotency-key "$CASE1_KEY" \
      --json >"$submission" 2>"$submission.err"; then
    record case1-submit FAIL "submission failed: $(tail -n 3 "$submission.err")"
    return
  fi
  run=$(json_after_preamble runId <"$submission" 2>/dev/null || true)
  if [ -z "$run" ]; then
    record case1-submit FAIL "the submission answer named no run: $(head -c 200 "$submission")"
    return
  fi
  after=$(fleet_workflow_count)
  if [ "$after" != "$((before + 1))" ]; then
    record case1-submit FAIL "workflow runs went from $before to $after, expected exactly one more"
    return
  fi
  CASE1_RUN=$run
  CASE1_BEFORE=$before
  CASE1_AFTER=$after
  export CASE1_RUN CASE1_BEFORE CASE1_AFTER
  record case1-submit PASS "run=$run runs $before->$after key=$CASE1_KEY evidence=$submission"
}

# ---------------------------------------------------------------------------
# restricted-ssh: the SSH path is genuinely restricted.
#
# The harness claims a real OpenSSH server with real forced commands rather than
# a stand-in, so it proves it: an arbitrary command presented on the admin key
# must not run, and the session must be the forced command instead. The evidence
# is sshd's own session log line.
# ---------------------------------------------------------------------------
case_restricted_ssh() {
  local marker="$ROOT/evidence/ssh-escape-attempt"
  local out
  out=$(evidence_path restricted-ssh.txt)
  T3_QUAL_SSH_CONFIG="$ROOT/client/ssh_config" "$ROOT/bin/ssh" \
    -oBatchMode=yes qual-admin "touch $marker" </dev/null >"$out" 2>&1 || true
  if [ -e "$marker" ]; then
    record restricted-ssh FAIL "the forced command ran an arbitrary command: $marker exists"
    return
  fi
  local sessions
  sessions=$(grep -c 'Starting session: forced-command' "$ROOT/evidence/sshd.log" || true)
  if [ "${sessions:-0}" -lt 1 ]; then
    record restricted-ssh FAIL 'sshd recorded no forced-command session'
    return
  fi
  record restricted-ssh PASS "sshd ran the forced command instead of the requested command; $sessions forced-command session(s) so far"
}

# ---------------------------------------------------------------------------
# Case 2: lose the submission response and repeat the request with the same
# idempotency key. The loss is made real: the client process is killed once the
# coordinator has created the run, before the client can read its answer.
# ---------------------------------------------------------------------------
case_two() {
  local directory before during after
  directory=$(fleet_campaign case2 good)
  before=$(fleet_workflow_count)
  local key="qual-case2-$$"
  local first killed=no
  first=$(evidence_path case2-first.json)

  # The loss is made real by killing the client the instant the coordinator
  # writes the first byte of this submission into its own bundle storage. The
  # watch is on the coordinator's storage, so the kill cannot land before the
  # coordinator has acted.
  local interrupted
  interrupted=$(evidence_path case2-interrupt.json)
  python3 "$HARNESS_DIR/interrupt_submit.py" "$ROOT/coordinator/bundles" 60 -- \
    env -i \
      PATH="$ROOT/bin:/usr/bin:/bin" \
      HOME="$ROOT/client/home" \
      XDG_CONFIG_HOME="$ROOT/client/home/.config" \
      XDG_STATE_HOME="$ROOT/client/home/.local/state" \
      T3_QUAL_SSH_CONFIG="$ROOT/client/ssh_config" \
      "$STEWARD" campaign --config "$ROOT/client/config-main.yaml" \
        submit "$directory" --idempotency-key "$key" --json \
    >"$interrupted" 2>"$interrupted.err" || true
  if [ ! -s "$interrupted" ]; then
    record case2-loss FAIL "the interrupter produced no report: $(tail -n 3 "$interrupted.err")"
    return
  fi
  killed=$(json_field killed <"$interrupted")
  local wrote
  wrote=$(json_field coordinatorWrote <"$interrupted")
  : >"$first"

  during=$(fleet_workflow_count)
  if [ "$killed" != True ]; then
    record case2-loss FAIL "the client completed before it could be killed; the loss was not real ($(cat "$interrupted"))"
  elif [ -z "$wrote" ]; then
    record case2-loss FAIL 'the client was killed before the coordinator wrote anything'
  else
    record case2-loss PASS "client killed after the coordinator wrote $(basename "$wrote"); runs $before->$during"
  fi

  local second
  second=$(evidence_path case2-second.json)
  if ! fleet_client_cli main campaign submit "$directory" \
      --idempotency-key "$key" --json \
      >"$second" 2>"$second.err"; then
    record case2-retry FAIL "the retry with the same key failed: $(tail -n 3 "$second.err")"
    return
  fi
  after=$(fleet_workflow_count)
  # Exactly one run must exist for this key: either the interrupted attempt
  # created it and the retry returned that same result, or the interrupted
  # attempt was killed before the record was committed and the retry created it
  # once. Both are one run, and two would be the failure this case exists for.
  if [ "$after" != "$((before + 1))" ]; then
    record case2-retry FAIL "the key produced $((after - before)) runs, expected exactly 1 (before=$before during=$during after=$after)"
    return
  fi
  record case2-retry PASS "one run for one key: before=$before during=$during after=$after; evidence=$second"
}

# ---------------------------------------------------------------------------
# Case 3: five campaigns that can never run. Each must be classified permanent
# and must create zero workflow runs, both when the client checks and when the
# coordinator validates at acceptance.
# ---------------------------------------------------------------------------
# case_three covers the three sub-cases that need a live repository observation
# on a worker. The two syntax sub-cases run in case_three_syntax, against a
# coordinator restarted with the malformed projects.
case_three() {
  local -a projects=(absent-forge private-https missing-ref)
  local project
  for project in "${projects[@]}"; do
    case_three_one "$project"
  done
}

case_three_syntax() {
  local -a projects=(bad-syntax argument-injection)
  local project
  for project in "${projects[@]}"; do
    case_three_one "$project"
  done
}

case_three_one() {
  local project=$1
  local directory before after check submit outcome permanent codes
  directory=$(fleet_campaign "case3-$project" "$project")
  before=$(fleet_workflow_count)

  check=$(evidence_path "case3-$project-check.json")
  # campaign check exits non-zero when the campaign is impossible, which is the
  # expected outcome here, so its status is read rather than trusted. It runs
  # from the client host, which is where an agent would run it.
  fleet_client_cli main campaign check "$directory" --json >"$check" 2>"$check.err" || true
  if [ ! -s "$check" ]; then
    record "case3-$project" FAIL "campaign check produced no matrix: $(tail -n 3 "$check.err")"
    return
  fi
  outcome=$(python3 -c 'import json,sys
print((json.load(sys.stdin).get("matrix") or {}).get("outcome") or "")' <"$check" 2>/dev/null || true)
  read -r permanent codes <<EOF
$(python3 -c 'import json,sys
matrix = json.load(sys.stdin).get("matrix") or {}
codes = []
def collect(reasons):
    for reason in reasons or []:
        if reason.get("permanent"):
            codes.append(reason.get("code"))
collect(matrix.get("reasons"))
for task in matrix.get("tasks") or []:
    collect(task.get("reasons"))
    for candidate in task.get("candidates") or []:
        collect(candidate.get("reasons"))
print(len(codes), ",".join(sorted(set(codes))) or "-")' <"$check" 2>/dev/null || echo '0 -')
EOF
  if [ "$outcome" != impossible ] || [ "${permanent:-0}" -lt 1 ]; then
    record "case3-$project" FAIL "check reported outcome=$outcome permanent-reasons=${permanent:-0} codes=${codes:--}"
    return
  fi

  submit=$(evidence_path "case3-$project-submit.txt")
  local status=0
  # --allow-unverified on purpose: it skips the client-side check, so what
  # refuses the submission is the coordinator's own transactional validation at
  # acceptance, which is the property this case is about. Streams are kept
  # apart because the warning goes to standard error.
  fleet_client_cli main campaign submit "$directory" \
    --idempotency-key "qual-case3-$project-$$" --allow-unverified \
    --reason 'qualification: the coordinator must refuse this at acceptance' \
    --json >"$submit" 2>"$submit.err" || status=$?
  after=$(fleet_workflow_count)
  if [ "$status" -eq 0 ]; then
    record "case3-$project" FAIL "the coordinator accepted a permanently impossible campaign (runs $before->$after)"
    return
  fi
  if [ "$after" != "$before" ]; then
    record "case3-$project" FAIL "a refused submission changed the run count from $before to $after"
    return
  fi
  record "case3-$project" PASS "impossible, $permanent permanent reason(s) [$codes], submit exit $status, runs stayed $after"
}

# ---------------------------------------------------------------------------
# remote-viability: the readiness check from the host that is not the
# coordinator.
#
# Gate 3 requires "campaign check on a tiny disposable campaign directory
# returns a viability matrix" from a non-coordinator host, and campaign submit
# runs the same query by default. This case asserts that it works, so that the
# harness reports the integrated behaviour rather than the behaviour of either
# branch alone.
# ---------------------------------------------------------------------------
case_remote_viability() {
  local directory out status=0
  directory=$(fleet_campaign remote-viability good)
  out=$(evidence_path remote-viability.txt)
  fleet_client_cli main campaign check "$directory" --json >"$out" 2>"$out.err" || status=$?
  local outcome
  outcome=$(json_after_preamble matrix outcome <"$out" 2>/dev/null || true)
  if [ -n "$outcome" ]; then
    record remote-viability PASS "campaign check answered $outcome over the remote carrier"
    return
  fi
  record remote-viability FAIL "campaign check from a non-coordinator host: exit $status: $(head -c 300 "$out" | tr '\n' ' ')"
}

# ---------------------------------------------------------------------------
# Case 4: a private repository over SSH that one worker holds a credential for
# and the other does not. The per-worker candidate matrix must distinguish them.
# ---------------------------------------------------------------------------
case_four() {
  local directory check
  directory=$(fleet_campaign case4 private)
  check=$(evidence_path case4-check.json)
  fleet_client_cli main campaign check "$directory" --json >"$check" 2>"$check.err" || true
  if [ ! -s "$check" ]; then
    record case4 FAIL "campaign check produced no matrix: $(tail -n 3 "$check.err")"
    return
  fi
  local summary
  summary=$(python3 -c 'import json,sys
matrix = json.load(sys.stdin).get("matrix") or {}
rows = []
for task in matrix.get("tasks") or []:
    for candidate in task.get("candidates") or []:
        codes = sorted({reason.get("code") for reason in candidate.get("reasons") or []})
        rows.append((candidate.get("worker"), candidate.get("outcome"), "+".join(codes) or "none"))
print(json.dumps(rows))' <"$check" 2>/dev/null || echo '[]')
  local verdict
  verdict=$(python3 -c 'import json,sys
rows = json.loads(sys.argv[1])
byworker = {worker: (outcome, codes) for worker, outcome, codes in rows}
a = byworker.get("worker-a")
b = byworker.get("worker-b")
if not a or not b:
    print("FAIL both workers must appear as candidates: " + json.dumps(rows)); raise SystemExit
if "repository" in a[1] or "ref-not-found" in a[1]:
    print("FAIL worker-a should read the repository, got " + json.dumps(a)); raise SystemExit
if "repository-authentication-failed" not in b[1]:
    print("FAIL worker-b should fail authentication, got " + json.dumps(b)); raise SystemExit
if a[1] == b[1]:
    print("FAIL the matrix does not distinguish the two workers: " + json.dumps(rows)); raise SystemExit
print("PASS worker-a=" + json.dumps(a) + " worker-b=" + json.dumps(b))' "$summary")
  if [ "${verdict%% *}" = PASS ]; then
    record case4 PASS "${verdict#PASS } evidence=$check"
  else
    record case4 FAIL "${verdict#FAIL }"
  fi
}
