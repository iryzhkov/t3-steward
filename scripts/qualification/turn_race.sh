#!/bin/sh
# The scripted first turn of the race scenario.
#
# QUAL_BIAS selects which way the race is biased:
#   registration-first  register the wait, then end the turn
#   completion-first    start the registration detached and end the turn at once,
#                       so the provider reports the turn complete while the
#                       registration is still in flight
set -u

if [ ! -f ./.t3-steward/task.env ]; then
  echo "backlog status: failed no execution identity in the workspace"
  exit 0
fi
. ./.t3-steward/task.env

# The identity is used exactly as the workspace record gives it.

printf '%s\n' "$T3_STEWARD_ATTEMPT_ID" >"$EVIDENCE/$QUAL_SIGNAL-attempt.txt"

# The check is run once before the attempt is parked, so a check that takes time
# delays the registration by that much. That is how the completion-first
# ordering is reached: the turn has ended and the worker has already reconciled
# the terminal thread by the time the registration arrives.
register() {
  "$STEWARD" wait add --config "$COORDINATOR_CONFIG" --task current --name "$QUAL_SIGNAL" --every 30s --max-every 30s --timeout 10m --run-timeout 5m -- /bin/sh -c "date -u +%FT%TZ > '$EVIDENCE/$QUAL_SIGNAL-check-started.txt'; if [ '$QUAL_BIAS' = completion-first ]; then n=0; until [ -f '$SIGNALS/$QUAL_SIGNAL-allow-registration' ]; do n=\$((n+1)); [ \$n -lt 240 ] || exit 2; sleep 1; done; fi; test -f '$SIGNALS/$QUAL_SIGNAL'" >"$EVIDENCE/$QUAL_SIGNAL-register.txt" 2>&1
  printf 'exit=%s\n' "$?" >>"$EVIDENCE/$QUAL_SIGNAL-register.txt"
}

if [ "${1:-}" = register ]; then
  register
  exit 0
fi

if [ "$QUAL_BIAS" = completion-first ]; then
  # Shell redirection alone leaves saved stdout/stderr descriptors open in a
  # background function. The provider's capture waits for those pipes and the
  # turn never ends before registration. A fresh process closes every extra FD.
  python3 - "$0" "$EVIDENCE/$QUAL_SIGNAL-check-started.txt" <<'PY'
import pathlib, subprocess, sys, time
child = subprocess.Popen(["/bin/sh", sys.argv[1], "register"],
    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    close_fds=True, start_new_session=True)
for _ in range(150):
    if pathlib.Path(sys.argv[2]).exists():
        break
    if child.poll() is not None:
        raise SystemExit("registration child exited before starting its check")
    time.sleep(0.1)
else:
    child.terminate()
    raise SystemExit("registration check did not start")
PY
  printf 'written before delayed registration\n' > result.txt
  echo "race: turn ended while the registration was in flight"
  exit 0
fi

register
echo "race: registered before ending the turn"
