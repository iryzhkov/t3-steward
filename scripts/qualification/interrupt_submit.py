#!/usr/bin/env python3
"""Start a submission and kill it the instant the coordinator has acted.

This is how the harness makes the lost response of case 2 real rather than
simulated. The client process is started, a tight loop watches the
coordinator's own bundle storage for the first byte the coordinator writes for
this submission, and the client is killed with SIGKILL as soon as that appears.
The client therefore never learns the answer to a request the coordinator has
already begun to act on, which is exactly the uncertainty the idempotency key
exists to resolve.

The watch is on the coordinator's storage rather than on a timer, so the kill
cannot land before the coordinator has done anything.

Usage: interrupt_submit.py WATCH_DIR TIMEOUT_SECONDS -- COMMAND [ARGS...]

It prints one JSON object describing what happened, and exits 0 when it
completed its own work. Whether the kill landed is reported in the JSON rather
than in the exit status, because the caller has to be able to tell a real loss
from a client that simply finished first.
"""

import json
import os
import signal
import subprocess
import sys
import time


def snapshot(root):
    """Return the set of paths under root, or an empty set when it is absent."""
    found = set()
    for base, directories, files in os.walk(root):
        for name in directories:
            found.add(os.path.join(base, name))
        for name in files:
            found.add(os.path.join(base, name))
    return found


def main():
    if len(sys.argv) < 5 or "--" not in sys.argv:
        print(__doc__, file=sys.stderr)
        return 2
    watch = sys.argv[1]
    timeout = float(sys.argv[2])
    command = sys.argv[sys.argv.index("--") + 1 :]

    before = snapshot(watch)
    started = time.monotonic()
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    killed = False
    observed = None
    while True:
        if process.poll() is not None:
            break
        current = snapshot(watch)
        new = current - before
        if new:
            observed = sorted(new)[0]
            try:
                os.kill(process.pid, signal.SIGKILL)
                killed = True
            except ProcessLookupError:
                killed = False
            break
        if time.monotonic() - started > timeout:
            break
    status = process.wait()
    print(
        json.dumps(
            {
                "killed": killed,
                "exitStatus": status,
                "coordinatorWrote": observed,
                "elapsedSeconds": round(time.monotonic() - started, 3),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
