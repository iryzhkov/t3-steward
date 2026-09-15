# Collection after a stopped turn

A parked-assignment report can be recent and still precede a task's registration.
If the report is empty, receiving it after the turn stops does not prove that the
task finished. Collection would remove the execution identity and publish output
before the coordinator's next exchange reports the park.

The worker durably records its journal sequence at the first stopped observation.
The coordinator reads its persisted worker snapshot before reading parked attempts
and returns that snapshot's worker epoch and sequence with its complete park report.
An empty report authorizes collection only when this acknowledgement covers the
stopped observation in the same worker epoch. A positive park takes effect immediately.

Before collection, the production driver also reads the latest provider TurnID
from a fresh T3 observation. The worker persists that ID in its journal; a changed
ID clears the stopped fence even if the worker saw neither a park nor a running
turn in between. Missing terminal TurnID defers collection. This closes the case
where a first park and wake finish between polls, then the resumed turn registers
a second wait before an old empty report arrives. Scoped drivers use the same
observation; no-effects mode uses an explicit local identity and runs no provider.

Observed resumption clears the stopped fence. Removing a previously reported park
also clears it, covering a resumed turn that starts and ends between worker polls.
The next stopped observation therefore requires another acknowledged snapshot.
This costs one additional exchange before normal collection. Restart retains the
fence and never turns an unacknowledged report into collection authority.

Compatibility: a coordinator that reports parks but omits acknowledgement causes
collection to defer safely. The existing policy for a genuinely older coordinator
that never reports parks remains unchanged. Workers advertise the built-in capability `task-wait-collection-fence-v1` in
their snapshots independently of operator configuration. The coordinator emits
the new acknowledgement fields only for workers advertising that capability.
Existing H4 workers therefore receive the old report shape their strict decoder
understands. New workers with an older H4 coordinator defer collection safely.
Rollback from a new worker to an old worker in the same worker epoch needs a
fresh capability observation before the coordinator may send new fields; this
case remains part of live rollback validation. No live rollout was performed.

Regression evidence: TestFreshPreStopEmptyStatementCannotAuthorizeCollection failed
before the implementation with one premature collection, then passed. Tests also
cover wrong epochs, stale acknowledgements, restart, and an unobserved resumed turn.
The real-driver wake-window test now persists worker snapshots at the coordinator
and proves that the identity survives until the next causally acknowledged exchange.
