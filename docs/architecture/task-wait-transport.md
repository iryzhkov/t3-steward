# Task-bound waits across hosts

Task registration uses the same configured coordinator client as campaign submission. The worker retains and executes the shell check in its local state database. Settlement, coordinator expiry, resumption, pending delivery and delivery transitions cross the authenticated administrator transport; coordinator time and the existing store transactions remain authoritative.

The coordinator derives each pending wake's worker ID from the attempt's durable assignment. Every production wait runner delivers only wakes owned by its configured local worker, using the private UpKeeper worker bootstrap when the local YAML has no worker identity. A contradictory bootstrap fences task delivery. Ownership does not depend on a saved local poll: a registered wait whose local save failed can still expire and wake its owning worker. Interactive waits continue using the local store and control path.

Registration retries with the same request ID reuse one local check without resetting a settled check. The local check ID is deterministic; insertion does not overwrite a concurrently updated poll. `wait list --native --json` includes task waits, `wait cancel tw-ID` cancels them through the coordinator, and `wait run-now tw-ID` explicitly refuses because only the owning worker can execute its check. First-outcome-wins settlement preserves cancellation and timeout results.

Deploy the coordinator before remote workers: older coordinators reject the new runtime operations. No new fields are persisted in wait, attempt, or assignment records; worker ID is derived response metadata. Drain active waits before downgrading because older workers cannot report remote outcomes and older coordinators cannot serve these operations.

Focused validation uses separate real SQLite stores, the actual CLI and authenticated coordinator-exchange framing through a subprocess carrier, two runners racing for delivery, registration replay, cancellation, missing-local-poll timeout, bootstrap-only identity, authorization denial, and invalid future revision refusal. The carrier test does not claim to run sshd; disposable fleet qualification supplies the real restricted SSH proof.
