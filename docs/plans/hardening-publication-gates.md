# Hardening: publication, canary and rollback gates

Written before any live state is touched. The order is binding: a gate that has not passed is not
a gate that may be skipped, and a blocked gate is reported as blocked rather than worked around.

## Fleet facts this procedure depends on

- Coordinator: `normandy-coordinator` on normandy (192.168.70.234).
- Workers: homelab and omarchy-pc enrolled, normandy not enrolled.
- Installed Steward release: `0.11.0-rc.48`, commit `8019693`, distributed as the
  `t3-steward` github-release component.
- Installed UpKeeper controller: the Python entry point from `~/.local/share/dev-fleet` at
  `dfbf7f7`, which predates the Go migration merged as `70892d9`.
- Canary host for the client transport: omarchy-pc, because it is a non-coordinator worker and is
  the host whose CLI could not reach the coordinator in the first place.

## Gate 1: disposable qualification (no live state)

All of it runs against disposable coordinator and worker state, Unix sockets, a restricted SSH
forced-command wrapper, disposable T3 data, throwaway Git repositories and credentials, throwaway
UpKeeper homes and synthetic provider observations. Nothing in this gate may touch
`~/.config/t3-steward`, `~/.local/state/t3-steward`, `~/.config/upkeeper`, the live coordinator
database, or either historical campaign run.

The seventeen cases required by the contract are the gate. Each must produce evidence that names
the run, task, attempt and wait identities it exercised.

## Gate 2: source publication

1. Both branches rebased on current canonical `main` and reviewed as a whole diff.
2. `make test` and `make lint` pass in t3-steward; `make go-gate` passes in UpKeeper.
3. Push, then confirm GitHub CI succeeds for the exact pushed commit SHAs, not for a branch name.
4. Publish the Steward release artifact for the exact commit and verify the artifact's reported
   version and commit match it.

A Git push alone is not publication and is not convergence.

## Gate 3: read-only live check from a non-coordinator host

On omarchy-pc, with the new client configuration present but nothing else changed:

1. `t3-steward coordinator identity --json` reports `normandy-coordinator`, its release, its
   configuration digest and the `ssh` carrier.
2. `t3-steward campaign check` on a tiny disposable campaign directory returns a viability matrix.
3. No workflow run is created by either command. Verified by comparing
   `t3-steward campaign list` before and after.

## Gate 4: one tiny live campaign, and one that must be refused

1. Submit one tiny idempotent campaign from omarchy-pc. Confirm exactly one coordinator run.
2. Repeat the identical request with the same idempotency key. Confirm still exactly one run.
3. Submit one campaign naming an intentionally wrong repository. Confirm zero workflows created
   and a permanent reason code.
4. Run one disposable wait-aware task: it registers a task-bound wait, ends its turn, and the
   campaign must remain non-terminal with no outputs collected and no verification run until the
   wait settles.

## Gate 5: UpKeeper canary

Canary host: omarchy-pc only.

1. Review the full `desired/release.json` candidate diff before anything is committed. Confirm
   that unrelated component pins, worker settings and secret references are unchanged.
2. Apply the fleet configuration to the canary host alone.
3. Inspect desired, observed and effective model sets and confirm they differ where they should.
4. Produce the enrollment plan and review the authorization-relevant diff and digests.
5. Apply enrollment explicitly, only after the plan is correct.
6. Run one tiny campaign over the newly enrolled route.

## Gate 6: rollback, exercised not assumed

Rollback is exercised on the canary before the fleet is expanded.

1. Restore the previous worker bootstrap from the retained prior document and confirm the host
   converges back to the `worker-configuration` v1 bytes it had.
2. Restore the previous Steward release pin and confirm the running binary reports it.
3. Confirm the coordinator still schedules work to the canary host afterwards.
4. Record what rollback did not restore, if anything.

## Gate 7: fleet expansion and release capture

1. Expand to the intended hosts only after gates 5 and 6 pass.
2. Capture the release from a host whose tooling and agent environment have been validated,
   reviewing the whole candidate manifest, not only the components that were meant to change.
3. Confirm GitHub CI succeeds for the exact UpKeeper release commit.
4. Confirm controller installation and convergence.

## Standing prohibitions during all gates

- Neither historical UpKeeper-migration run is used as a test fixture, cancelled, retried,
  amended or otherwise mutated. They are regression evidence.
- No live coordinator or worker configuration, enrollment, service, active wait or active T3
  thread is changed before gate 1 passes and this procedure is written, which it now is.
- `upkeeper push` is treated as publication in every form. The candidate manifest is reviewed
  before it runs, never after.
- No secret value enters a repository, manifest, log, prompt or artifact.
