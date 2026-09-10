# Backlog-v2 production-binding handoff

Updated: 2026-09-10

## Authority

- Repository: `/home/igor/Work/t3-steward`
- Host: Normandy
- Branch: `feature/backlog-orchestrator`
- Authoritative plan:
  `docs/plans/backlog-v2-production-binding.md`
- Architecture contract: `docs/architecture/system-model.md`
- Predecessor implementation plan: `docs/plans/backlog-v2.md`
- Predecessor implementation handoff: `docs/plans/backlog-v2-handoff.md`
- Current readiness report:
  `docs/plans/backlog-v2-deployment-readiness.md`
- Initial planning baseline: `6c8726f`
- No installation, deployment, live-service restart, live configuration/state
  mutation, real worker/T3 dispatch, push, or pull request is authorized.

## Current selection

No implementation stage has started from this handoff yet.

The first unattended thread must reconcile the repository, select the first
incomplete stage (currently expected to be S14), and replace this section before
editing code with:

- selected stage and title;
- exact full starting commit;
- copied exit gates;
- focused test commands selected for the stage;
- any pre-existing or unexplained worktree state.

## Completed stages

None. The predecessor M1–M9/S10–S13 release-candidate chain is complete at
`5836493`; the architecture review is committed at `6c8726f`.

## Known blockers carried forward

- The production composition root still runs the legacy path.
- Worker exchange cannot yet carry an execution package or artifacts.
- No production coordinator/worker runtime or authenticated transport exists.
- Admin CLI invocation can execute pending coordinator commands.
- SQLite opening implicitly migrates schema.
- Configuration accepts unknown YAML fields.
- Schedule syntax/timer ownership, submission idempotency/limits, and the
  production quota observation bridge are incomplete.
- Native non-admin audit, coherent backup/restore, and explicit unknown-state
  recovery remain incomplete.
- Current end-to-end evidence is same-process and temporary-state only.

## Successor rule

When a stage is complete, update the plan checkboxes and this handoff, commit
the entire stage, confirm the tree is clean, and queue exactly one successor
using the command in
`docs/plans/backlog-v2-production-session-prompt.md`. Do not queue on
`continue` or `needs-input`. S19 queues nothing.
