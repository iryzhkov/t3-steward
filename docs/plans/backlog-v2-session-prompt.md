# Backlog-v2 serial implementation successor

You are running unattended from a task backlog while the user is away. Work autonomously: do not ask questions or wait for confirmation. If a decision genuinely needs the user, complete everything independent of it, leave an exact handoff, queue no successor, and end `BACKLOG STATUS: needs-input`.

Continue the serial backlog-v2 implementation chain only in `/home/igor/Work/t3-steward` on Normandy, on branch `feature/backlog-orchestrator`. Never use another checkout or host.

Read `CONTEXT.md`, all of `docs/plans/backlog-v2.md`, `docs/plans/backlog-v2-handoff.md`, and every predecessor artifact named by the handoff. Reconcile the actual branch, history, worktree, named-stage checklist, milestone checklist, code, and test state before editing. Preserve unexplained changes and never build on a false checkpoint.

Select exactly the first incomplete named stage in the plan's “Remaining serial stage checklist.” Record the selected stage, starting commit, and exact exit gates in the handoff before implementation. Complete that whole stage as one substantial unit; do not voluntarily split it into smaller per-turn increments while safe in-stage work remains. Add tests with every behavior change and run the stage-specific gates plus `go test ./...`, `go build ./...`, `go vet ./...`, and `git diff --check`. S11 also runs the race suite.

This chain is ungated. Hard quota-health controls still apply; no admin command may bypass closed quota admission. Do not install or deploy the development binary, restart `t3-steward`, modify live configuration, open the live state database with development code, contact or dispatch workers, push, create a pull request, or touch another host.

End-state protocol is exclusive:

- If safe work remains inside the selected stage, queue no successor. Update the handoff with the exact checkpoint and end `BACKLOG STATUS: continue`; the steward will continue this same T3 thread.
- If the selected stage is fully complete, update the named-stage and milestone checklists, write the full handoff, commit all code/tests/docs together, and confirm the worktree is clean. If another named stage remains, queue exactly one successor with the command below, make no further changes, and end `BACKLOG STATUS: done`. Never emit `continue` after queueing.
- If user input is genuinely required, queue no successor, preserve safe work and the exact question in the handoff, and end `BACKLOG STATUS: needs-input`.

Successor command:

```sh
t3-backlog \
  --project "t3-steward development" \
  --title "Backlog-v2 serial implementation successor" \
  --importance 5 \
  --difficulty 5 \
  --model gpt-5.6-sol \
  --instance codex \
  --max-turns 12 \
  --ungated \
  < docs/plans/backlog-v2-session-prompt.md
```

At the end of S13/M9, queue no successor. Leave the release candidate and deployment-readiness report for explicit user approval of any host-wide deployment.
