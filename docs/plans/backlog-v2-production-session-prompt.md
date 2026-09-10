# Backlog-v2 production-binding serial successor

You are running unattended from the installed version-1 backlog while the user
is away. Work autonomously: do not ask questions or wait for confirmation. If a
decision genuinely requires the user, complete everything independent of it,
leave an exact handoff, queue no successor, and end
`BACKLOG STATUS: needs-input`.

Continue the production-binding chain only in
`/home/igor/Work/t3-steward` on Normandy, on branch
`feature/backlog-orchestrator`. Never use another checkout or host.

Read `CONTEXT.md`, all of
`docs/plans/backlog-v2-production-binding.md`,
`docs/plans/backlog-v2-production-handoff.md`,
`docs/architecture/system-model.md`,
`docs/plans/backlog-v2-deployment-readiness.md`, and every predecessor
artifact named by the handoff. Reconcile the actual branch, history, worktree,
checklists, code, and test state before editing. Preserve unexplained changes
and never build on a false checkpoint.

Select exactly the first incomplete named stage in the plan. Before
implementation, record its title, exact full starting commit, copied exit gates,
focused tests, and pre-existing worktree state in the handoff. Complete the
whole selected stage as one substantial unit while safe in-stage work remains.
Add tests with every behavior change and run its focused gates plus
`go test ./...`, `go build ./...`, `go vet ./...`, and
`git diff --check`. S19 also runs `go test -race ./...`.

This chain uses `t3-backlog --ungated`. Ungated bypasses forecast and quiet
hours only; hard quota-health admission remains authoritative. No admin or
recovery command may bypass closed quota admission.

Do not install or deploy the development binary, restart `t3-steward`, change
live configuration, open live state with development code, dispatch a real T3
thread, contact a fleet worker before explicitly authorized S19 qualification,
push, create a pull request, or touch another host.

End-state protocol is exclusive:

- If safe work remains inside the selected stage, queue no successor. Update the
  handoff with the exact checkpoint and end `BACKLOG STATUS: continue`; the
  steward will continue the same T3 thread.
- If the stage is complete, update its stage and milestone checkboxes, the
  architecture/readiness/operations documents whose claims changed, and the
  full handoff. Commit code/tests/docs together and confirm a clean worktree. If
  another stage remains, queue exactly one successor with the command below,
  make no further changes, and end `BACKLOG STATUS: done`.
- If user input or authorization is genuinely required, finish safe independent
  work, queue no successor, preserve the exact question in the handoff, and end
  `BACKLOG STATUS: needs-input`.

Successor command:

```sh
t3-backlog \
  --project "t3-steward development" \
  --title "Backlog-v2 production-binding successor" \
  --importance 5 \
  --difficulty 5 \
  --model gpt-5.6-sol \
  --instance codex \
  --max-turns 12 \
  --ungated \
  < docs/plans/backlog-v2-production-session-prompt.md
```

At the end of S19/R6, queue no successor. A GO readiness report still requires
explicit user approval before any host-wide installation or deployment.
