# Continue the backlog orchestrator implementation

Work only in `/home/igor/Work/t3-steward` on Normandy. Implement the next coherent incomplete increment from `docs/plans/backlog-v2.md` using the session execution contract in that plan.

Read `CONTEXT.md`, the full plan, and `docs/plans/backlog-v2-handoff.md` when it exists. Reconcile the repository and test state before editing. Work on branch `feature/backlog-orchestrator`. Preserve unexplained changes and stop with a clear handoff if they cannot be reconciled safely.

This implementation chain runs ungated. Every successor submission must use `t3-backlog --ungated`, bypassing the forecast and quiet-hours gate while retaining hard quota-health controls.

Add or update tests with every behavior change. Run targeted tests, then `go test ./...` before committing. Run the milestone's other required checks when completing it. Update the checklist and handoff, commit the tested increment, and leave the working tree clean.

Do not install or deploy the development binary. Do not restart `t3-steward`, modify its live configuration, open its live state database with development code, push branches, create a pull request, or touch another host.

If implementation remains after this session, queue exactly one successor only after the commit succeeds:

```sh
t3-backlog \
  --project "t3-steward development" \
  --title "Continue backlog orchestrator implementation" \
  --importance 5 \
  --difficulty 5 \
  --model gpt-5.6-sol \
  --instance codex \
  --max-turns 6 \
  --ungated \
  < docs/plans/backlog-v2-session-prompt.md
```

After queueing the successor, do not make further changes. If M9 is complete, do not queue another task. Leave the release candidate and deployment-readiness report for the user, who will explicitly approve any host-wide deployment.

End with exactly one of:

```text
BACKLOG STATUS: done
BACKLOG STATUS: continue
BACKLOG STATUS: needs-input
```
