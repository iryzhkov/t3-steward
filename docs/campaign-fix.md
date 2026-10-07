# Campaign fix

`t3-steward campaign fix` creates a new version 2 campaign from a recorded review that requested changes. It uses the ordinary submission path for readiness, acceptance, deterministic idempotency and thread wakes.

```sh
t3-steward campaign fix run-source/review --idempotency-key fix-source-1
t3-steward campaign fix run-source/review --idempotency-key fix-source-1 \
  --gate 'make check-review' --gate-timeout 30m --out ./fix-source
t3-steward campaign fix run-source/review --idempotency-key inspect-source \
  --no-gate --out ./inspect-source --dry-run
```

The key is required. Retrying with the same source, flags and key produces the same archive and replays the same run. Text starts with `run <id>`, followed by the fix summary and ordinary submit's receipt and wake instructions. `--json` prints `schemaVersion`, `runId`, `replay`, `sourceRun`, `reviewTask`, `reviewedCommit`, `verdict`, `lineage`, `gate` and `submission`. Failures use the normal JSON error envelope.

`--notify-thread current|ID` defaults to the current thread; `--no-notify` opts out. `--out DIR` retains the generated campaign and refuses an existing path. Without it a private temporary directory is removed after submission. `--dry-run` requires `--out`, validates and prints the generated plan, and submits nothing. Repeatable `--context FILE` adds context; basename collisions with other context and generated names are refused before coordinator access.

## Review and commit selection

The source is the review task's latest terminal, succeeded attempt. Its recorded `Attempt.ReviewVerdict` is authoritative. Only a task with no `review_output` and a declared output named exactly `review.md` may use the first-line fallback: exactly `VERDICT: ACCEPT` or `VERDICT: CHANGES_REQUESTED`. The receipt reports `recorded` or `review.md first line`. Structured review output should use verdict JSON; H1's verdict-line parser does not accept the `VERDICT:` prefix.

An accepted review refuses with “nothing to fix”. A missing verdict names `review_output` and older coordinators; an unsucceeded review is refused. No run is created.

The reviewed commit is the single declared commit consumed by the review through local dependency inputs or carried inputs. Carried inputs require source-run provenance. Modern attempt and artifact pins must still match the producer; a stale reviewed source is refused rather than rebound to newer work. Retained outputs must belong to the latest succeeded attempt. `--commit RUN/TASK/NAME` explicitly selects a declared commit of a succeeded producer when automatic selection is absent or ambiguous. Zero candidates and multiple candidates are refused with recovery instructions. The commit provenance supplies the commit and base; a `type: fresh` environment is refused.

## Generated template

The new workflow copies the source environment, project and class. The source producer's routes, placement hosts, resource preset, maximum turns and verification commands carry into the fix tasks; the source reviewer's routes and maximum turns carry into the reviews.

| Task | Inputs and behavior |
| --- | --- |
| `fix1` | External references to every declared producer and review output; focused checks and a declared `fix` commit. |
| `review2` | `fix1`'s commit, handoff and verification log; independent review with `review.md` and structured `verdict.json`. |
| `fix2` | `fix1`'s commit and handoff plus `review2`'s review and verdict; final worker gate. |
| `review3` | `fix2`'s commit, handoff, verification log and gate log when present, plus `review2`'s files. |

Both review tasks declare `review_output: {verdict: verdict.json}`; human review files retain the `VERDICT:` first line. After `review2` accepts, `fix2` checks out and declares the same commit unchanged and writes a no-op handoff and verification log. `review3` carries that acceptance with the fresh gate result. With only one round remaining the graph is `fix1` (gate) -> `review2`.

Embedded prompts and `inputs/fix/rules.md` require regression tests for reproduced defects, focused checks, a self-review swarm and test-integrity listing. Reviewers never edit source. Tasks discover dependency directories by listing `.t3/dependencies/`.

The original producer prompt is retained at `inputs/fix/brief/prompt.md`, with its inputs under `inputs/fix/brief/inputs/`. Subsequent fix runs carry `inputs/fix/brief/**` unchanged. Campaign size and file limits apply.

## Gate and lineage

No agent gate task is generated. H2's worker-owned `gate:` runs on the final fix task after verification, outside the agent turn. Every attempt executes its gate afresh, including a no-op fix. Gate resolution is explicit repeatable `--gate CMD` commands (timeout `--gate-timeout DUR`, default 30m), then `--no-gate`, then recorded lineage gate, then producer gate. If no gate resolves, pass `--gate` or `--no-gate`. Coordinator verification timeout limits still apply.

`inputs/fix/lineage.json` uses schema `steward.fix-lineage/v1` and records root run, root producing and review tasks, `round_limit`, rounds used before the run, rounds declared, gate and prior fix-run review evidence. Malformed, incomplete, null or duplicate lineage authority fields are refused. A fix task with work counts as one round: `fix1` always counts; `fix2` counts unless `review2`'s recorded verdict accepts.

The default limit is 4, inherited from lineage thereafter. `--round-limit N` overrides it and accepts 1 through 8. Each new campaign declares at most two remaining rounds. At exhaustion the command submits nothing and reports `review-round-limit-exhausted`, `roundsUsed`, `roundLimit`, the latest verdict and finding titles. Recovery is a design pass or an explicit higher limit.

## Relation to other tools

This replaces mkfixchain's manual artifact copying and bundle shipping with retained declared-commit and review references. Bundle-only chains need a declared commit, an explicit `--commit`, or mkfixchain.

M16-4 counts in-task checkpoints of one review authority. This command counts cross-run fix rounds; the shared vocabulary does not make those counters interchangeable. G7/N5 will later move loop expansion and escalation into declared coordinator workflows while preserving lineage compatibility. This unit adds no runtime loops, conditional edges, reviewer selection or approver asks. `campaign rerun` remains the way to re-execute existing task definitions; fix generates new task definitions.
