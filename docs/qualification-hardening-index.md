# Hardening qualification evidence index — 2026-09-15

**Disposable qualification gate: PASS.** All seventeen cases have the bounded
process evidence below. The final client rollback gap is repaired and proven:
receipt-backed historical rollback restores exact prior client bytes or absence,
refuses unexplained drift, and preserves unowned files. Live gates remain pending.

This is a composite evidence index, not a claim that one binary ran all seventeen
cases. Different revisions are retained deliberately. Later fixes replaced the
specific evidence they invalidated; unrelated older scenarios are not discarded
merely because their commit differs. Live canary, service convergence, enrollment
rollback and publication gates remain separate.

## Evidence sets and source identities

All paths below name retained disposable evidence directories. Binary revisions
were checked using `go version -m`; short revisions are unambiguous in these
repositories.

| Set | Evidence directory | Tested source and outcome |
| --- | --- | --- |
| Main | `/tmp/t3qual.Hao5eR/evidence` | Steward `7c9857cfb464442c4acb8c6837bf6335013e4ecc`, clean. Lead foreground exec 71407 completed exit 0; all printed summary rows PASS. **Its case 12 oracle is invalid**, replaced below. Cleanup completed. |
| Race | `/tmp/t3qual.JRHKuJ/evidence` | Steward `881cad284157801f75256963bfa5309ecaea9c0f`, clean; same product code as integration `35461fe`, with corrected harness. Selective case 12 exit 0, three iterations per ordering. Log `/tmp/case12-strict-final.log`. |
| Ownership | `/tmp/t3qual.0883tv/evidence` | Steward `64de404a3eeaf8b4c8dc644948f8e18731f96d79`; UpKeeper `30e6b26db87f34cc7fb2ed4d705a9a08a8727f4a`, both clean. Nine assertions PASS, exit 0. |
| Candidate | `/tmp/t3-candidate-contract.jtiH6t/evidence` | Steward `35461fe90a1b77217c880ce30b5be065a5d71ad7`; UpKeeper `f48c393ab853c93f4c8b479e633473d62daf1996`, clean. Actual candidate projection applies and loads; bootstrap digests retained. This is configuration evidence, not task execution. |
| GitHub | `/tmp/t3qual.tbvnKz/evidence` | Steward `5219a71f41a43cbaa417f37c37543037ba62b472`, build marked dirty while its supplemental harness was being developed. Actual private GitHub one-worker/both-worker checks PASS; zero runs. Do not describe this as a clean exact-commit build. |
| Drift/limits | `/tmp/t3qual.DfYWBm/evidence` | Steward `27c8b631983df8d97c72e7131447ad557897ac02`, build dirty from supplemental harness; UpKeeper `63d108dcd37e54cd82ba29bcb2a8e1e2ca871972`, clean. Case 6 PASS. Case 17 combines original observations with the documented corrected security rerun; original whole command was not green. |
| Recovery | `/tmp/t3qual.Q7eHlh/evidence` | Steward `35461fe90a1b77217c880ce30b5be065a5d71ad7`, clean. Cases 9, 10 and 16 all PASS in `supplemental-results.json`. Supersedes earlier `61S9F9` for these scenarios. |
| Rollback | `/tmp/t3-coordinator-rollback.21ztblfp/evidence` | UpKeeper `785a064c86280f6d178d932f8e59b5cf43dde454`, clean. `verdict.json` has no failures for the 15 recorded assertions. **These assertions do not establish the newly missing client-document restoration case.** |

Main stdout was delivered through the lead's foreground tool result rather than
saved to a main log file. Its named JSON, coordinator journals, SSH journal and
provider journal remain in Main. No inference of a passed case comes from an
empty evidence directory.

## The seventeen contract cases

The historical harness label `case16-wake-all` is a supplemental wake-mode check.
**Contract case 16 is artifact plus durable Git-reference provenance after pruning.**
It is indexed independently below.

| Contract case | Verdict | Evidence and measured outcome |
| --- | --- | --- |
| 1. Non-coordinator admin identity/check/submit | PASS | Main `case1-identity.json`, `case1-submit.json`, `sshd.log`: `qual-coordinator`, SSH carrier, actual restricted forced command, one run `run-839e8b96f4f09c446f5cc8debcac1f04`. Remote viability responds without a direct coordinator shell. |
| 2. Lost response and idempotent retry | PASS | Main `case2-interrupt.json` records the client killed after the coordinator write; `case2-second.json` names `run-469f42f52a689884af1d35929f4317ce`. Lead summary confirms one run for the repeated key. |
| 3. Permanent repository/ref/syntax failures | PASS | Main `case3-{absent-forge,private-https,missing-ref,bad-syntax,argument-injection}-check.json` and matching submit errors. All five refuse submission permanently with exit 8 and no new runs. Forgejo wording is reproduced by a local forced command; private HTTPS actually reaches GitHub without credentials. |
| 4. Private GitHub readiness with one then both credentials | PASS | GitHub `private-github-{one,both}.json`, `private-github-config-hashes.json`, `private-github-result.json`: worker-a authenticated first, worker-b explicitly missing its reference; then both authenticate. No workflows. Existing authorized SSH key referenced, not copied. This proves repository readiness; stale synthetic quota can still yield `accepted_waiting`. |
| 5. Authored fleet configuration and explicit enrollment | PASS | Ownership `case5-verdicts.txt`, `case5-enrollment-plan.out`, `case5-model-sets.json`, `case5-run-worker-{a,b}.json`: actual UpKeeper application/restart, readable model diff, absent observation refuses enrollment, observed unapproved model excluded, explicit enrollment then successful illustrative Opus tasks. Runs `run-341ffea7e6d6f2378b599561e54d38eb` and `run-859fbb152bd88ea1c1c7b693c7a4de5d`. Candidate additionally proves current authored projection loads and bootstrap bytes remain compatible. |
| 6. Catalog drift across coordinator restart | PASS | Drift/limits `case6-check.json`, `case6-drift.json`, `case6-codes.txt`, `case6-run.json`: full old/new digests, enrollment revision 1 retained, stale catalog/revision exits `8 8`, current fence exit `0`; added observed model runs successfully on worker-b in `run-d434567979e3bca7ee8d54015f34bf09`. No task was active during catalog change. |
| 7. Bounded preparation failure and retained cause | PASS | Main `case7-preparation-logs.txt`, `case7-run.json`: three immutable logs, original cause retained, terminal failed attempt `attempt-e0446e38cec824722103a53f6715b4fa` in `run-dad7ca4abea9d4ac3a0f48297fd67c7b`; no provider turn. Replaces the earlier stuck-verifying failure. |
| 8. Legacy intake quarantine across restarts | PASS | Main `case8-quarantine.json` and `coordinator*.log`: one durable entry for unmapped `no-such-project-anywhere`, three coordinator lifetimes, one diagnostic log line. No workflow is created for that source. |
| 9. Rerun from failed root with provenance | PASS | Recovery `case9-{original,new,original-after}.json`, `case9-rerun-response.json`: source `run-0677c79b49a37db7d761f68d1a4773ff`, new `run:rerun:qual-rerun-new`, failed source attempt `attempt-4812820049b61b56ffc329a84f72e0f3`. Descendant succeeds with original ancestor bytes, ancestor executes once, source records remain unchanged. |
| 10. Codex-session terminal notification | PASS, synthetic session boundary | Recovery `case10-{submit,run,delivery}.json`: `run-0eb279711dd355f9fd62bd176c495092`; only provider session `qual-codex-provider-session` supplied, native event-log projection resolves `qual-interactive-canonical`, exactly one terminal notification. The projection is a fixture; no actual Codex provider turn is claimed. |
| 11. Park, release capacity, resume and collect once | PASS | Main `case11-{parked,woken}.json`, `case11-neighbour.json`, `case11-register.txt`, `case11-turn2-workspace.txt`, provider journal. Attempt `attempt-c5d87023c9af3dfabd5855ea5fc3bc83`, run `run-a863eac53b890962df4ff6ea70442309`, wait `tw-bc5e03d1`, thread `thread-3ccba3951858d4f6f75132b398da85cc`. Park has no outputs/verification; neighbour runs on the one-slot pool; same thread resumes once with identity intact; one output and verification, success. |
| 12. Registration/completion race both ways | PASS, corrected oracle | Race `case12-observations.txt` and per-signal `*-samples.jsonl`, `*-verdict.json`, `*-register.txt`, `*-release.json`, `*-final.json`. Three actual parks and three exact terminal refusals (exit 8); all six tasks succeed with output/verification, nine total scripted turns. Every verdict binds run/attempt/thread. No sampled state is both parked and terminal. |
| 13. Worker and coordinator restart across a park | PASS | Main `case13-{submit,woken}.json`, `case13-register.txt`, journals: run `run-5e73167b7ccefaeb6ba4b4d1a152c4be`, attempt `attempt-2c978fef1563a814f2a80ec608eebfc2`, thread `thread-80da0f70946a993c613bb8a7346f7098`; one wake, two total turns, one verification and success after both restarts. |
| 14. Refuse terminal registration and replay | PASS | Main `case14-register.txt`, `case14-source.json`: explicit terminal-success refusal, exit 8, no new local wait. `replay-2-register.txt` additionally refuses settled `tw-qual-replayed-request`; it does not print the end-your-turn parked message. |
| 15. Interactive wait remains independent | PASS | Main `case15-create.json`, `case15-register.txt`, provider journal: one wake to `thread-interactive-case15`, no workflow count change or other-thread wake. |
| 16. Artifact and durable Git ref after cache pruning | PASS | Recovery `case16-{before-prune,pruning,consumer,after}.json`, producer commit/workspace records. Run `run-40e93501fada8011cd6c897bce204ae7`, commit `591b8b6a93cb546593a8e2bb2e76ca3fb6ca1853`; consumer receives original file and resolves `refs/campaigns/run-40e93501fada8011cd6c897bce204ae7/task-d15785f3ec7ed079cee795fc4eb2c7b0/implementation` after the commit is demonstrably gone from the pruned repository cache. |
| 17. Limits, authorization and rollback | PASS | Drift/limits corrected wire-role, oversized-frame, artifact cap, stale snapshot, actual 605-second repository-cache expiry and closed-quota assertions pass. Existing worker/client byte restoration and the Rollback set's historical coordinator document/absence/moved-owner assertions pass. The final client rollback suite adds 28 passing assertions at `/tmp/t3-client-rollback.faiv37xr/evidence`; the coordinator suite passes 15 at `/tmp/t3-coordinator-rollback.ekkjobjm/evidence`. Both use clean UpKeeper `964f5a18ea802d0b4885a9cef7339fd9a141ec89` (integrated as `33d980c`). Original absence, exact legacy bytes, repeated rollback, drift/missing-proof refusals and unchanged adoption are covered. The baseline failed four restoration checks despite CLI exit 0; the fixed process suites exit 0 with every assertion passing. |

## Decisive supplemental wake evidence

Main `case16-all-{registered,held,delivered}.json` proves both waits are `all`.
During the hold, `tw-b917430c` is met while `tw-731e006b` is pending, with no
collection. Both later deliver under the single ID `task-wake:tw-731e006b:7`;
`case16-all-final.json` succeeds on original thread
`thread-201177b94beb27c781e54619bdb86e8e`. The independent each scenario succeeds
while another wait remains pending. This distinguishes actual all/each semantics
from an implementation that waits for everything regardless of mode.

Main also retains commanded-cancel and actual-owner lease-expiry attack evidence.
These prove zero premature output/verification; the lease case checks the owning
worker-b stopped and the same assignment became unknown after its real deadline.
They are supplemental safety checks, not substitutes for numbered cases.

## Invalidated evidence and remaining boundaries

- `FKS3US` has mixed script provenance and exposed real lifecycle failures. It is
  diagnostic history, not clean whole-run qualification.
- `bYyz9g` completed exit 1 with duplicate all wake and failed preparation
  collection defects. Main replaces those results.
- Both older full runs' printed case 12 PASS labels lack completion-first proof.
  The shell background child retained saved output-pipe descriptors, so capture
  waited for registration before reporting provider completion. Race closes extra
  descriptors and releases its in-flight check only after durable terminal state.
  Two valid controls plus nine altered evidence variants were checked; all invalid
  variants were refused (`/tmp/case12-oracle-negative.json`).
- Old Ownership `7RkH3y` hid the authorization diff and is superseded by
  `0883tv`. Old case 5 in Drift/limits lacked a runtime ownership consumer.
- The current authored-route and recovered-project checks were exercised with
  failing-before/passing-after real-store/real-workspace regressions; Candidate
  checks the current projection consumer. These fixes add refusals and do not
  invalidate unchanged authorized routes in earlier positive scenarios.
- Provider observations/turns are synthetic, except the actual network Git reads.
  Case 10 proves session resolution, not commercial provider behavior. No live
  fleet rollout or task is inferred from these process tests.
- The rollback process uses actual CLI/historical release selection/target agent,
  but substitutes local subprocess transport and absent-service observations.
  It does not replace the separately required live canary rollback, capability
  refresh, enrollment check or service convergence.
- No requirement is satisfied by an unattempted case. The final client-prior
  restoration process proof closes case 17; its independent review also covered
  bounded private reads, malformed receipts, symlink containment and crash windows.
  See [the final client rollback report](qualification-client-rollback-result.md).

Detailed companion reports: [ownership](qualification-case5-runtime-result.md),
[drift and limits](qualification-fleet-limits-result.md),
[rollback process](qualification-coordinator-rollback-result.md),
[private GitHub](qualification-private-github.md),
[recovery/provenance](qualification-recovery-provenance.md).
