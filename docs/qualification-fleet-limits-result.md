# Supplemental qualification result, 2026-09-15

Evidence root: `/tmp/t3qual.DfYWBm/evidence`.

Binary build metadata saved there proves:

- Steward: `27c8b631983df8d97c72e7131447ad557897ac02`.
  The build reports a dirty tree because the supplemental Python/shell harness
  files were untracked; no product source was edited.
- UpKeeper: `63d108dcd37e54cd82ba29bcb2a8e1e2ca871972`,
  `vcs.modified=false`.

## Case verdicts

**Case 5: FAIL.** The compiled UpKeeper CLI rendered an explicit unapplied
enrollment plan with Opus desired on homelab and omarchy-pc and absent model
observations represented as null. Compiled target-side processes applied each
profile. Those are passing subclaims, but no runtime consumer connects the
review-only coordinator projection to Steward authorization at this source
revision. Observed/effective Opus availability is not proven. See
`case5-plan.out` and `case5-<host>-initial.out`.

**Case 6: PASS within the disposable synthetic-provider boundary.**
`case6-check.json` reports `catalog-digest-mismatch`, the full desired and
accepted digests, and expected enrollment revision 1 after coordinator restart.
`case6-codes.txt` records stale catalog / stale enrollment revision / current
fence exits `8 8 0`. The independently updated provider cache advertises the
added model before correct re-enrollment. `case6-run.json` shows one successful
task on worker-b and its declared output. `t3-stub.jsonl` contains one turn start
and one scripted turn. There were zero drift-related repeated log messages;
normal periodic worker-reconciliation INFO messages continue. No task was
running at the moment the catalog changed.

**Case 17: all named subclaims PASS across the recorded main run and corrected\nsecurity rerun.** This is not a claim that the final combined script had a wholly\ngreen rerun. The original main command exited 1 for case 5 and its two obsolete\nassertions described below. Measured subclaims:

- Actual admin forced command refuses a worker-credential-signed frame:
  `case17-wire-worker-role.*`.
- Actual server refuses a request over 4 MiB before JSON parsing:
  `case17-wire-oversize.*`.
- A real 25-byte output downloads normally, then a one-byte artifact cap refuses
  it with exit 7 and no payload: `case17-artifact-baseline.*`,
  `case17-artifact-refused.*`.
- Expired disposable worker snapshots yield temporary `worker-stale`:
  `case17-stale-check.out`.
- Good repository evidence remained cached immediately after the endpoint failed;\n  after an actual 605-second wait both workers re-probed and reported\n  `network-unavailable`, with overall `accepted_waiting`:\n  `case17-expiry-{prime,cached,after}.out`. Both expiry and temporary-network\n  assertions passed.\n- Fresh closed admission yields temporary `quota-closed` and
  `accepted_waiting`: `case17-closed-admission.out`.
- UpKeeper earlier-intent reapplication restores worker and client documents
  byte-for-byte on both disposable hosts, after both documents changed:
  `case5-<host>-{initial,change,rollback}.out` and
  `case17-rollback-digests.txt`. This proves target-side restoration, not the
  controller's release-selection command or a live service rollback.

## Harness corrections and exact checks

The initial artifact assertion expected the word `limit`; the product correctly
returned `invalid coordinator artifact response`. The corrected test additionally
downloads the same artifact with its normal cap before testing refusal.

The initial closed-quota fixture changed a pool with checks disabled. The runtime
correctly ignores retained quota admission when checks are disabled. The corrected
fixture enables checks for only the disposable pool, injects a fresh admission,
and restores both records afterward.

`python3 scripts/qualification/fleet_security.py /tmp/t3qual.DfYWBm` exited 0
with all four wire/quota/artifact assertions passing after these corrections.
The original supplemental output deliberately remains unchanged, including its
obsolete failed assertions; read the corrected independent evidence alongside it.

`bash -n scripts/qualification/fleet_limits.sh`,
`python3 -m py_compile scripts/qualification/fleet_limits.py scripts/qualification/fleet_security.py`,
and `git diff --check` passed. No Go product source was changed.

No live configuration, credential store, worker enrollment, active wait or T3
thread was changed. No push, publication or deployment occurred.
