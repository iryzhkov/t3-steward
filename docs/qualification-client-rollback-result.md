# Client ownership rollback qualification, 2026-09-15

**PASS: 28 client assertions and 15 coordinator assertions on exact clean source.**

UpKeeper binary identity in both final evidence roots:
`964f5a18ea802d0b4885a9cef7339fd9a141ec89`, `vcs.modified=false`.
This commit builds on `f48c393` and adds receipt-backed client restoration.

## Exact process proof

```sh
python3 scripts/qualification/client_rollback.py /home/igor/Work/wt-qual-candidate-upkeeper
python3 scripts/qualification/coordinator_rollback.py /home/igor/Work/wt-qual-candidate-upkeeper
```

Both exit 0. The retained runners compile the actual binary, create disposable
historical Git releases, apply through actual target agent processes, then invoke
the supported CLI historical rollback, which dispatches a separate real agent.
The only transport substitution is an explicit local subprocess SSH adapter.
A systemctl fixture reports absent legacy units; no real service is contacted.

- Client log: `/tmp/client-rollback-final.log`.
- Client evidence: `/tmp/t3-client-rollback.faiv37xr/evidence`.
- Coordinator log: `/tmp/coordinator-rollback-with-client-final.log`.
- Coordinator evidence: `/tmp/t3-coordinator-rollback.ekkjobjm/evidence`.

Client scenarios apply two successive endpoint values, then roll back twice:

| Scenario | Expected and observed result |
| --- | --- |
| Original client absent | Restore absence twice |
| Original client present | Restore its exact original bytes twice |
| Unexplained client change | Refuse twice; preserve changed bytes |
| Ownership receipt missing | Refuse twice; preserve current client |

The pre-existing worker bootstrap remains byte-identical in every assertion.
Refusal cases are deliberate passing safety assertions, not successful rollback
claims. Coordinator scenarios restore an existing prior document or absence when
the historical component is absent or names another coordinator.

The positive coordinator runner now keeps its original client absence. Its old
unowned client sentinels would correctly trigger the new manual-recovery boundary;
the client runner separately exercises that refusal. Earlier coordinator results
remain valid for their recorded source, not a claim about this newer policy.

## Genuine negative control and review fixes

At clean `f48c393`, the original client runner exited 1:
`/tmp/client-rollback-negative.log`,
`/tmp/t3-client-rollback.gfg7fj1b/evidence`.
All four prior-byte/absence checks failed even though the real rollback CLI
returned success; worker bootstrap preservation already passed.

The receipt records the original bytes/absence and a pending digest before
activation, then the active digest after verification. Repeated updates preserve
that original boundary. Restored tombstones make repeated rollback idempotent.
A first converged apply records ownership without changing its existing bytes
or claiming changed authorization. The pre-fix converged-adoption test failed
in `/tmp/client-converged-negative.log`.

Independent source review found and verified fixes for legacy prior-byte
restoration, preflight of both retirements before effects, symlink ancestors with
missing suffixes, null receipt fields, and bounded/private checks at the first
client planning read. The final review reported no remaining concrete blocker
within the assigned scope.

Focused tests cover interrupted preparation, activation and restoration, both
prior presence states, repeat rollback and re-adoption; unknown/missing receipts,
drift, permissions, final and ancestor symlinks, bounded reads, FIFO refusal and
coordinator refusal before client mutation.

## Gates

- Final focused tests: PASS, `/tmp/client-receipt-targeted6.log`.
- Final `make go-gate`: PASS, `/tmp/client-receipt-final-go-gate.log`
  (format, vet, package tests, race tests, linux amd64/arm64 builds).
- Gate ran on the final code before commit; both process runs then rebuilt the
  exact clean commit. Earlier intermediate logs are preserved and superseded.
- No full 17-case suite rerun is claimed by these bounded extensions.

## Candidate endpoint smoke check

The latest candidate's selected fleet and worker components, read through Huyang
at candidate workspace revision 9, were applied in another disposable PC home.
Its client address was exactly `normandy-steward-admin`; retirement restored the
original client absence; the PC bootstrap digest remained
`2055fa24568cc8758ba029f819f9b6573bca4bd8034c2c18a95a1921a144b8e4`.
Application reported enrollment false.

Evidence and reviewable probe: `/tmp/t3-client-alias-check.P4lqRV`.
Log: `/tmp/client-alias-check.log`, exit 0. This endpoint check does not claim a
live SSH alias, provider call, or live Sol model admission.

## Migration boundary

Older prior.json files lack applied-state proof. Missing receipt plus existing
client requires manual recovery and preserves the file. Applying explicit matching
intent can establish a new boundary at its current bytes; it cannot infer an older
absence. Unexplained edits after retirement likewise require manual recovery.

No live configuration, credentials, service, enrollment, publication or deployment
changed. All test processes exit; only disposable evidence remains.
